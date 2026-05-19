# trl-proxy

One leg of an active/standby HA pair that bridges a Janus audiobridge to
MediaMTX. The proxy receives RTP from its local Janus, runs every language
stream through a GStreamer pipeline (decode → volume → re-encode), and
publishes the result into MediaMTX over RTSP — but **only when keepalived
tells it to**. The standby leg keeps the ingress side hot at all times, so a
takeover is essentially zero-warmup.

```
                                    MQTT cluster (signaling)
                                            ▲
                            role/command    │   janus/status, proxy/status
                ┌───────────────────────────┴───────────────────────────┐
                │                                                       │
        ┌───────┴────────┐                                       ┌──────┴──────────┐
        │   keepalived   │  VRRPv3 unicast over vRack            │   keepalived    │
        │   (notify.sh)  │ ◄──────────────────────────────────►  │  (notify.sh)    │
        └───────┬────────┘                                       └──────┬──────────┘
                │ /health (loopback)                                    │ /health
                ▼                                                       ▼
        ┌────────────────┐                                       ┌──────────────────┐
        │  trl-proxy-1   │   active (publishing) | standby       │   trl-proxy-2    │
        │  10.20.30.10   │                                       │   10.20.30.21    │
        └───┬────────────┘                                       └──────┬───────────┘
            │ RTP from Janus 1 (always)                                 │ RTP from Janus 2 (always)
            ▲                                                           ▲
        ┌───┴────────┐                                              ┌───┴────────┐
        │  Janus 1   │                                              │  Janus 2   │
        └────────────┘                                              └────────────┘
            │                                                           │
            │  Only the active leg actually publishes 27 streams        │
            └───────────────────────────────►  MediaMTX  ◄──────────────┘
                                                  │
                                                  ▼
                                              listeners
```

## Design at a glance

- **Each proxy knows only about itself.** Topics use a `NODE_ID` namespace
  (`trl/janus/{N}/status`, `trl/proxy/{N}/...`). There is no peer-to-peer
  communication between proxies — keepalived is the sole orchestrator.
- **keepalived = single source of truth for role.** Local `track_script`
  curls `/health`; VRRP unicast over vRack picks the master; a notify script
  publishes the resulting role into `trl/proxy/{N}/role/command` (retained,
  QoS 1).
- **Idempotency + anti-flap in the state machine.** A repeated `active`
  command while already active is a no-op. Two transitions closer than
  `ROLE_ANTIFLAP` (default 5s) are dropped with a warning.
- **No floating VIP.** Election happens, but the IPs stay put. MediaMTX sees
  a clean publisher cut-over.
- **Active takeover kicks zombies first.** Before opening its 27 RTSP
  publishers the new active leg lists MediaMTX paths and kicks any leftover
  publisher session, then waits a short pause, then opens egress.
- **Warm standby.** While standby, ingress (udpsrc → jitterbuffer → decoder
  → volume → encoder) keeps running. Only the egress (rtspclientsink) is
  added/removed dynamically on role transitions.
- **Structured logs with reason for every decision** — easier post-incident
  triage.

## Repository layout

```
cmd/trlproxy/main.go              -- wire-up, signal handling, graceful shutdown
internal/config/                  -- env parsing + validation
internal/logx/                    -- log/slog setup (file or stdout)
internal/health/                  -- HealthAggregator + HTTP /health
internal/mediamtx/                -- REST client (list paths, kick zombies, ping)
internal/mqttx/                   -- Paho v3 client (LWT, autoreconnect, restored subs)
internal/pipeline/                -- per-language GStreamer worker + manager
internal/role/                    -- role state machine (anti-flap, idempotent)
.env.example                      -- annotated template for the env file
```

## Requirements

- **Go 1.24+** to build.
- **GStreamer 1.20+** runtime: core + `gst-plugins-base`, `gst-plugins-good`,
  `gst-plugins-bad` (`opus`, `rtp`, `rtsp`, `audioconvert`, `audioresample`,
  `volume`, `tee`, `udpsrc`, `rtspclientsink`).
- **MediaMTX v1.x** with the v3 REST API enabled (default).
- **MQTT broker** (one endpoint or a cluster; this proxy uses Paho v3).
- **keepalived** with a track_script polling `http://127.0.0.1:9090/health`
  and a notify script that publishes the role into MQTT. (This repo does
  **not** include the keepalived configuration — that lives on the host.)

## Build

```bash
go build -o trlproxy ./cmd/trlproxy
```

The binary statically links against `cgo`/GStreamer headers; the host
running the binary still needs GStreamer **runtime** libraries available.

On Rocky Linux 9:

```bash
dnf install -y gstreamer1 gstreamer1-plugins-base gstreamer1-plugins-good \
               gstreamer1-plugins-bad-free gstreamer1-plugins-ugly-free
```

## Configuration

All configuration is read from environment variables — there is no config
file parser inside the binary. Copy `.env.example` and edit it:

```bash
cp .env.example /etc/trl-proxy/proxy.env
chown root:trlproxy /etc/trl-proxy/proxy.env
chmod 0640 /etc/trl-proxy/proxy.env
```

The minimum you must change per node:

| Variable | trl-proxy-1 | trl-proxy-2 |
|----------|-------------|-------------|
| `NODE_ID` | `1` | `2` |
| `MQTT_PASS` | actual secret | actual secret |
| `MEDIAMTX_API` | real URL | real URL |
| `MEDIAMTX_RTSP` | real URL | real URL |
| `JANUS_RTP_BIND_ADDR` (optional) | `10.20.30.10` | `10.20.30.21` |

See `.env.example` for every supported variable with comments.

### Strict validation

`NODE_ID`, `MQTT_BROKERS`, `MEDIAMTX_API`, `MEDIAMTX_RTSP` are mandatory.
The binary exits with a clear error if any is missing or has an invalid
value — there is no "implicit active" mode.

## Run

### Manually (dev / smoke test)

```bash
set -a; source .env; set +a
./trlproxy
```

### Via systemd (recommended for production)

`/etc/systemd/system/trl-proxy.service`:

```ini
[Unit]
Description=TRL Proxy (HA leg)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=trlproxy
EnvironmentFile=/etc/trl-proxy/proxy.env
ExecStart=/usr/local/bin/trlproxy
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

```bash
useradd -r -s /sbin/nologin trlproxy
install -d -o trlproxy -g trlproxy /var/log/trl-proxy
install -m 0755 trlproxy /usr/local/bin/trlproxy
systemctl daemon-reload
systemctl enable --now trl-proxy
```

Logs land in `/var/log/trl-proxy/trlproxy.log` (main) and
`/var/log/trl-proxy/{lang}.log` (one per language). With `LOG_DIR` set,
stdout is silent — systemd journal stays clean.

## MQTT contract

All topics are namespaced by `NODE_ID`. A proxy with `NODE_ID=1` will:

- **Publish** (retained, QoS 1):
  - `trl/proxy/1/status` — `{"status":"online", "node_id":1, "started_at":"..."}`
    on connect, `{"status":"offline"}` as LWT or on graceful shutdown.
  - `trl/proxy/1/role/current` — JSON echo of the current role after every
    transition. Shape:
    ```json
    {
      "role": "active",
      "since": "2026-05-18T10:39:42.123Z",
      "sessions": 27,
      "last_transit_ms": 312,
      "last_error": ""
    }
    ```

- **Subscribe** (QoS 1):
  - `trl/janus/1/status` — produced by the Janus sidecar. Accepted payloads:
    - plain text: `online` / `offline`
    - JSON: `{"status":"online"}` or `{"online":true}`
    - anything else is interpreted as offline.
  - `trl/proxy/1/role/command` — produced by keepalived's notify script.
    Accepted payloads: `active` / `standby`. Anything else is ignored
    with a warning.

A proxy with `NODE_ID=2` uses `.../2/...` topics — it never touches its
sibling's namespace.

## Health endpoint

`GET /health` returns either 200 + JSON when everything is good, or 503 +
JSON with a `reason` field. Sample healthy response:

```json
{
  "status": "ok",
  "janus_online": true,
  "mediamtx_reachable": true,
  "rtp_age_ms": 42,
  "rtp_last_ts": "2026-05-18T10:39:42.812Z",
  "mediamtx_last_check": "2026-05-18T10:39:43.001Z"
}
```

Failure reasons surfaced in the `reason` field:

| Reason | Meaning |
|--------|---------|
| `janus_offline` | Last `trl/janus/{N}/status` message says offline (or has not arrived yet). |
| `no_rtp_yet` | Pipeline has not seen a single RTP packet since startup. |
| `rtp_stale` | The newest RTP packet from any language is older than `RTP_HEALTH_THRESHOLD`. |

Bind the endpoint to loopback (`HEALTH_HTTP_LISTEN=127.0.0.1:9090`) — only
the local keepalived `track_script` needs to reach it.

### Why MediaMTX reachability does NOT affect `/health`

MediaMTX runs behind its own HA pair with a floating VIP. When that VIP is
migrating, the API is briefly unreachable **from both legs of our proxy pair
at the same time**. If we let that drive `/health` to 503, both proxies
would simultaneously become "unhealthy" and keepalived would just flap
between two equally-blind legs without solving anything.

So `mediamtx_reachable` is published in the JSON for diagnostics, the
background pinger keeps running, and the role state machine uses it to
decide whether to bother attempting a kick on takeover — but it has no
influence on the HTTP status code. While MediaMTX is failing over,
`rtspclientsink` from GStreamer handles the disconnect on its own
(automatic reconnect), so the active leg simply resumes publishing once the
VIP is back.

## Pipeline internals

For each of the 27 languages a single GStreamer pipeline is built (and kept
in `PLAYING` whenever the worker is alive):

```
udpsrc port=<lang>
  ! application/x-rtp,media=audio,clock-rate=48000,encoding-name=OPUS,payload=<PT>
  ! rtpjitterbuffer latency=<ms> drop-on-latency=true post-drop-messages=true
  ! rtpopusdepay
  ! opusdec plc=true
  ! audioconvert
  ! audioresample
  ! volume volume=<gain>
  ! opusenc bitrate=<br>
  ! tee name=t allow-not-linked=true
       ! queue ! fakesink                       (idle branch, always linked)
```

- A pad probe on `udpsrc.src` calls `HealthAggregator.TouchRTP()` on every
  buffer (cheap atomic store) — that drives `rtp_age_ms` in `/health`.
- `allow-not-linked=true` on the `tee` keeps the encoder running even when
  the egress branch is detached.

On a transition to `active`, an extra branch is added per worker:

```
       t. ! queue ! rtspclientsink location=<MEDIAMTX_RTSP>/trl_<lang>
```

(via `tee.GetRequestPad("src_%u")` + `SyncStateWithParent`).

On a transition to `standby`, the branch is torn down cleanly:

1. Install a blocking `PadProbeTypeIdle` on the tee src pad.
2. Unlink, send `EOS` downstream so `rtspclientsink` issues RTSP TEARDOWN.
3. Wait briefly, then drive `queue` and `rtspclientsink` to `NULL`.
4. `pipeline.RemoveMany(...)` and `tee.ReleaseRequestPad(...)`.

If the whole pipeline crashes (e.g. encoder error), the worker rebuilds it
after `RESTART_DELAY` and, if egress was previously open, reopens it on the
fresh pipeline.

## Role transitions

| From → To | Steps |
|-----------|-------|
| `* → active` | 1. `mediamtx.KickAllOnPaths(...)` for the 27 paths (timeout `MEDIAMTX_KICK_TIMEOUT`). 2. Pause `MEDIAMTX_KICK_PAUSE`. 3. `manager.OpenAll()` adds egress branches. 4. Publish echo to `trl/proxy/{N}/role/current`. |
| `* → standby` | 1. `manager.CloseAll()` tears down every egress branch. 2. Publish echo. |
| `X → X` | No-op; echo is still published to refresh the retained snapshot. |
| anything inside `ROLE_ANTIFLAP` window | Ignored with a warning. The previous role stays in effect. |

The very first role after startup is taken from `ROLE_STARTUP`
(default `standby`). A retained `role/command` from MQTT will arrive within
milliseconds of subscribing and will normally overrule this default.

## Logs

- Main log: `{LOG_DIR}/trlproxy.log` (JSON, one event per line).
- Per-language log: `{LOG_DIR}/{lang}.log` (JSON, scoped to that language's
  worker).
- If `LOG_DIR=""`, logs go to stdout instead. Useful for dev runs.

Level is controlled by `LOG_LEVEL` (`debug|info|warn|error`).

Recommended `logrotate` snippet (`/etc/logrotate.d/trl-proxy`):

```
/var/log/trl-proxy/*.log {
    daily
    rotate 14
    missingok
    compress
    delaycompress
    notifempty
    copytruncate
}
```

`copytruncate` matters: the binary keeps the log files open, and we don't
implement a SIGHUP-reopen.

## Troubleshooting

| Symptom | First thing to check |
|---------|----------------------|
| `/health` returns 503 with `reason: janus_offline` | The Janus sidecar is not publishing to `trl/janus/{N}/status`, or it published `offline`. Check the sidecar. |
| `/health` returns 503 with `reason: rtp_stale` or `no_rtp_yet` | Janus is not actually sending RTP. Verify `rtp_forward` and firewall: `tcpdump -ni any udp portrange 24500-24568`. |
| `mediamtx_reachable: false` in `/health` body, but status still 200 | This is by design — MediaMTX VIP failover should not cause our pair to flap. Check that MediaMTX itself recovers; the active leg will resume publishing via `rtspclientsink` auto-reconnect once it does. |
| Active leg won't publish to MediaMTX after VIP recovers | Look at `{LOG_DIR}/trlproxy.log` for repeated pipeline restarts. `rtspclientsink` usually reconnects on its own; if it does not, the worker will rebuild the pipeline after `RESTART_DELAY`. |
| Active leg can't open egress on takeover | Look at `{LOG_DIR}/trlproxy.log` for `kick zombies failed` or `open all egress`. Possibly the previous active leg did not release publishers; the kick step is supposed to fix that — if it fails, check MediaMTX API auth/connectivity. |
| Both legs think they are active | This is a keepalived problem, not a proxy problem. Check VRRP advertisements over vRack and that each `track_script` is calling its **local** `/health`. |
| Repeated `role command ignored by anti-flap` | Something is causing keepalived to oscillate. The anti-flap window protects MediaMTX, but the root cause is upstream — check VRRP and the health source. |

## What this repo does NOT contain

- keepalived configuration / notify scripts.
- The Janus-side MQTT sidecar that publishes `trl/janus/{N}/status`.
- MediaMTX configuration.
- A systemd unit file (sample shown above; commit one to your ops repo).
