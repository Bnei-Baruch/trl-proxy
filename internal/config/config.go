// Package config loads and validates the application configuration from
// environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all trl-proxy settings: base pipeline parameters
// (RTP/RTSP/encoder) as well as HA-related ones (MQTT, role, health).
type Config struct {
	NodeID             int           // HA leg ID (1 or 2). Drives the MQTT topic namespace.
	MQTTBrokers        []string      // broker cluster endpoints: host:port[,host:port,...]
	MQTTUser           string        // MQTT username
	MQTTPass           string        // MQTT password
	MQTTClientIDPrefix string        // client_id prefix; final id = "{prefix}-{NodeID}"
	MQTTKeepAlive      time.Duration // MQTT keepalive interval (default 15s)
	MQTTConnectTimeout time.Duration // initial connect timeout (default 10s)

	JanusRTPBindAddr string // bind address for UDP sockets; "" = all interfaces

	MediaMTXAPI          string        // http://host:port (MediaMTX control plane)
	MediaMTXRTSPBase     string        // rtsp://host:port/basepath (without mountpoint)
	MediaMTXPingInterval time.Duration // background API ping interval (default 2s)
	MediaMTXPingTimeout  time.Duration // single HTTP request timeout (default 1s)
	MediaMTXKickTimeout  time.Duration // total timeout for kicking zombies on takeover (default 2s)
	MediaMTXKickPause    time.Duration // pause between kick and opening egress (default 200ms)

	HealthHTTPListen   string        // address for the /health HTTP endpoint
	RTPHealthThreshold time.Duration // maximum allowed age of the last RTP packet (default 3s)

	LogDir   string // log directory; "" means "stdout only"
	LogLevel string // debug|info|warn|error

	RoleAntiflap time.Duration // minimum interval between role transitions (default 5s)
	RoleStartup  string        // initial role before any MQTT command: "standby" (default) | "active"

	GainLinear   float64       // volume= property for the GStreamer volume element (0.5 = -6 dB)
	JitterMs     int           // rtpjitterbuffer latency in milliseconds
	PayloadType  int           // expected RTP payload type
	OpusBitrate  int           // bitrate for opusenc
	RestartDelay time.Duration // delay before restarting a crashed pipeline

	Ports map[string]int // language code → UDP port for incoming RTP
}

// DefaultPorts returns the 27/35 predefined languages with the UDP ports
// that are statically configured in Janus' rtp_forward.
func DefaultPorts() map[string]int {
	return map[string]int{
		"amh": 24548, "ara": 24554, "arm": 24558, "aze": 24568,
		"bul": 24500, "chn": 24546, "cro": 24532, "dan": 24560,
		"dut": 24544, "eng": 24502, "est": 24562, "fre": 24504,
		"geo": 24526, "ger": 24506, "gre": 24564, "heb": 24508,
		"hin": 24550, "hun": 24528, "ind": 24556, "ita": 24510,
		"jpn": 24534, "lat": 24542, "lit": 24512, "nor": 24540,
		"per": 24552, "pol": 24538, "por": 24524, "rom": 24514,
		"rus": 24516, "slo": 24536, "spa": 24518, "swe": 24530,
		"tgl": 24566, "trk": 24520, "ukr": 24522,
	}
}

// Load reads environment variables and produces the final configuration.
// Returns an error if any required parameter is missing or invalid.
func Load() (*Config, error) {
	cfg := &Config{
		MQTTClientIDPrefix: getenv("MQTT_CLIENT_ID_PREFIX", "trl-proxy"),
		MQTTKeepAlive:      mustDuration("MQTT_KEEPALIVE", 15*time.Second),
		MQTTConnectTimeout: mustDuration("MQTT_CONNECT_TIMEOUT", 10*time.Second),

		JanusRTPBindAddr: os.Getenv("JANUS_RTP_BIND_ADDR"),

		MediaMTXAPI:          os.Getenv("MEDIAMTX_API"),
		MediaMTXRTSPBase:     os.Getenv("MEDIAMTX_RTSP"),
		MediaMTXPingInterval: mustDuration("MEDIAMTX_PING_INTERVAL", 2*time.Second),
		MediaMTXPingTimeout:  mustDuration("MEDIAMTX_PING_TIMEOUT", 1*time.Second),
		MediaMTXKickTimeout:  mustDuration("MEDIAMTX_KICK_TIMEOUT", 2*time.Second),
		MediaMTXKickPause:    mustDuration("MEDIAMTX_KICK_PAUSE", 200*time.Millisecond),

		HealthHTTPListen:   getenv("HEALTH_HTTP_LISTEN", ":9090"),
		RTPHealthThreshold: mustDuration("RTP_HEALTH_THRESHOLD", 3*time.Second),

		LogDir:   getenv("LOG_DIR", "./logs"),
		LogLevel: strings.ToLower(getenv("LOG_LEVEL", "info")),

		RoleAntiflap: mustDuration("ROLE_ANTIFLAP", 5*time.Second),
		RoleStartup:  strings.ToLower(getenv("ROLE_STARTUP", "standby")),

		GainLinear:   mustFloat("GAIN_LINEAR", 0.5),
		JitterMs:     mustInt("JITTER_MS", 200),
		PayloadType:  mustInt("PAYLOAD_TYPE", 100),
		OpusBitrate:  mustInt("OPUS_BITRATE", 64000),
		RestartDelay: mustDuration("RESTART_DELAY", 2*time.Second),

		Ports: DefaultPorts(),
	}

	nodeIDStr := os.Getenv("NODE_ID")
	if nodeIDStr == "" {
		return nil, errors.New("NODE_ID must be set (1 or 2)")
	}
	nodeID, err := strconv.Atoi(nodeIDStr)
	if err != nil {
		return nil, fmt.Errorf("invalid NODE_ID %q: %w", nodeIDStr, err)
	}
	if nodeID != 1 && nodeID != 2 {
		return nil, fmt.Errorf("NODE_ID must be 1 or 2, got %d", nodeID)
	}
	cfg.NodeID = nodeID

	brokersRaw := os.Getenv("MQTT_BROKERS")
	if strings.TrimSpace(brokersRaw) == "" {
		return nil, errors.New("MQTT_BROKERS must be set (comma-separated host:port list)")
	}
	for _, b := range strings.Split(brokersRaw, ",") {
		b = strings.TrimSpace(b)
		if b == "" {
			continue
		}
		cfg.MQTTBrokers = append(cfg.MQTTBrokers, b)
	}
	if len(cfg.MQTTBrokers) == 0 {
		return nil, errors.New("MQTT_BROKERS is empty after parsing")
	}

	cfg.MQTTUser = os.Getenv("MQTT_USER")
	cfg.MQTTPass = os.Getenv("MQTT_PASS")

	if cfg.MediaMTXAPI == "" {
		return nil, errors.New("MEDIAMTX_API must be set (e.g. http://mediamtx.internal:9997)")
	}
	if cfg.MediaMTXRTSPBase == "" {
		return nil, errors.New("MEDIAMTX_RTSP must be set (e.g. rtsp://mediamtx.internal:8554/galaxy)")
	}
	cfg.MediaMTXRTSPBase = strings.TrimRight(cfg.MediaMTXRTSPBase, "/")
	cfg.MediaMTXAPI = strings.TrimRight(cfg.MediaMTXAPI, "/")

	switch cfg.RoleStartup {
	case "standby", "active":
	default:
		return nil, fmt.Errorf("ROLE_STARTUP must be 'standby' or 'active', got %q", cfg.RoleStartup)
	}

	switch cfg.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return nil, fmt.Errorf("LOG_LEVEL must be debug|info|warn|error, got %q", cfg.LogLevel)
	}

	return cfg, nil
}

// MQTTClientID returns the client_id used when connecting to the broker.
func (c *Config) MQTTClientID() string {
	return fmt.Sprintf("%s-%d", c.MQTTClientIDPrefix, c.NodeID)
}

// TopicJanusStatus is the retained status topic of the neighbouring Janus sidecar.
func (c *Config) TopicJanusStatus() string {
	return fmt.Sprintf("janus/trl%d/status", c.NodeID)
}

// TopicProxyStatus is the retained status topic of this proxy (online/offline + LWT).
func (c *Config) TopicProxyStatus() string {
	return fmt.Sprintf("trl/proxy/%d/status", c.NodeID)
}

// TopicRoleCommand is the retained topic for commands from keepalived (active|standby).
func (c *Config) TopicRoleCommand() string {
	return fmt.Sprintf("trl/proxy/%d/role/command", c.NodeID)
}

// TopicRoleCurrent is the retained topic where we echo the current role (JSON snapshot).
func (c *Config) TopicRoleCurrent() string {
	return fmt.Sprintf("trl/proxy/%d/role/current", c.NodeID)
}

// RTSPMountpoint returns the full RTSP URL for publishing into MediaMTX.
func (c *Config) RTSPMountpoint(lang string) string {
	return fmt.Sprintf("%s/trl_%s", c.MediaMTXRTSPBase, lang)
}

// MediaMTXPathName returns the path name as the MediaMTX API sees it (no scheme/host).
// For "rtsp://host:8554/galaxy/trl_eng" the path is "galaxy/trl_eng".
func (c *Config) MediaMTXPathName(lang string) string {
	idx := strings.Index(c.MediaMTXRTSPBase, "://")
	base := c.MediaMTXRTSPBase
	if idx >= 0 {
		base = c.MediaMTXRTSPBase[idx+3:]
	}
	slash := strings.Index(base, "/")
	if slash < 0 {
		return fmt.Sprintf("trl_%s", lang)
	}
	return fmt.Sprintf("%s/trl_%s", strings.Trim(base[slash:], "/"), lang)
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func mustInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		panic(fmt.Errorf("invalid %s=%q: %w", key, v, err))
	}
	return n
}

func mustFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		panic(fmt.Errorf("invalid %s=%q: %w", key, v, err))
	}
	return f
}

func mustDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		panic(fmt.Errorf("invalid %s=%q (use Go duration like 5s, 200ms): %w", key, v, err))
	}
	return d
}
