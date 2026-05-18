// Package health aggregates pipeline health signals and exposes them over
// HTTP /health.
//
// Sources:
//   - JanusOnline       — from MQTT trl/janus/{N}/status (true if "online").
//   - LastRTPUnixNano   — updated by the pipeline on every received RTP packet.
//   - MediaMTXReachable — set by a background goroutine that pings MediaMTX.
//
// The proxy is healthy when all three are true AND the last RTP packet is
// no older than RTPThreshold.
package health

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Aggregator is a thread-safe accumulator of health signals.
type Aggregator struct {
	mu                sync.RWMutex
	janusOnline       bool
	mediamtxReachable bool

	lastRTPUnixNano atomic.Int64 // updated on the hot path from a pad probe

	rtpThreshold time.Duration

	// Diagnostic details surfaced via /health (last mediamtx error, raw janus
	// payload, etc.) — useful when triaging incidents.
	janusPayload      string
	mediamtxLastErr   string
	mediamtxLastCheck time.Time
}

// New creates an Aggregator with the given "RTP freshness" threshold.
func New(rtpThreshold time.Duration) *Aggregator {
	return &Aggregator{rtpThreshold: rtpThreshold}
}

// SetJanusOnline is invoked when a new MQTT trl/janus/{N}/status message arrives.
// The raw payload is stored for /health to surface.
func (a *Aggregator) SetJanusOnline(online bool, payload string) {
	a.mu.Lock()
	a.janusOnline = online
	a.janusPayload = payload
	a.mu.Unlock()
}

// SetMediaMTX records the result of the latest MediaMTX API ping.
func (a *Aggregator) SetMediaMTX(ok bool, errMsg string) {
	a.mu.Lock()
	a.mediamtxReachable = ok
	a.mediamtxLastErr = errMsg
	a.mediamtxLastCheck = time.Now()
	a.mu.Unlock()
}

// TouchRTP is called from a pad probe on every received RTP packet.
// Must stay extremely cheap — just an atomic store.
func (a *Aggregator) TouchRTP() {
	a.lastRTPUnixNano.Store(time.Now().UnixNano())
}

// Snapshot returns an immutable snapshot of the current state.
func (a *Aggregator) Snapshot() Snapshot {
	a.mu.RLock()
	defer a.mu.RUnlock()

	lastRTPNs := a.lastRTPUnixNano.Load()
	var lastRTP time.Time
	if lastRTPNs > 0 {
		lastRTP = time.Unix(0, lastRTPNs)
	}

	return Snapshot{
		JanusOnline:       a.janusOnline,
		JanusPayload:      a.janusPayload,
		MediaMTXReachable: a.mediamtxReachable,
		MediaMTXLastErr:   a.mediamtxLastErr,
		MediaMTXLastCheck: a.mediamtxLastCheck,
		LastRTP:           lastRTP,
		RTPThreshold:      a.rtpThreshold,
	}
}

// Healthy returns the integral check result, plus a reason string when unhealthy.
func (a *Aggregator) Healthy() (bool, string) {
	s := a.Snapshot()
	return s.Healthy()
}

// Snapshot is an immutable view of the aggregator state.
type Snapshot struct {
	JanusOnline       bool
	JanusPayload      string
	MediaMTXReachable bool
	MediaMTXLastErr   string
	MediaMTXLastCheck time.Time
	LastRTP           time.Time
	RTPThreshold      time.Duration
}

// Healthy verifies all three health signals are good.
func (s Snapshot) Healthy() (bool, string) {
	if !s.JanusOnline {
		return false, "janus_offline"
	}
	if !s.MediaMTXReachable {
		return false, "mediamtx_unreachable"
	}
	if s.LastRTP.IsZero() {
		return false, "no_rtp_yet"
	}
	if age := time.Since(s.LastRTP); age > s.RTPThreshold {
		return false, "rtp_stale"
	}
	return true, ""
}

// Handler returns the http.Handler for /health.
// 200 + JSON when healthy; 503 + JSON with a "reason" field otherwise.
func (a *Aggregator) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := a.Snapshot()
		ok, reason := s.Healthy()

		resp := map[string]any{
			"status":             "ok",
			"janus_online":       s.JanusOnline,
			"mediamtx_reachable": s.MediaMTXReachable,
		}
		if !s.LastRTP.IsZero() {
			resp["rtp_age_ms"] = time.Since(s.LastRTP).Milliseconds()
			resp["rtp_last_ts"] = s.LastRTP.Format(time.RFC3339Nano)
		} else {
			resp["rtp_age_ms"] = nil
		}
		if s.JanusPayload != "" {
			resp["janus_payload"] = s.JanusPayload
		}
		if s.MediaMTXLastErr != "" {
			resp["mediamtx_last_err"] = s.MediaMTXLastErr
		}
		if !s.MediaMTXLastCheck.IsZero() {
			resp["mediamtx_last_check"] = s.MediaMTXLastCheck.Format(time.RFC3339Nano)
		}

		if !ok {
			resp["status"] = "unhealthy"
			resp["reason"] = reason
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
}
