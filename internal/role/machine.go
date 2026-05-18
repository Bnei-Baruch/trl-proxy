// Package role implements the state machine that toggles the proxy between
// the "active" and "standby" roles based on MQTT commands.
//
// Contract:
//   - commands enter exclusively via Apply(cmd);
//   - a repeated command for the same role is a no-op (echo is still refreshed);
//   - commands arriving faster than the anti-flap interval are ignored;
//   - active transition  : KickAll → pause → OpenAll, then echo;
//   - standby transition : CloseAll, then echo.
package role

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Role lists the acceptable proxy roles.
type Role string

const (
	RoleUnknown Role = ""
	RoleActive  Role = "active"
	RoleStandby Role = "standby"
)

// ParseRole parses a role/command payload. An empty/unknown string yields RoleUnknown.
func ParseRole(s string) Role {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "active":
		return RoleActive
	case "standby":
		return RoleStandby
	default:
		return RoleUnknown
	}
}

// EgressController is the contract the pipeline manager exposes for the state machine.
type EgressController interface {
	OpenAll() error
	CloseAll() error
	OpenSessions() int
	PathNames() []string
}

// MediaMTXKicker is the contract for the MediaMTX client used to kick zombie publishers.
type MediaMTXKicker interface {
	KickAllOnPaths(ctx context.Context, paths []string) error
}

// EchoPublisher publishes the JSON role snapshot to MQTT (retained).
type EchoPublisher func(snapshot EchoSnapshot) error

// EchoSnapshot is the payload sent to trl/proxy/{N}/role/current.
type EchoSnapshot struct {
	Role          Role      `json:"role"`
	Since         time.Time `json:"since"`
	Sessions      int       `json:"sessions"`
	LastError     string    `json:"last_error,omitempty"`
	LastTransitMs int64     `json:"last_transit_ms,omitempty"`
}

// Config bundles the machine parameters.
type Config struct {
	Initial      Role          // default role (RoleStandby recommended)
	AntiFlap     time.Duration // minimum interval between role transitions
	KickTimeout  time.Duration // timeout for kicking zombie publishers
	KickPause    time.Duration // pause between kick and OpenAll
	EchoOnSetup  bool          // also publish echo on the very first role bootstrap
	OnTransition func(from, to Role, reason string, durMs int64, err error)
}

// Machine is the role state machine.
type Machine struct {
	cfg     Config
	egress  EgressController
	kicker  MediaMTXKicker
	echo    EchoPublisher
	log     *slog.Logger

	mu         sync.Mutex
	current    Role
	since      time.Time
	lastChange time.Time
	lastErr    string
	lastDurMs  int64
}

// New constructs the machine. Bootstrap(ctx) must be called once after creation.
func New(cfg Config, egress EgressController, kicker MediaMTXKicker, echo EchoPublisher, log *slog.Logger) *Machine {
	if cfg.Initial == RoleUnknown {
		cfg.Initial = RoleStandby
	}
	return &Machine{
		cfg:     cfg,
		egress:  egress,
		kicker:  kicker,
		echo:    echo,
		log:     log,
		current: RoleUnknown,
	}
}

// Bootstrap applies the initial role (anti-flap is bypassed). Call once at startup.
func (m *Machine) Bootstrap(ctx context.Context) {
	m.mu.Lock()
	if m.current != RoleUnknown {
		m.mu.Unlock()
		return
	}
	target := m.cfg.Initial
	m.mu.Unlock()

	m.log.Info("role bootstrap", "initial", target)
	m.applyInternal(ctx, target, "bootstrap", true)
}

// Apply processes a command coming from MQTT. Idempotent, anti-flap, thread-safe.
// reason is recorded in logs (e.g. "mqtt_command").
func (m *Machine) Apply(ctx context.Context, target Role, reason string) {
	if target == RoleUnknown {
		m.log.Warn("role command ignored: unknown role", "reason", reason)
		return
	}
	m.applyInternal(ctx, target, reason, false)
}

// Current returns the role the machine is currently in (handy for callers).
func (m *Machine) Current() Role {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current
}

func (m *Machine) applyInternal(ctx context.Context, target Role, reason string, isBootstrap bool) {
	m.mu.Lock()
	current := m.current
	since := m.since
	lastChange := m.lastChange
	m.mu.Unlock()

	if !isBootstrap && current == target {
		m.log.Info("role command no-op (already in target)", "role", target, "reason", reason)
		m.publishEcho(target, since, "")
		return
	}
	if !isBootstrap && !lastChange.IsZero() {
		elapsed := time.Since(lastChange)
		if elapsed < m.cfg.AntiFlap {
			m.log.Warn("role command ignored by anti-flap",
				"target", target, "current", current,
				"elapsed_ms", elapsed.Milliseconds(),
				"antiflap_ms", m.cfg.AntiFlap.Milliseconds(),
				"reason", reason)
			return
		}
	}

	start := time.Now()
	var transitionErr error

	switch target {
	case RoleActive:
		transitionErr = m.toActive(ctx)
	case RoleStandby:
		transitionErr = m.toStandby(ctx)
	}

	dur := time.Since(start)
	now := time.Now()

	m.mu.Lock()
	prev := m.current
	m.current = target
	m.since = now
	m.lastChange = now
	m.lastDurMs = dur.Milliseconds()
	if transitionErr != nil {
		m.lastErr = transitionErr.Error()
	} else {
		m.lastErr = ""
	}
	m.mu.Unlock()

	if transitionErr != nil {
		m.log.Error("role transition failed",
			"from", prev, "to", target,
			"reason", reason,
			"duration_ms", dur.Milliseconds(),
			"err", transitionErr)
	} else {
		m.log.Info("role transition done",
			"from", prev, "to", target,
			"reason", reason,
			"duration_ms", dur.Milliseconds())
	}

	if m.cfg.OnTransition != nil {
		m.cfg.OnTransition(prev, target, reason, dur.Milliseconds(), transitionErr)
	}

	m.publishEcho(target, now, m.errString(transitionErr))
}

func (m *Machine) toActive(parent context.Context) error {
	if m.kicker != nil {
		kickCtx, cancel := context.WithTimeout(parent, m.cfg.KickTimeout)
		paths := m.egress.PathNames()
		err := m.kicker.KickAllOnPaths(kickCtx, paths)
		cancel()
		if err != nil {
			m.log.Warn("kick zombies failed (continuing anyway)", "err", err)
		} else {
			m.log.Info("kicked zombie publishers (if any)", "paths_checked", len(paths))
		}
	}

	if m.cfg.KickPause > 0 {
		select {
		case <-parent.Done():
			return parent.Err()
		case <-time.After(m.cfg.KickPause):
		}
	}

	if err := m.egress.OpenAll(); err != nil {
		return fmt.Errorf("open all egress: %w", err)
	}
	return nil
}

func (m *Machine) toStandby(_ context.Context) error {
	if err := m.egress.CloseAll(); err != nil {
		return fmt.Errorf("close all egress: %w", err)
	}
	return nil
}

func (m *Machine) publishEcho(role Role, since time.Time, lastErr string) {
	if m.echo == nil {
		return
	}
	m.mu.Lock()
	durMs := m.lastDurMs
	m.mu.Unlock()

	snap := EchoSnapshot{
		Role:          role,
		Since:         since,
		Sessions:      m.egress.OpenSessions(),
		LastError:     lastErr,
		LastTransitMs: durMs,
	}
	if err := m.echo(snap); err != nil {
		m.log.Warn("role echo publish failed", "err", err)
	}
}

func (m *Machine) errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
