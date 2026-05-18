// Command trlproxy is one leg of the HA audio-translation pipeline.
//
// It receives RTP from its local Janus, keeps a warm standby (decoding and
// encoding are always running), and switches between active/standby based on
// MQTT commands issued by keepalived. While active it publishes 27 streams
// into MediaMTX over RTSP.
//
// Per-leg health is aggregated in HealthAggregator and exposed over HTTP
// /health, which keepalived polls via a local track_script.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"trl-proxy/internal/config"
	"trl-proxy/internal/health"
	"trl-proxy/internal/logx"
	"trl-proxy/internal/mediamtx"
	"trl-proxy/internal/mqttx"
	"trl-proxy/internal/pipeline"
	"trl-proxy/internal/role"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	closeLog, err := logx.Init(cfg.LogDir, cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	defer closeLog()

	log := slog.Default().With("node_id", cfg.NodeID)
	log.Info("trl-proxy starting",
		"mqtt_brokers", cfg.MQTTBrokers,
		"mediamtx_api", cfg.MediaMTXAPI,
		"mediamtx_rtsp", cfg.MediaMTXRTSPBase,
		"health_addr", cfg.HealthHTTPListen,
		"log_dir", cfg.LogDir,
		"log_level", cfg.LogLevel,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Info("signal received, shutting down", "signal", sig.String())
		cancel()
	}()

	agg := health.New(cfg.RTPHealthThreshold)

	mediamtxClient := mediamtx.NewClient(cfg.MediaMTXAPI, cfg.MediaMTXPingTimeout)
	pinger := mediamtx.NewPinger(
		mediamtxClient,
		agg,
		cfg.MediaMTXPingInterval,
		cfg.MediaMTXPingTimeout,
		log.With("component", "mediamtx_pinger"),
	)

	httpServer := health.NewServer(cfg.HealthHTTPListen, agg, log.With("component", "health_http"))

	mgr, err := pipeline.NewManager(cfg, agg, log.With("component", "pipeline"))
	if err != nil {
		return fmt.Errorf("init pipeline manager: %w", err)
	}
	defer mgr.Close()

	mqttClient, err := setupMQTT(ctx, cfg, log.With("component", "mqtt"), agg)
	if err != nil {
		return fmt.Errorf("setup mqtt: %w", err)
	}
	defer mqttClient.Disconnect()

	echoPublisher := func(snap role.EchoSnapshot) error {
		payload := map[string]any{
			"role":            string(snap.Role),
			"since":           snap.Since.Format(time.RFC3339Nano),
			"sessions":        snap.Sessions,
			"last_transit_ms": snap.LastTransitMs,
		}
		if snap.LastError != "" {
			payload["last_error"] = snap.LastError
		}
		return mqttClient.PublishJSON(cfg.TopicRoleCurrent(), payload)
	}

	roleMachine := role.New(
		role.Config{
			Initial:     role.ParseRole(cfg.RoleStartup),
			AntiFlap:    cfg.RoleAntiflap,
			KickTimeout: cfg.MediaMTXKickTimeout,
			KickPause:   cfg.MediaMTXKickPause,
		},
		mgr,
		mediamtxClient,
		echoPublisher,
		log.With("component", "role"),
	)

	// Single-consumer worker for role commands. The MQTT message handler must
	// stay non-blocking (paho v3: handlers are short-lived even with
	// OrderMatters=false; long work would otherwise spawn unbounded goroutines
	// per incoming message). We copy the parsed role into a buffered channel
	// and let one goroutine drive roleMachine.Apply sequentially.
	roleCmdCh := make(chan role.Role, 16)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case target := <-roleCmdCh:
				roleMachine.Apply(ctx, target, "mqtt_command")
			}
		}
	}()

	if err := mqttClient.Subscribe(cfg.TopicRoleCommand(), 1, func(topic string, payload []byte) {
		s := strings.TrimSpace(string(payload))
		target := role.ParseRole(s)
		log.Info("role command received", "topic", topic, "payload", s, "parsed", target)
		select {
		case roleCmdCh <- target:
		default:
			log.Warn("role command channel full, dropping",
				"target", target, "channel_cap", cap(roleCmdCh))
		}
	}); err != nil {
		return fmt.Errorf("subscribe role command: %w", err)
	}
	if err := mqttClient.Subscribe(cfg.TopicJanusStatus(), 1, func(topic string, payload []byte) {
		online := parseJanusStatus(payload)
		log.Info("janus status received", "topic", topic, "online", online, "payload", string(payload))
		agg.SetJanusOnline(online, string(payload))
	}); err != nil {
		return fmt.Errorf("subscribe janus status: %w", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		pinger.Run(ctx)
	}()
	httpServer.Start()

	wg.Add(1)
	go func() {
		defer wg.Done()
		mgr.Run(ctx)
	}()

	roleMachine.Bootstrap(ctx)
	log.Info("trl-proxy ready")

	<-ctx.Done()
	log.Info("trl-proxy shutting down")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)

	if err := mgr.CloseAll(); err != nil {
		log.Warn("close all egress during shutdown", "err", err)
	}

	wg.Wait()
	log.Info("trl-proxy stopped")
	return nil
}

func setupMQTT(ctx context.Context, cfg *config.Config, log *slog.Logger, agg *health.Aggregator) (*mqttx.Client, error) {
	_ = agg

	onlinePayload, _ := json.Marshal(map[string]any{
		"status":     "online",
		"node_id":    cfg.NodeID,
		"started_at": time.Now().UTC().Format(time.RFC3339Nano),
	})

	client := mqttx.New(mqttx.Options{
		Brokers:        cfg.MQTTBrokers,
		ClientID:       cfg.MQTTClientID(),
		Username:       cfg.MQTTUser,
		Password:       cfg.MQTTPass,
		KeepAlive:      cfg.MQTTKeepAlive,
		ConnectTimeout: cfg.MQTTConnectTimeout,
		WillTopic:      cfg.TopicProxyStatus(),
		WillPayload:    []byte(`{"status":"offline"}`),
		OnlineTopic:    cfg.TopicProxyStatus(),
		OnlinePayload:  onlinePayload,
	}, log)

	connectCtx, cancel := context.WithTimeout(ctx, cfg.MQTTConnectTimeout)
	defer cancel()
	if err := client.Connect(connectCtx); err != nil {
		return nil, err
	}
	return client, nil
}

// parseJanusStatus decodes the payload of trl/janus/{N}/status.
// Supported formats:
//   - plain "online" / "offline"
//   - JSON with a "status" string field ("online" | "offline")
//   - JSON with an "online" bool field
//
// Any parse error → online=false.
func parseJanusStatus(payload []byte) bool {
	s := strings.TrimSpace(string(payload))
	if s == "" {
		return false
	}
	lower := strings.ToLower(s)
	if lower == "online" {
		return true
	}
	if lower == "offline" {
		return false
	}
	var obj map[string]any
	if err := json.Unmarshal(payload, &obj); err == nil {
		if v, ok := obj["status"].(string); ok {
			return strings.EqualFold(v, "online")
		}
		if v, ok := obj["online"].(bool); ok {
			return v
		}
	}
	return false
}
