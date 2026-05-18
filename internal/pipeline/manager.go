package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/go-gst/go-gst/gst"

	"trl-proxy/internal/config"
	"trl-proxy/internal/health"
	"trl-proxy/internal/logx"
)

// Manager orchestrates all Workers and implements the EgressController
// contract consumed by the role package.
type Manager struct {
	cfg     *config.Config
	health  *health.Aggregator
	log     *slog.Logger
	workers []*Worker
	closers []func()
	wg      sync.WaitGroup
}

// NewManager initialises GStreamer, builds workers and their loggers.
func NewManager(cfg *config.Config, h *health.Aggregator, log *slog.Logger) (*Manager, error) {
	gst.Init(nil)

	m := &Manager{
		cfg:    cfg,
		health: h,
		log:    log,
	}

	langs := make([]string, 0, len(cfg.Ports))
	for lang := range cfg.Ports {
		langs = append(langs, lang)
	}
	sort.Strings(langs)

	for _, lang := range langs {
		port := cfg.Ports[lang]
		wlog, closer, err := logx.WorkerFileLogger(cfg.LogDir, lang, cfg.LogLevel)
		if err != nil {
			m.Close()
			return nil, fmt.Errorf("create logger for %s: %w", lang, err)
		}
		w := NewWorker(cfg, lang, port, h, wlog)
		m.workers = append(m.workers, w)
		m.closers = append(m.closers, closer)
	}
	m.log.Info("manager initialized", "workers", len(m.workers))
	return m, nil
}

// Run launches every worker in its own goroutine and blocks until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) {
	m.log.Info("starting workers", "count", len(m.workers))
	for _, w := range m.workers {
		w := w
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			w.Run(ctx)
		}()
	}
	m.wg.Wait()
	m.log.Info("all workers stopped")
}

// OpenAll opens the egress on every worker. Errors are collected without
// breaking the loop.
func (m *Manager) OpenAll() error {
	var errs []error
	for _, w := range m.workers {
		if err := w.OpenEgress(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", w.Lang(), err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// CloseAll closes the egress on every worker.
func (m *Manager) CloseAll() error {
	var errs []error
	for _, w := range m.workers {
		if err := w.CloseEgress(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", w.Lang(), err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// OpenSessions returns the number of workers whose egress is currently open.
func (m *Manager) OpenSessions() int {
	n := 0
	for _, w := range m.workers {
		if w.IsEgressOpen() {
			n++
		}
	}
	return n
}

// PathNames returns the MediaMTX path names for every language (used to kick zombies).
func (m *Manager) PathNames() []string {
	names := make([]string, 0, len(m.workers))
	for _, w := range m.workers {
		names = append(names, m.cfg.MediaMTXPathName(w.Lang()))
	}
	return names
}

// Close closes per-worker log files. Must be invoked after Run returns.
func (m *Manager) Close() {
	for _, c := range m.closers {
		c()
	}
}
