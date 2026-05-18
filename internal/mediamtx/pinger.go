package mediamtx

import (
	"context"
	"log/slog"
	"time"
)

// HealthSink is the contract for forwarding ping results into HealthAggregator.
type HealthSink interface {
	SetMediaMTX(ok bool, errMsg string)
}

// Pinger periodically pings the MediaMTX API and reports results to HealthSink.
type Pinger struct {
	client   *Client
	sink     HealthSink
	interval time.Duration
	timeout  time.Duration
	log      *slog.Logger
}

// NewPinger builds a pinger. interval is the gap between pings, timeout is
// the per-request deadline.
func NewPinger(client *Client, sink HealthSink, interval, timeout time.Duration, log *slog.Logger) *Pinger {
	return &Pinger{
		client:   client,
		sink:     sink,
		interval: interval,
		timeout:  timeout,
		log:      log,
	}
}

// Run blocks until ctx is cancelled, issuing periodic pings.
func (p *Pinger) Run(ctx context.Context) {
	p.pingOnce(ctx)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.pingOnce(ctx)
		}
	}
}

func (p *Pinger) pingOnce(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, p.timeout)
	defer cancel()

	err := p.client.Ping(ctx)
	if err != nil {
		p.sink.SetMediaMTX(false, err.Error())
		p.log.Warn("mediamtx ping failed", "err", err)
		return
	}
	p.sink.SetMediaMTX(true, "")
	p.log.Debug("mediamtx ping ok")
}
