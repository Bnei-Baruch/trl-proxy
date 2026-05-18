// Package pipeline encapsulates the per-language GStreamer pipelines.
//
// One Worker's layout:
//
//	udpsrc → caps → rtpjitterbuffer → rtpopusdepay → opusdec → audioconvert
//	      → audioresample → volume → opusenc → tee name=t (allow-not-linked=true)
//
//	t. ! queue ! fakesink                       (idle branch, always linked)
//	t. ! queue ! rtspclientsink                 (egress branch, dynamically
//	                                             added/removed based on role)
//
// The ingress part is always PLAYING. This provides a warm standby (decoder
// is hot, encoder is primed) and lets a pad probe on udpsrc.src update
// lastRTP in HealthAggregator on every received packet.
package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-gst/go-gst/gst"

	"trl-proxy/internal/config"
	"trl-proxy/internal/health"
)

// Worker owns a single pipeline (1 language = 1 worker).
type Worker struct {
	cfg    *config.Config
	lang   string
	port   int
	health *health.Aggregator
	log    *slog.Logger

	mu       sync.Mutex
	pipeline *gst.Pipeline
	tee      *gst.Element
	udpsrc   *gst.Element
	probeID  uint64

	egressMu     sync.Mutex
	egressBranch *egressBranch // nil = standby; non-nil = active

	restartCount atomic.Int64
}

// egressBranch is the dynamically-built egress chain.
type egressBranch struct {
	queue        *gst.Element
	rtspsink     *gst.Element
	teePad       *gst.Pad
	queueSinkPad *gst.Pad
}

// NewWorker constructs a worker. The logger should carry a "lang" attribute.
func NewWorker(cfg *config.Config, lang string, port int, h *health.Aggregator, log *slog.Logger) *Worker {
	return &Worker{
		cfg:    cfg,
		lang:   lang,
		port:   port,
		health: h,
		log:    log,
	}
}

// Lang returns the language code (used for log attributes).
func (w *Worker) Lang() string { return w.lang }

// Run drives the worker in an infinite loop with automatic restart on errors.
// It only exits when ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	w.log.Info("worker starting", "port", w.port)
	for {
		select {
		case <-ctx.Done():
			w.log.Info("worker stopping (context done)")
			return
		default:
		}

		err := w.runOnce(ctx)
		count := w.restartCount.Add(1)

		if err != nil {
			if ctx.Err() != nil {
				w.log.Info("worker stopped (context done)", "restart_count", count)
				return
			}
			w.log.Error("pipeline crashed, will restart", "err", err, "restart_count", count)
		} else {
			w.log.Info("pipeline EOS, will restart", "restart_count", count)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(w.cfg.RestartDelay):
		}
	}
}

// runOnce builds the pipeline and watches its bus until error/EOS/cancel.
func (w *Worker) runOnce(ctx context.Context) error {
	pipelineStr := w.buildPipelineString()
	w.log.Debug("creating pipeline", "desc", pipelineStr)

	p, err := gst.NewPipelineFromString(pipelineStr)
	if err != nil {
		return fmt.Errorf("parse pipeline: %w", err)
	}
	defer p.Unref()

	tee, err := p.GetElementByName("t")
	if err != nil {
		return fmt.Errorf("get tee element: %w", err)
	}
	udpsrc, err := p.GetElementByName("udpsrc")
	if err != nil {
		return fmt.Errorf("get udpsrc element: %w", err)
	}

	w.mu.Lock()
	w.pipeline = p
	w.tee = tee
	w.udpsrc = udpsrc
	w.mu.Unlock()

	// Pad probe to track RTP freshness: every buffer triggers a cheap
	// atomic.Store via HealthAggregator.TouchRTP().
	srcPad := udpsrc.GetStaticPad("src")
	if srcPad == nil {
		return fmt.Errorf("get udpsrc src pad")
	}
	probeID := srcPad.AddProbe(gst.PadProbeTypeBuffer, func(_ *gst.Pad, _ *gst.PadProbeInfo) gst.PadProbeReturn {
		w.health.TouchRTP()
		return gst.PadProbeOK
	})
	w.mu.Lock()
	w.probeID = probeID
	w.mu.Unlock()

	// If the egress branch was active before this pipeline was rebuilt, the
	// stored reference is stale — drop it and rebuild after PLAYING.
	w.egressMu.Lock()
	wasActive := w.egressBranch != nil
	w.egressBranch = nil
	w.egressMu.Unlock()

	bus := p.GetBus()
	defer bus.Unref()

	w.log.Info("setting pipeline to PLAYING")
	if err := p.SetState(gst.StatePlaying); err != nil {
		return fmt.Errorf("set state PLAYING: %w", err)
	}
	defer func() {
		w.log.Debug("setting pipeline to NULL")
		_ = p.SetState(gst.StateNull)
	}()

	if wasActive {
		w.log.Info("restoring egress after pipeline restart")
		if err := w.OpenEgress(); err != nil {
			w.log.Warn("failed to restore egress", "err", err)
		}
	}

	const oneSecond gst.ClockTime = 1_000_000_000
	for {
		select {
		case <-ctx.Done():
			w.log.Debug("context done, sending EOS")
			p.SendEvent(gst.NewEOSEvent())
			return context.Canceled
		default:
		}

		msg := bus.TimedPopFiltered(oneSecond, gst.MessageError|gst.MessageEOS|gst.MessageWarning)
		if msg == nil {
			continue
		}
		switch msg.Type() {
		case gst.MessageError:
			gerr := msg.ParseError()
			msg.Unref()
			return fmt.Errorf("pipeline error: %s", gerr.Error())
		case gst.MessageEOS:
			msg.Unref()
			return nil
		case gst.MessageWarning:
			gwarn := msg.ParseWarning()
			w.log.Warn("pipeline warning", "msg", gwarn.Error())
			msg.Unref()
		default:
			msg.Unref()
		}
	}
}

// buildPipelineString renders the gst-launch description.
// The ingress part is always present; the egress branch is added dynamically.
func (w *Worker) buildPipelineString() string {
	addr := ""
	if w.cfg.JanusRTPBindAddr != "" {
		addr = fmt.Sprintf(" address=%s", w.cfg.JanusRTPBindAddr)
	}
	return fmt.Sprintf(
		"udpsrc name=udpsrc port=%d%s "+
			"caps=\"application/x-rtp,media=audio,clock-rate=48000,encoding-name=OPUS,payload=%d\" ! "+
			"rtpjitterbuffer latency=%d drop-on-latency=true post-drop-messages=true ! "+
			"rtpopusdepay ! "+
			"opusdec plc=true ! "+
			"audioconvert ! "+
			"audioresample ! "+
			"volume volume=%.3f ! "+
			"opusenc bitrate=%d ! "+
			"tee name=t allow-not-linked=true ! "+
			"queue ! fakesink sync=false async=false",
		w.port,
		addr,
		w.cfg.PayloadType,
		w.cfg.JitterMs,
		w.cfg.GainLinear,
		w.cfg.OpusBitrate,
	)
}

// OpenEgress dynamically adds a tee → queue → rtspclientsink branch.
// Idempotent: if the egress is already open, this is a no-op.
func (w *Worker) OpenEgress() error {
	w.egressMu.Lock()
	defer w.egressMu.Unlock()

	if w.egressBranch != nil {
		w.log.Debug("OpenEgress: already open")
		return nil
	}

	w.mu.Lock()
	pipeline := w.pipeline
	tee := w.tee
	w.mu.Unlock()
	if pipeline == nil || tee == nil {
		return fmt.Errorf("OpenEgress: pipeline not ready")
	}

	location := w.cfg.RTSPMountpoint(w.lang)
	w.log.Info("opening egress", "location", location)

	queue, err := gst.NewElement("queue")
	if err != nil {
		return fmt.Errorf("create queue: %w", err)
	}
	rtspsink, err := gst.NewElement("rtspclientsink")
	if err != nil {
		return fmt.Errorf("create rtspclientsink: %w", err)
	}
	if err := rtspsink.SetProperty("location", location); err != nil {
		return fmt.Errorf("set rtspclientsink location: %w", err)
	}

	if err := pipeline.AddMany(queue, rtspsink); err != nil {
		return fmt.Errorf("add elements to pipeline: %w", err)
	}
	if err := queue.Link(rtspsink); err != nil {
		_ = pipeline.RemoveMany(queue, rtspsink)
		return fmt.Errorf("link queue→rtspsink: %w", err)
	}

	if !queue.SyncStateWithParent() {
		w.log.Warn("queue.SyncStateWithParent returned false")
	}
	if !rtspsink.SyncStateWithParent() {
		w.log.Warn("rtspsink.SyncStateWithParent returned false")
	}

	teePad := tee.GetRequestPad("src_%u")
	if teePad == nil {
		_ = pipeline.RemoveMany(queue, rtspsink)
		return fmt.Errorf("tee.GetRequestPad failed")
	}
	queueSink := queue.GetStaticPad("sink")
	if queueSink == nil {
		tee.ReleaseRequestPad(teePad)
		_ = pipeline.RemoveMany(queue, rtspsink)
		return fmt.Errorf("queue.GetStaticPad(sink) failed")
	}
	if ret := teePad.Link(queueSink); ret != gst.PadLinkOK {
		tee.ReleaseRequestPad(teePad)
		_ = pipeline.RemoveMany(queue, rtspsink)
		return fmt.Errorf("link tee→queue: %d", ret)
	}

	w.egressBranch = &egressBranch{
		queue:        queue,
		rtspsink:     rtspsink,
		teePad:       teePad,
		queueSinkPad: queueSink,
	}
	w.log.Info("egress opened")
	return nil
}

// CloseEgress tears the egress branch down cleanly: blocks the tee pad,
// sends EOS downstream so rtspclientsink can issue RTSP TEARDOWN, then drives
// the elements to NULL and removes them from the pipeline. Idempotent.
func (w *Worker) CloseEgress() error {
	w.egressMu.Lock()
	branch := w.egressBranch
	w.egressBranch = nil
	w.egressMu.Unlock()

	if branch == nil {
		w.log.Debug("CloseEgress: already closed")
		return nil
	}

	w.mu.Lock()
	pipeline := w.pipeline
	tee := w.tee
	w.mu.Unlock()
	if pipeline == nil || tee == nil {
		w.log.Warn("CloseEgress: pipeline gone, dropping branch refs")
		return nil
	}

	w.log.Info("closing egress")

	// Block the stream on tee.src — buffers will no longer flow into our
	// branch. PadProbeTypeIdle ensures the callback fires in an idle moment,
	// which is the safe spot to unlink.
	done := make(chan struct{}, 1)
	probeID := branch.teePad.AddProbe(gst.PadProbeTypeIdle, func(_ *gst.Pad, _ *gst.PadProbeInfo) gst.PadProbeReturn {
		select {
		case done <- struct{}{}:
		default:
		}
		return gst.PadProbeRemove
	})
	if probeID == 0 {
		w.log.Warn("CloseEgress: failed to install idle probe, proceeding without")
		close(done)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		w.log.Warn("CloseEgress: idle probe timeout, proceeding anyway")
	}

	branch.teePad.Unlink(branch.queueSinkPad)

	// EOS downstream — rtspclientsink will perform RTSP TEARDOWN.
	if !branch.queueSinkPad.SendEvent(gst.NewEOSEvent()) {
		w.log.Warn("CloseEgress: SendEvent(EOS) returned false")
	}

	// Give rtspclientsink a moment to actually emit TEARDOWN.
	// SetState(NULL) below would finish the session anyway, but with EOS
	// it's a cleaner shutdown.
	time.Sleep(150 * time.Millisecond)

	if err := branch.rtspsink.SetState(gst.StateNull); err != nil {
		w.log.Warn("CloseEgress: rtspsink → NULL", "err", err)
	}
	if err := branch.queue.SetState(gst.StateNull); err != nil {
		w.log.Warn("CloseEgress: queue → NULL", "err", err)
	}

	if err := pipeline.RemoveMany(branch.queue, branch.rtspsink); err != nil {
		w.log.Warn("CloseEgress: RemoveMany", "err", err)
	}
	tee.ReleaseRequestPad(branch.teePad)

	w.log.Info("egress closed")
	return nil
}

// IsEgressOpen reports whether the egress branch is currently open (for logs/echo).
func (w *Worker) IsEgressOpen() bool {
	w.egressMu.Lock()
	defer w.egressMu.Unlock()
	return w.egressBranch != nil
}
