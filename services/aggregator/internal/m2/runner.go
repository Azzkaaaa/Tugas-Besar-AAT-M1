package m2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/ingest"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type Runner struct {
	Config  Config
	Store   *storage.Backend
	metrics *metrics
}
type metrics struct {
	inputs, parked                             metric.Int64Counter
	latency                                    metric.Float64Histogram
	offset, lag, fires, singles, late, breaker metric.Int64Gauge
}

func NewRunner(c Config, s *storage.Backend) *Runner {
	m := otel.Meter("bnpb.m2")
	v := &metrics{}
	v.inputs, _ = m.Int64Counter("bnpb.m2.inputs")
	v.parked, _ = m.Int64Counter("bnpb.m2.parked")
	v.latency, _ = m.Float64Histogram("bnpb.m2.ingest.duration", metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(.01, .025, .05, .1, .25, .5, 1, 2, 3, 5, 10, 30, 60))
	v.offset, _ = m.Int64Gauge("bnpb.satellite.offset")
	v.lag, _ = m.Int64Gauge("bnpb.satellite.offset.lag")
	v.fires, _ = m.Int64Gauge("bnpb.satellite.fires")
	v.singles, _ = m.Int64Gauge("bnpb.satellite.singletons")
	v.late, _ = m.Int64Gauge("bnpb.satellite.late")
	v.breaker, _ = m.Int64Gauge("bnpb.coastal.breaker.open")
	return &Runner{c, s, v}
}
func (r *Runner) save(ctx context.Context, h *domain.HazardRecord, corr string) error {
	_, _, err := r.Store.Hazards.ApplyRevision(ctx, h, func(saved *domain.HazardRecord) (storage.OutboxEvent, error) {
		id, payload, err := ingest.BuildPublishedEnvelope(saved, corr)
		return storage.OutboxEvent{EventID: id, HazardID: saved.HazardID, Source: saved.Source, Payload: payload, TraceContext: telemetry.TraceCarrier(ctx)}, err
	})
	return err
}
func (r *Runner) park(ctx context.Context, source string, raw []byte, reason string, received time.Time) error {
	var err error
	if r.Store.Inbox != nil {
		err = ingest.ParkExternal(ctx, r.Store.Inbox, source, raw, reason, stableID("input-", string(raw)), received)
	} else {
		err = r.Store.Streams.Park(ctx, storage.ParkedInput{MessageID: stableID("msg-", source+"|"+string(raw)), Source: source, Payload: raw, Reason: reason, ReceivedAt: received, Status: "parked", Attempts: 1})
	}
	if err == nil {
		r.metrics.parked.Add(ctx, 1, metric.WithAttributes(attribute.String("source", source)))
		slog.WarnContext(ctx, "input parked", "source", source, "reason", reason, "correlation_id", stableID("input-", string(raw)))
	}
	return err
}
func (r *Runner) commit(ctx context.Context, stream string, p *storage.StreamCheckpoint, e storage.StreamEntry) error {
	next := storage.StreamCheckpoint{Sequence: e.Sequence, Cursor: e.Cursor}
	if err := r.Store.Streams.Commit(ctx, stream, p.Sequence, next); err != nil {
		return err
	}
	*p = next
	return nil
}

// Satellite is a separate leader job. Each new term reconstructs state from the
// durable journal. Retrying a partly applied input repeats identical revisions.
func (r *Runner) Satellite(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go r.sampleLag(ctx)
	if err := r.satellite(ctx); err != nil && ctx.Err() == nil {
		slog.ErrorContext(ctx, "satellite job restarting", "error", err, "correlation_id", "satellite-recovery")
		_ = r.Store.Sources.RecordFailure(ctx, domain.SourceKeySatellite, err.Error())
		pause(ctx, time.Second)
	}
}

func (r *Runner) sampleLag(ctx context.Context) {
	c := client(2 * time.Second)
	c.Timeout = 2 * time.Second
	defer c.CloseIdleConnections()
	for ctx.Err() == nil {
		b, err := get(ctx, c, strings.TrimRight(r.Config.SatelliteURL, "/")+"/v1/session", r.Config.SatelliteToken, 65536)
		var session struct {
			Latest int64 `json:"latest_offset"`
		}
		if err == nil && json.Unmarshal(b, &session) == nil {
			if p, err := r.Store.Streams.Checkpoint(ctx, r.Config.SatelliteStream); err == nil {
				offset, _ := strconv.ParseInt(p.Cursor, 10, 64)
				r.metrics.lag.Record(ctx, max(0, session.Latest-offset))
			}
		}
		if !pause(ctx, 5*time.Second) {
			return
		}
	}
}
func (r *Runner) satellite(ctx context.Context) error {
	stream := r.Config.SatelliteStream
	p, err := r.Store.Streams.Checkpoint(ctx, stream)
	if err != nil {
		return err
	}
	e := NewFireEngine(r.Config.Lateness, r.Config.MaxDetections)
	for seq := int64(0); seq < p.Sequence; {
		rows, err := r.Store.Streams.Read(ctx, stream, seq, 200)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return errors.New("journal_gap_during_recovery")
		}
		for _, row := range rows {
			if row.Sequence > p.Sequence {
				break
			}
			if row.Sequence != seq+1 {
				return errors.New("journal_gap")
			}
			_, err = e.Apply(row.Kind, row.Payload, row.Sequence, row.ReceivedAt)
			if errors.Is(err, errCapacity) {
				return err
			}
			seq = row.Sequence
		}
	}
	apply := func(row storage.StreamEntry) error {
		corr := fmt.Sprintf("satellite-%d", row.Sequence)
		work, span := otel.Tracer("bnpb.m2").Start(ctx, "satellite.process")
		defer span.End()
		span.SetAttributes(attribute.String("correlation_id", corr))
		records, err := e.Apply(row.Kind, row.Payload, row.Sequence, row.ReceivedAt)
		if errors.Is(err, errCapacity) {
			return err
		}
		if err != nil {
			if err = r.park(work, domain.SourceSatellite, row.Payload, err.Error(), row.ReceivedAt); err != nil {
				return err
			}
		}
		for _, h := range records {
			if err = r.save(work, h, corr); err != nil {
				return err
			}
		}
		if err = r.commit(work, stream, &p, row); err != nil {
			return err
		}
		r.metrics.inputs.Add(work, 1, metric.WithAttributes(attribute.String("source", domain.SourceSatellite)))
		if len(records) > 0 {
			r.metrics.latency.Record(work, time.Since(row.ReceivedAt).Seconds(), metric.WithAttributes(attribute.String("source", domain.SourceSatellite)))
		}
		fires, singles := e.Counts()
		r.metrics.fires.Record(work, int64(fires))
		r.metrics.singles.Record(work, int64(singles))
		r.metrics.late.Record(work, e.Late)
		if n, err := strconv.ParseInt(p.Cursor, 10, 64); err == nil {
			r.metrics.offset.Record(work, n)
		}
		return r.Store.Sources.RecordSuccess(work, domain.SourceKeySatellite, e.MaxTime)
	}
	// Recover the one input appended before a previous process died.
	for {
		rows, err := r.Store.Streams.Read(ctx, stream, p.Sequence, 1)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
		if rows[0].Sequence != p.Sequence+1 {
			return errors.New("journal_gap")
		}
		if err = apply(rows[0]); err != nil {
			return err
		}
	}
	c := client(5 * time.Second)
	defer c.CloseIdleConnections()
	backoff := time.Second
	for ctx.Err() == nil {
		before := p.Sequence
		err = r.connection(ctx, c, p.Cursor, func(f sseFrame) error {
			cursor := p.Cursor
			if f.ID != "" {
				id, err := strconv.ParseInt(f.ID, 10, 64)
				if err != nil || id < 0 {
					return errors.New("invalid_feed_offset")
				}
				if cursor != "" {
					prev, _ := strconv.ParseInt(cursor, 10, 64)
					if id <= prev {
						return nil
					}
					if id != prev+1 {
						return errors.New("feed_offset_gap")
					}
				}
				cursor = f.ID
			} else if f.Kind != "heartbeat" {
				return errors.New("feed_event_without_offset")
			}
			row, err := r.Store.Streams.Append(ctx, stream, storage.StreamEntry{Sequence: p.Sequence + 1, Kind: f.Kind, Cursor: cursor, Payload: f.Data, ReceivedAt: time.Now().UTC()})
			if err != nil {
				return err
			}
			if err = apply(row); err != nil {
				return &processingError{err}
			}
			return nil
		})
		var fatal *processingError
		if errors.As(err, &fatal) {
			return fatal.error
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_ = r.Store.Sources.RecordFailure(ctx, domain.SourceKeySatellite, fmt.Sprint(err))
		slog.WarnContext(ctx, "satellite disconnected; resuming committed cursor", "cursor", p.Cursor, "error", err, "correlation_id", "satellite-reconnect")
		if p.Sequence > before {
			backoff = time.Second
		}
		wait := backoff
		var status *statusError
		if errors.As(err, &status) && status.RetryAfter > wait {
			wait = status.RetryAfter
		}
		if !pause(ctx, wait) {
			return ctx.Err()
		}
		backoff = min(30*time.Second, backoff*2)
	}
	return ctx.Err()
}

type processingError struct{ error }

func (e *processingError) Unwrap() error { return e.error }

func (r *Runner) connection(ctx context.Context, c *http.Client, cursor string, consume func(sseFrame) error) error {
	u := strings.TrimRight(r.Config.SatelliteURL, "/") + "/v1/stream?from=" + url.QueryEscape(cursor)
	if cursor == "" {
		u = strings.TrimRight(r.Config.SatelliteURL, "/") + "/v1/stream?from=0"
	}
	// Idle timer bounds a silent socket; real frames reset it. Context cancels on lease loss.
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	idle := time.AfterFunc(30*time.Second, cancel)
	defer idle.Stop()
	req, err := http.NewRequestWithContext(streamCtx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+r.Config.SatelliteToken)
	req.Header.Set("Accept", "text/event-stream")
	if cursor != "" {
		req.Header.Set("Last-Event-ID", cursor)
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return httpFailure(resp)
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		return errors.New("expected_event_stream")
	}
	return ReadSSE(resp.Body, r.Config.MaxBodyBytes, func(f sseFrame) error { idle.Reset(30 * time.Second); return consume(f) })
}
