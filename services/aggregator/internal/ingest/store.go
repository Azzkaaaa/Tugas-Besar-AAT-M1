package ingest

import (
	"context"
	"errors"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/telemetry"
)

// ingestRecords applies each mapped record and enqueues a HazardEventPublished for every new or
// changed one. It returns how many changed and the newest OccurredAt seen (at least watermark).
//
// Each record is atomic with its event on its own, so a failure part way keeps the records before
// it. That is safe: the caller only advances the watermark when every record succeeded, and the
// next poll re-applies the rest; records already stored are unchanged and emit nothing.
func ingestRecords(ctx context.Context, hazards storage.HazardStore, records []*domain.HazardRecord,
	watermark time.Time, corrID string,
) (int, time.Time, error) {
	newEvent := publishedEvent(ctx, corrID)
	changedCount := 0
	newWatermark := watermark
	for _, h := range records {
		_, changed, err := hazards.ApplyRevision(ctx, h, newEvent)
		if err != nil {
			return changedCount, watermark, err
		}
		if changed {
			changedCount++
		}
		if h.OccurredAt.After(newWatermark) {
			newWatermark = h.OccurredAt
		}
	}
	return changedCount, newWatermark, nil
}

func applySequencedRecord(ctx context.Context, hazards storage.HazardStore, h *domain.HazardRecord, seq int64, corrID string) (bool, error) {
	store, ok := hazards.(storage.SequencedHazardStore)
	if !ok {
		return false, errors.New("store does not support sequenced writes")
	}
	_, changed, err := store.ApplyIngestRevision(ctx, h, seq, publishedEvent(ctx, corrID))
	return changed, err
}

func publishedEvent(ctx context.Context, corrID string) func(*domain.HazardRecord) (storage.OutboxEvent, error) {
	traceContext := telemetry.TraceCarrier(ctx)
	return func(saved *domain.HazardRecord) (storage.OutboxEvent, error) {
		eventID, payload, err := BuildPublishedEnvelope(saved, corrID)
		if err != nil {
			return storage.OutboxEvent{}, err
		}
		return storage.OutboxEvent{
			EventID:      eventID,
			HazardID:     saved.HazardID,
			Source:       saved.Source,
			Payload:      payload,
			TraceContext: traceContext,
		}, nil
	}
}
