package storage

import (
	"context"
	"errors"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	HazardPageTargetBytes = 8 << 20
	// The RPC handler checks the exact complete response size including its
	// cursor/freshness metadata. Never truncate an individual record.
	MaxHazardProjectionBytes = 16 << 20
	MaxHazardPageRecords     = 1000
)

var ErrHazardProjectionTooLarge = errors.New("complete hazard projection exceeds the response byte budget")

type HazardCandidate struct {
	Header *domain.HazardRecord
	// FullBytes is a revision-checked cache, zero when unknown.
	FullBytes int
}

func HazardPageSize(size int) int { return max(1, min(size, MaxHazardPageRecords)) }

func HazardSummaryHeader(h *domain.HazardRecord) *domain.HazardRecord {
	return &domain.HazardRecord{HazardID: h.HazardID, Source: h.Source, HazardType: h.HazardType,
		Severity: h.Severity, AreaName: h.AreaName, OccurredAt: h.OccurredAt, IngestedAt: h.IngestedAt, Revision: h.Revision}
}

func MatchesHazardQuery(h *domain.HazardRecord, q HazardQuery) bool {
	return !domain.Withdrawn(h) && (q.Source == "" || h.Source == q.Source) &&
		(q.HazardType == "" || h.HazardType == q.HazardType) && (q.Severity == "" || h.Severity == q.Severity) &&
		(q.Since.IsZero() || !h.OccurredAt.Before(q.Since)) && (q.Until.IsZero() || h.OccurredAt.Before(q.Until))
}

// LoadHazardPayloads receives a bounded, globally ordered metadata page. consumed
// identifies the last metadata position processed, including rows concurrently
// moved/withdrawn. A byte-limited caller must issue its continuation there,
// before any unreturned candidate, even when the metadata page was the last one.
func LoadHazardPayloads(ctx context.Context, q HazardQuery, candidates []HazardCandidate,
	load func(*domain.HazardRecord) (*domain.HazardRecord, error),
) (items []*domain.HazardRecord, consumed int, err error) {
	size := 0
	for i, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return items, consumed, err
		}
		predicted := candidate.FullBytes
		if q.SummaryOnly {
			predicted = 1 + protowire.SizeBytes(proto.Size(candidate.Header.ToSummaryProto()))
		}
		if len(items) > 0 && (size >= HazardPageTargetBytes || predicted > 0 && size+predicted+8 > HazardPageTargetBytes) {
			break
		}
		h := candidate.Header
		if !q.SummaryOnly {
			h, err = load(h)
			if err != nil {
				return items, consumed, err
			}
			// A point read may observe a concurrent move after metadata selection.
			// Do not emit that row at its former position and break global order.
			if h == nil || h.HazardID != candidate.Header.HazardID || !h.OccurredAt.Equal(candidate.Header.OccurredAt) || !MatchesHazardQuery(h, q) {
				consumed = i + 1
				continue
			}
			predicted = domain.FullProjectionSize(h)
		} else {
			h = HazardSummaryHeader(h)
		}
		if predicted > MaxHazardProjectionBytes {
			return items, consumed, ErrHazardProjectionTooLarge
		}
		// The enclosing repeated items field also consumes a tag and length.
		n := 1 + protowire.SizeBytes(predicted)
		if len(items) > 0 && size+n > HazardPageTargetBytes {
			break
		}
		items = append(items, h)
		size += n
		consumed = i + 1
	}
	return
}
