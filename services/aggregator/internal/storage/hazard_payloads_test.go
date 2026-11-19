package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

func payloadRecord(id string, bytes int) *domain.HazardRecord {
	return &domain.HazardRecord{HazardID: id, Source: domain.SourceSatellite, SourceRefID: "private-ref", Latitude: 1, Longitude: 2,
		OccurredAt: time.Unix(1, 0), IngestedAt: time.Unix(2, 0), Attributes: map[string]any{"body": strings.Repeat("x", bytes)}}
}

func TestHazardPayloadBudgetKeepsOrderedPrefixAndContinuationPosition(t *testing.T) {
	for _, known := range []bool{false, true} {
		large := payloadRecord("large", 3<<20)
		candidates := make([]HazardCandidate, 5)
		for i := range candidates {
			candidates[i] = HazardCandidate{Header: HazardSummaryHeader(large)}
			if known {
				candidates[i].FullBytes = domain.FullProjectionSize(large)
			}
		}
		loads := 0
		items, consumed, err := LoadHazardPayloads(context.Background(), HazardQuery{}, candidates, func(*domain.HazardRecord) (*domain.HazardRecord, error) { loads++; return large, nil })
		wantLoads := 3
		if known {
			wantLoads = 2
		}
		if err != nil || len(items) != 2 || consumed != 2 || loads != wantLoads {
			t.Fatal(known, len(items), consumed, loads, err)
		}
	}
}

func TestHazardPayloadBudgetServesCompleteLargeHead(t *testing.T) {
	large := payloadRecord("large", 9<<20)
	loads := 0
	items, consumed, err := LoadHazardPayloads(context.Background(), HazardQuery{}, []HazardCandidate{{Header: large}, {Header: large}}, func(*domain.HazardRecord) (*domain.HazardRecord, error) { loads++; return large, nil })
	if err != nil || len(items) != 1 || consumed != 1 || loads != 1 || len(items[0].Attributes["body"].(string)) != 9<<20 {
		t.Fatal(len(items), consumed, loads, err)
	}
}

func TestHazardSummaryNeverLoadsOrRetainsDetail(t *testing.T) {
	full := payloadRecord("summary", 3<<20)
	items, consumed, err := LoadHazardPayloads(context.Background(), HazardQuery{SummaryOnly: true}, []HazardCandidate{{Header: full}}, func(*domain.HazardRecord) (*domain.HazardRecord, error) {
		t.Fatal("summary hydrated payload")
		return nil, nil
	})
	if err != nil || consumed != 1 || len(items) != 1 || items[0].Attributes != nil || items[0].SourceRefID != "" || items[0].Latitude != 0 || items[0].Longitude != 0 {
		t.Fatal(items, consumed, err)
	}
}

func TestHazardPayloadDoesNotEmitConcurrentMoveAtOldPosition(t *testing.T) {
	header := payloadRecord("moved", 1)
	moved := *header
	moved.OccurredAt = header.OccurredAt.Add(time.Hour)
	items, consumed, err := LoadHazardPayloads(context.Background(), HazardQuery{}, []HazardCandidate{{Header: header}}, func(*domain.HazardRecord) (*domain.HazardRecord, error) { return &moved, nil })
	if err != nil || len(items) != 0 || consumed != 1 {
		t.Fatal("concurrent move broke metadata order", items, consumed, err)
	}
}

func TestHazardPayloadSkipsConcurrentWithdrawalAndPropagatesFailure(t *testing.T) {
	withdrawn := payloadRecord("withdrawn", 1)
	withdrawn.Attributes["withdrawn"] = true
	keep := payloadRecord("keep", 1)
	sentinel := errors.New("storage unavailable")
	loads := 0
	items, consumed, err := LoadHazardPayloads(context.Background(), HazardQuery{}, []HazardCandidate{{Header: withdrawn}, {Header: keep}, {Header: keep}}, func(*domain.HazardRecord) (*domain.HazardRecord, error) {
		loads++
		switch loads {
		case 1:
			return withdrawn, nil
		case 2:
			return keep, nil
		default:
			return nil, sentinel
		}
	})
	if !errors.Is(err, sentinel) || consumed != 2 || len(items) != 1 || items[0].HazardID != "keep" {
		t.Fatal(items, consumed, err)
	}
	tooLarge := payloadRecord("too-large", 16<<20)
	if _, _, err := LoadHazardPayloads(context.Background(), HazardQuery{}, []HazardCandidate{{Header: tooLarge}}, func(*domain.HazardRecord) (*domain.HazardRecord, error) { return tooLarge, nil }); !errors.Is(err, ErrHazardProjectionTooLarge) {
		t.Fatal(err)
	}
}
