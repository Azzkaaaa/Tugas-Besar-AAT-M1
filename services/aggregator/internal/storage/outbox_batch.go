package storage

import (
	"context"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

const (
	MaxOutboxClaimEvents = 50
	// Stop after reaching this target, preserving the complete boundary event.
	// The batch is bounded by this target plus one boundary payload per priority
	// class (coastal/regular), rather than a requested row count multiplied by
	// every record's maximum size.
	OutboxPayloadTargetBytes = 8 << 20
)

func OutboxClaimLimit(requested int) int {
	if requested <= 0 || requested > MaxOutboxClaimEvents {
		return MaxOutboxClaimEvents
	}
	return requested
}

// LoadOutboxPayloads receives bounded metadata candidates. It preserves each
// class's order, giving coastal priority while reserving regular byte capacity.
// Only returned events are hydrated; callers lease those events. Missing rows
// are skipped (they may have been acknowledged); storage errors abort the claim. A large
// first record is served intact, so it cannot permanently block its successors.
func LoadOutboxPayloads(ctx context.Context, candidates []*ClaimedEvent, load func(*ClaimedEvent) (bool, error)) ([]*ClaimedEvent, error) {
	var coastal, regular []*ClaimedEvent
	for _, ev := range candidates {
		if ev.Source == domain.SourceCoastal {
			coastal = append(coastal, ev)
		} else {
			regular = append(regular, ev)
		}
	}
	read := func(group []*ClaimedEvent, target, maxEvents int) (selected []*ClaimedEvent, size int, err error) {
		for _, ev := range group {
			if size >= target || len(selected) >= maxEvents {
				break
			}
			if err := ctx.Err(); err != nil {
				return nil, 0, err
			}
			present, err := load(ev)
			if err != nil {
				return nil, 0, err
			}
			if !present {
				continue
			}
			selected = append(selected, ev)
			size += len(ev.Payload)
		}
		return
	}
	coastalTarget, coastalEvents := OutboxPayloadTargetBytes, MaxOutboxClaimEvents
	if len(coastal) > 0 && len(regular) > 0 {
		coastalTarget /= 2
		coastalEvents /= 2
	}
	selected, size, err := read(coastal, coastalTarget, coastalEvents)
	if err != nil {
		return nil, err
	}
	regularTarget := OutboxPayloadTargetBytes - size
	if len(regular) > 0 {
		regularTarget = max(OutboxPayloadTargetBytes/2, regularTarget)
	}
	rest, _, err := read(regular, regularTarget, MaxOutboxClaimEvents-len(selected))
	if err != nil {
		return nil, err
	}
	return append(selected, rest...), nil
}
