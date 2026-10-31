package dedup

import (
	"context"
	"sync"
	"time"
)

type memoryEntry struct {
	token     string // empty once delivered
	expiresAt time.Time
}

// Memory is a process-local Claimer with the same semantics as Redis. It only deduplicates
// within one replica; use it for tests and single-instance local runs.
type Memory struct {
	mu           sync.Mutex
	entries      map[string]memoryEntry
	deliveredTTL time.Duration
	now          func() time.Time
}

func NewMemory(deliveredTTL time.Duration) *Memory {
	return &Memory{entries: map[string]memoryEntry{}, deliveredTTL: deliveredTTL, now: time.Now}
}

// lookup returns the live entry for k, dropping it when expired. Callers hold mu.
func (m *Memory) lookup(k string) (memoryEntry, bool) {
	entry, ok := m.entries[k]
	if ok && !m.now().Before(entry.expiresAt) {
		delete(m.entries, k)
		return memoryEntry{}, false
	}
	return entry, ok
}

func (m *Memory) Claim(_ context.Context, eventID, webhookID string, lease time.Duration) (Claim, Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := key(eventID, webhookID)
	if entry, ok := m.lookup(k); ok {
		if entry.token == "" {
			return Claim{}, AlreadyDelivered, nil
		}
		return Claim{}, HeldElsewhere, nil
	}
	claim := newClaim(eventID, webhookID)
	m.entries[k] = memoryEntry{token: claim.Token, expiresAt: m.now().Add(lease)}
	return claim, Claimed, nil
}

func (m *Memory) MarkDelivered(_ context.Context, claim Claim) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := key(claim.EventID, claim.WebhookID)
	if entry, ok := m.lookup(k); ok && claim.Token != "" && entry.token == claim.Token {
		m.entries[k] = memoryEntry{expiresAt: m.now().Add(m.deliveredTTL)}
	}
	return nil
}

func (m *Memory) Release(_ context.Context, claim Claim) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := key(claim.EventID, claim.WebhookID)
	if entry, ok := m.lookup(k); ok && claim.Token != "" && entry.token == claim.Token {
		delete(m.entries, k)
	}
	return nil
}

func (m *Memory) Close() error {
	return nil
}
