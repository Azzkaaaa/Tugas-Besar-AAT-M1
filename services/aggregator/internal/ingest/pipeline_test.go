package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

type testInbox struct {
	storage.IngestInbox
	messages map[string]storage.IngestMessage
	chunks   [][]byte
	seq      int64
}

func (s *testInbox) Put(_ context.Context, m storage.IngestMessage) error {
	if _, exists := s.messages[m.ID]; !exists {
		s.seq++
		m.Sequence = s.seq
		s.messages[m.ID] = m
	}
	return nil
}

func (s *testInbox) GetMessage(_ context.Context, id string) (*storage.IngestMessage, error) {
	m, exists := s.messages[id]
	if !exists {
		return nil, nil
	}
	return &m, nil
}

func (s *testInbox) ListMessages(_ context.Context, source string, _ storage.IngestCursor, _ int) (storage.IngestPage, error) {
	var page storage.IngestPage
	for _, m := range s.messages {
		if m.Source == source && m.State != storage.IngestResolved {
			page.Items = append(page.Items, m)
		}
	}
	return page, nil
}

func (s *testInbox) ChangeState(_ context.Context, m storage.IngestMessage, state, reason string, attempts int, next time.Time) (bool, error) {
	current := s.messages[m.ID]
	if current.State != m.State || current.Attempts != m.Attempts {
		return false, nil
	}
	current.State, current.Reason, current.Attempts, current.NextAttempt = state, reason, attempts, next
	s.messages[m.ID] = current
	return true, nil
}

func (s *testInbox) ArchiveChunk(_ context.Context, _ string, _ int, chunk []byte) error {
	s.chunks = append(s.chunks, bytes.Clone(chunk))
	return nil
}

type testSequencedHazards struct {
	storage.HazardStore
	fail    bool
	changes []domain.HazardRecord
}

func (s *testSequencedHazards) ApplyIngestRevision(_ context.Context, h *domain.HazardRecord, _ int64,
	_ func(*domain.HazardRecord) (storage.OutboxEvent, error)) (*domain.HazardRecord, bool, error) {
	if s.fail {
		return nil, false, errors.New("transient write failure")
	}
	s.changes = append(s.changes, *h)
	return h, true, nil
}

func TestPipelineParkRetryReplayOrder(t *testing.T) {
	ctx := context.Background()
	inbox := &testInbox{messages: make(map[string]storage.IngestMessage)}
	hazards := &testSequencedHazards{fail: true}
	p := NewPipeline(inbox, hazards, false, 1024)
	stream := domain.SourceKeyPVMBGVolcanic
	bad := strings.Replace(validVolcanic, `"alert_level":"Normal"`, `"volcano_alert":"Siaga"`, 1)
	other := strings.Replace(validVolcanic, `"r1"`, `"r2"`, 1)
	m := storage.IngestMessage{Source: "PVMBG", Stream: string(stream), Format: formatVolcanic, SchemaVersion: "3"}
	idFor := func(raw string) string {
		identity := m
		identity.SourceRefID = itemMetadata([]byte(raw), m.Format).ref
		return messageID(identity, []byte(raw))
	}
	_, err := p.stage(ctx, incoming{IngestMessage: m, items: []json.RawMessage{[]byte(bad), []byte(validVolcanic), []byte(other)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.process(ctx, stream); err != nil {
		t.Fatal(err)
	}
	badID := idFor(bad)
	parked := inbox.messages[badID]
	retry := inbox.messages[idFor(other)]
	if parked.State != storage.IngestParked || !bytes.Equal(parked.Raw, []byte(bad)) || retry.State != storage.IngestRetrying || retry.NextAttempt.IsZero() {
		t.Fatalf("park/retry contract failed: %+v, %+v", parked, retry)
	}
	if inbox.messages[idFor(validVolcanic)].State != storage.IngestPending {
		t.Fatal("later revision overtook a parked revision")
	}
	if _, err := inbox.ChangeState(ctx, parked, storage.IngestRetrying, "operator_replay", parked.Attempts, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err := inbox.ChangeState(ctx, retry, storage.IngestRetrying, "test elapsed", retry.Attempts, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	hazards.fail = false
	p = NewPipeline(inbox, hazards, true, 1024)
	if count, err := p.process(ctx, stream); err != nil || count != 3 {
		t.Fatalf("expected three recoveries, got %d: %v", count, err)
	}
	var revisions []string
	for _, hazard := range hazards.changes {
		if hazard.SourceRefID == "r1" {
			revisions = append(revisions, hazard.Severity)
		}
	}
	if strings.Join(revisions, ",") != "SIAGA,NORMAL" {
		t.Fatalf("out-of-order replay: %v", revisions)
	}
	if count, err := p.process(ctx, stream); err != nil || count != 0 {
		t.Fatalf("duplicate replay: %d, %v", count, err)
	}
}

func TestPipelinePreservesOversizedAndBrokenResponse(t *testing.T) {
	ctx := context.Background()
	inbox := &testInbox{messages: make(map[string]storage.IngestMessage)}
	p := NewPipeline(inbox, nil, false, 64)
	broken := []byte(`{"items":[{"report_id":`)
	receive := func(raw []byte) {
		t.Helper()
		resp := &http.Response{Body: io.NopCloser(bytes.NewReader(raw)), Header: make(http.Header)}
		if _, err := p.receive(ctx, resp, domain.SourceKeyPVMBGVolcanic, formatVolcanic, "corr"); !errors.Is(err, errUncertainProgress) {
			t.Fatalf("expected poll progress to be held, got %v", err)
		}
	}
	receive(broken)
	for _, message := range inbox.messages {
		if !bytes.Equal(message.Raw, broken) || message.Reason != "malformed_envelope" {
			t.Fatalf("malformed body changed: %+v", message)
		}
	}
	large := bytes.Repeat([]byte{0xff, 0, 1}, archiveChunkSize/2)
	receive(large)
	if !bytes.Equal(bytes.Join(inbox.chunks, nil), large) || len(inbox.messages) != 2 {
		t.Fatal("oversized response was truncated or lost")
	}
}

// Exercise quarantine and subsequent healthy work together, rather than only
// calling the validator: a poisoned item must not block an unrelated key.
func TestPipelinePoisonIsolationAndIntactArchive(t *testing.T) {
	for reason, raw := range map[string][]byte{
		"invalid_json_object": []byte(validVolcanic[:len(validVolcanic)-3]),
		"invalid_utf8":        []byte(strings.Replace(validVolcanic, "v-merapi", "v-\xff", 1)),
		"json_depth_exceeded": []byte(`{"new":` + strings.Repeat("[", 40) + "0" + strings.Repeat("]", 40) + "}"),
		"payload_too_large":   bytes.Repeat([]byte{'x'}, maxItemBytes+1),
	} {
		t.Run(reason, func(t *testing.T) {
			ctx := context.Background()
			inbox := &testInbox{messages: make(map[string]storage.IngestMessage)}
			hazards := &testSequencedHazards{}
			p := NewPipeline(inbox, hazards, true, 1<<20)
			m := storage.IngestMessage{Source: "PVMBG", Stream: string(domain.SourceKeyPVMBGVolcanic), Format: formatVolcanic, SchemaVersion: "2"}
			_, err := p.stage(ctx, incoming{IngestMessage: m, items: []json.RawMessage{raw, []byte(validVolcanic)}}, nil)
			if err != nil && !errors.Is(err, errUncertainProgress) {
				t.Fatal(err)
			}
			if count, err := p.process(ctx, domain.SourceKeyPVMBGVolcanic); err != nil || count != 1 {
				t.Fatalf("healthy key failed: count=%d err=%v", count, err)
			}
			parked := 0
			for _, message := range inbox.messages {
				if message.State != storage.IngestParked {
					continue
				}
				parked++
				payload := message.Raw
				if message.ArchiveChunks > 0 {
					payload = bytes.Join(inbox.chunks, nil)
				}
				if message.Reason != reason || !bytes.Equal(payload, raw) || message.Attempts != 1 {
					t.Fatalf("quarantine changed poison or retried it: %+v", message)
				}
			}
			if parked != 1 {
				t.Fatalf("want one parked item, got %d", parked)
			}
			if count, err := p.process(ctx, domain.SourceKeyPVMBGVolcanic); err != nil || count != 0 {
				t.Fatalf("poison retry loop or duplicate healthy write: %d, %v", count, err)
			}
		})
	}
}
