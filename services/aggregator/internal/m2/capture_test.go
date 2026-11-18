package m2

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

type captureInbox struct {
	storage.IngestInbox
	messages map[string]storage.IngestMessage
	chunks   map[string][][]byte
}

func (s *captureInbox) Put(_ context.Context, m storage.IngestMessage) error {
	s.messages[m.ID] = m
	return nil
}
func (s *captureInbox) GetMessage(_ context.Context, id string) (*storage.IngestMessage, error) {
	m, ok := s.messages[id]
	if !ok {
		return nil, nil
	}
	return &m, nil
}
func (s *captureInbox) ArchiveChunk(_ context.Context, id string, n int, b []byte) error {
	if len(b) > 64<<10 {
		return errors.New("unbounded chunk")
	}
	rows := s.chunks[id]
	if n < len(rows) {
		rows[n] = bytes.Clone(b)
	} else {
		rows = append(rows, bytes.Clone(b))
	}
	s.chunks[id] = rows
	return nil
}

type captureTransport func(*http.Request) (*http.Response, error)

func (f captureTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type brokenCapture struct{ io.Reader }

func (b brokenCapture) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	if errors.Is(err, io.EOF) {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

func TestHTTPArchiveIntactOversizeAndIncompleteTransfers(t *testing.T) {
	for _, complete := range []bool{true, false} {
		t.Run(map[bool]string{true: "complete", false: "interrupted"}[complete], func(t *testing.T) {
			inbox := &captureInbox{messages: map[string]storage.IngestMessage{}, chunks: map[string][][]byte{}}
			r := NewRunner(Config{MaxBodyBytes: 128, CoastalToken: "secret"}, &storage.Backend{Inbox: inbox})
			raw := bytes.Repeat([]byte{0, 0xff, '\n', 'x'}, 75000)
			c := &http.Client{Transport: captureTransport(func(q *http.Request) (*http.Response, error) {
				var body io.Reader = bytes.NewReader(raw)
				if !complete {
					body = brokenCapture{body}
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(body), Header: http.Header{}}, nil
			})}
			for range 2 {
				if _, err := r.getCaptured(context.Background(), c, "http://sensor/observations", "/v1/observations", "cursor-7"); err == nil {
					t.Fatal("poison parsed")
				}
			}
			if len(inbox.messages) != 1 {
				t.Fatal("archive retry duplicated input")
			}
			for _, m := range inbox.messages {
				if !bytes.Equal(bytes.Join(inbox.chunks[m.ArchiveID], nil), raw) {
					t.Fatal("failed response bytes truncated")
				}
				var meta struct {
					Complete bool
					Bytes    int
					Position map[string]string
				}
				if err := json.Unmarshal(m.Companion, &meta); err != nil {
					t.Fatal(err)
				}
				if meta.Complete != complete || meta.Bytes != len(raw) || meta.Position["cursor"] != "cursor-7" {
					t.Fatalf("incorrect capture: %+v", meta)
				}
			}
		})
	}
}

func TestSSEArchiveRetainsFullWireFrameAndContinuesHealthyInput(t *testing.T) {
	poison := []byte("id: 1\r\nevent: detection\r\ndata: " + string(bytes.Repeat([]byte{'x'}, 300000)) + "\r\n\r\n")
	input := append(bytes.Clone(poison), []byte("id: 2\ndata: {\"good\":true}\n\n")...)
	var archived []byte
	var frames []sseFrame
	err := ReadSSECaptured(bytes.NewReader(input), 1024, func(f sseFrame) error { frames = append(frames, f); return nil }, func(f sseFrame, raw io.Reader, readErr error) error {
		if readErr != nil || f.ID != "1" {
			t.Fatal(f, readErr)
		}
		archived, _ = io.ReadAll(raw)
		return nil
	})
	if !errors.Is(err, io.EOF) || !bytes.Equal(archived, poison) || len(frames) != 2 || frames[0].Kind != "quarantined" || frames[1].ID != "2" || string(frames[1].Data) != `{"good":true}` {
		t.Fatalf("frame loss or poison loop: %v %+v", err, frames)
	}
}
func TestSSECaptureDoesNotAdvanceIncompleteOrUnarchivedFrame(t *testing.T) {
	for _, failArchive := range []bool{false, true} {
		raw := []byte("id: 7\ndata: incomplete")
		called := false
		err := ReadSSECaptured(bytes.NewReader(raw), 1024, func(sseFrame) error { t.Fatal("incomplete frame committed"); return nil }, func(_ sseFrame, r io.Reader, readErr error) error {
			called = true
			b, _ := io.ReadAll(r)
			if !bytes.Equal(b, raw) || !errors.Is(readErr, io.ErrUnexpectedEOF) {
				t.Fatal("incomplete transfer lost")
			}
			if failArchive {
				return errors.New("archive offline")
			}
			return nil
		})
		if err == nil || !called {
			t.Fatal("missing incomplete capture")
		}
	}
	err := ReadSSECaptured(bytes.NewReader([]byte("id: 1\ndata: "+string(bytes.Repeat([]byte{'x'}, 3000))+"\n\n")), 1024, func(sseFrame) error { t.Fatal("unarchived poison committed"); return nil }, func(sseFrame, io.Reader, error) error { return errors.New("archive offline") })
	if err == nil {
		t.Fatal("archive failure ignored")
	}
}
