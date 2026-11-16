package ingest

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

func TestExternalQuarantinePreservesLargePoisonBytesAndDeduplicates(t *testing.T) {
	inbox := &testInbox{messages: map[string]storage.IngestMessage{}}
	raw := bytes.Repeat([]byte{0, 0xff, 0xfe, '\n'}, 40000)
	for range 2 {
		if err := ParkExternal(context.Background(), inbox, "SENSOR_PANTAI", raw, "invalid_utf8", "coastal-correlation", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if len(inbox.messages) != 1 || !bytes.Equal(bytes.Join(inbox.chunks, nil), raw) {
		t.Fatal("lost bytes or duplicate archive on retry")
	}
	for _, m := range inbox.messages {
		if m.State != storage.IngestParked || m.Reason != "invalid_utf8" || m.ArchiveChunks != 3 || m.SourceRefID != "" || len(m.Raw) != 0 {
			t.Fatalf("wrong quarantine metadata: %+v", m)
		}
	}
}
