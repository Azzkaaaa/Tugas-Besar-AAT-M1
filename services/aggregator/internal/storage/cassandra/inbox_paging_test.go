package cassandra

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

func TestInboxPagingFindsPendingBeyondResolvedAndOtherSourceHistory(t *testing.T) {
	rows := make([]storage.IngestMessage, 0, 270)
	var want []string
	for i := range 270 {
		m := storage.IngestMessage{ID: fmt.Sprintf("msg-00%062x", i), Source: "PVMBG", State: storage.IngestResolved}
		if i >= 200 && i%3 == 0 {
			m.State = storage.IngestParked
			want = append(want, m.ID)
		} else if i%2 == 0 {
			m.Source, m.State = "BMKG", storage.IngestPending
		}
		rows = append(rows, m)
	}
	read := func(_ context.Context, shard int, after string) ([]storage.IngestMessage, error) {
		if shard != 0 {
			return nil, nil
		}
		var out []storage.IngestMessage
		for _, m := range rows {
			if m.ID > after {
				out = append(out, m)
			}
			if len(out) == inboxMetadataPageSize {
				break
			}
		}
		return out, nil
	}
	for _, limit := range []int{1, 7, 1000} {
		cursor := storage.IngestCursor{}
		var got []string
		emptyContinuations := 0
		for calls := 0; ; calls++ {
			if calls > 300 {
				t.Fatal("pagination did not terminate")
			}
			page, err := collectInboxPage(context.Background(), "PVMBG", cursor, limit, read)
			if err != nil || len(page.UnavailableUnits) > 0 || len(page.Items) > limit {
				t.Fatal(page, err)
			}
			for _, m := range page.Items {
				got = append(got, m.ID)
			}
			if page.Next == nil {
				break
			}
			if *page.Next == cursor {
				t.Fatal("cursor did not advance through history")
			}
			if len(page.Items) == 0 {
				emptyContinuations++
			}
			cursor = *page.Next
		}
		if emptyContinuations < 3 {
			t.Fatal("test did not exercise filtered physical pages")
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("limit=%d: lost/duplicated pending rows: %v, want %v", limit, got, want)
		}
	}
}

func TestInboxPagingPreservesFailedShardMarkerAndOtherShardResults(t *testing.T) {
	want := storage.IngestMessage{ID: "msg-01" + fmt.Sprintf("%062x", 1), Source: "PVMBG", State: storage.IngestParked}
	page, err := collectInboxPage(context.Background(), "PVMBG", storage.IngestCursor{}, 100,
		func(_ context.Context, shard int, _ string) ([]storage.IngestMessage, error) {
			if shard == 0 {
				return nil, errors.New("unavailable")
			}
			if shard == 1 {
				return []storage.IngestMessage{want}, nil
			}
			return nil, nil
		})
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != want.ID || !reflect.DeepEqual(page.UnavailableUnits, []string{"ingest-inbox-00"}) {
		t.Fatal(page, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = collectInboxPage(ctx, "PVMBG", storage.IngestCursor{}, 100, func(context.Context, int, string) ([]storage.IngestMessage, error) {
		t.Fatal("read after cancellation")
		return nil, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost", err)
	}
}
