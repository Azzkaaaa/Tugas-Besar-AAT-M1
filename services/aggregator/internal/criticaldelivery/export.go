package criticaldelivery

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"time"
)

type Manifest struct {
	Kind       string    `json:"kind"`
	Complete   bool      `json:"complete"`
	Versions   []int64   `json:"versions"`
	Count      int64     `json:"count"`
	CapturedAt time.Time `json:"captured_at"`
}

func VersionCount(versions []int64) (int64, error) {
	if len(versions) != 1 && len(versions) != 16 {
		return 0, errors.New("invalid snapshot version vector")
	}
	var total int64
	for _, v := range versions {
		if v < 0 || v > math.MaxInt64-total {
			return 0, errors.New("invalid snapshot generation")
		}
		total += v
	}
	return total, nil
}

// ExportJournal reads an immutable generation boundary while live writes may
// continue. The captured generation sum proves the exact expected row count.
// A partial read or missing generation never receives a complete manifest.
func (w *Worker) ExportJournal(ctx context.Context, out io.Writer) error {
	var head struct {
		Versions []int64 `json:"versions"`
	}
	if err := w.request(ctx, http.MethodGet, "/internal/critical/version", nil, &head); err != nil {
		return err
	}
	expected, err := VersionCount(head.Versions)
	if err != nil {
		return err
	}
	encoded, _ := json.Marshal(head.Versions)
	version := base64.RawURLEncoding.EncodeToString(encoded)
	cursor := ""
	count := int64(0)
	encoder := json.NewEncoder(out)
	for {
		var page Page
		if err := w.request(ctx, http.MethodGet, "/internal/critical/events?limit=100&versions="+url.QueryEscape(version)+"&cursor="+url.QueryEscape(cursor), nil, &page); err != nil {
			return err
		}
		if page.Partial {
			return errors.New("canonical journal snapshot is partial")
		}
		for _, e := range page.Events {
			if e.Generation < 1 {
				return errors.New("journal event has no committed generation")
			}
			if err := encoder.Encode(e); err != nil {
				return err
			}
			count++
		}
		if page.Next == "" {
			break
		}
		if page.Next == cursor {
			return errors.New("journal cursor did not advance")
		}
		cursor = page.Next
	}
	if count != expected {
		return errors.New("canonical journal snapshot does not match its committed generation count")
	}
	return encoder.Encode(Manifest{Kind: "critical-journal-manifest", Complete: true, Versions: head.Versions, Count: count, CapturedAt: time.Now().UTC()})
}
