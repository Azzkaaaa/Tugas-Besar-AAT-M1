package m2

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/ingest"
)

// Keep only the parse prefix in memory. Failed and oversized finite responses
// are drained into the shared durable archive before returning to the poller.
func (r *Runner) getCaptured(ctx context.Context, c *http.Client, url, endpoint, cursor string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+r.Config.CoastalToken)
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	prefix, readErr := io.ReadAll(io.LimitReader(resp.Body, int64(r.Config.MaxBodyBytes)+1))
	var failure error
	switch {
	case resp.StatusCode != http.StatusOK:
		failure = httpFailure(resp)
	case readErr != nil:
		failure = errors.New("incomplete_response")
	case len(prefix) > r.Config.MaxBodyBytes:
		failure = errors.New("body_size_exceeded")
	default:
		return prefix, nil
	}
	if r.Store.Inbox == nil {
		return nil, errors.New("durable_capture_requires_ingest_inbox")
	}
	reader := io.MultiReader(bytes.NewReader(prefix), resp.Body)
	err = ingest.ParkExternalReader(ctx, r.Store.Inbox, domain.SourceCoastal, reader, readErr, failure.Error(), "coastal-poll", time.Now().UTC(), map[string]string{"endpoint": endpoint, "cursor": cursor})
	if err != nil {
		return nil, err
	}
	r.metrics.parked.Add(ctx, 1)
	return nil, failure
}
