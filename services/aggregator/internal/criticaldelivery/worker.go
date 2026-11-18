package criticaldelivery

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/telemetry"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type Worker struct {
	APIURL, Token, ReceiverURL, ReceiverID, ReceiverSecret string
	PublicKey                                              ed25519.PublicKey
	Concurrency                                            int
	Client                                                 *http.Client
}

func NewHTTPClient() *http.Client {
	return &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport), Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
func (w *Worker) request(ctx context.Context, method, path string, body []byte, output any) error {
	req, err := http.NewRequestWithContext(ctx, method, w.APIURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+w.Token)
	req.Header.Set("Content-Type", "application/json")
	response, err := w.Client.Do(req)
	if err != nil {
		return errors.New("critical API request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return fmt.Errorf("critical API returned %d", response.StatusCode)
	}
	if output != nil {
		return json.NewDecoder(io.LimitReader(response.Body, 16<<20)).Decode(output)
	}
	_, err = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	return err
}
func (w *Worker) deliver(ctx context.Context, e storage.CriticalEvent) error {
	hash := sha256.Sum256(e.Payload)
	if hex.EncodeToString(hash[:]) != e.PayloadSHA256 {
		return errors.New("critical payload hash mismatch")
	}
	parent := telemetry.ContextFromCarrier(ctx, e.TraceContext)
	ctx, span := otel.Tracer("bnpb.critical").Start(parent, "critical.deliver")
	defer span.End()
	span.SetAttributes(attribute.String("event.id", e.EventID), attribute.String("correlation_id", e.EventID))
	work, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(work, http.MethodPost, w.ReceiverURL, bytes.NewReader(e.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-BNPB-Signature", RequestMAC(w.ReceiverSecret, e.Payload))
	req.Header.Set("X-Correlation-ID", e.EventID)
	response, err := w.Client.Do(req)
	if err != nil {
		return errors.New("Field Team receiver unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return fmt.Errorf("Field Team receiver returned %d", response.StatusCode)
	}
	var receipt storage.CriticalReceipt
	if err := json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&receipt); err != nil {
		return errors.New("invalid receiver receipt")
	}
	if receipt.EventID != e.EventID || receipt.HazardID != e.HazardID || receipt.PayloadSHA256 != e.PayloadSHA256 {
		return errors.New("receiver receipt identity mismatch")
	}
	if err := VerifyReceipt(w.PublicKey, w.ReceiverID, receipt); err != nil {
		return err
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	return w.request(ctx, http.MethodPost, "/internal/critical/receipts", encoded, nil)
}

// Cycle never persists progress locally. Both replica loss and loss of the
// broker/notification DB leave the canonical pending index authoritative.
func (w *Worker) Cycle(ctx context.Context, cursor string) (string, error) {
	var page Page
	if err := w.request(ctx, http.MethodGet, "/internal/critical/pending?limit=32&cursor="+url.QueryEscape(cursor), nil, &page); err != nil {
		return cursor, err
	}
	parallel := max(1, min(w.Concurrency, 8))
	jobs := make(chan storage.CriticalEvent)
	var workers sync.WaitGroup
	var first error
	var once sync.Once
	for range parallel {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for e := range jobs {
				if err := w.deliver(ctx, e); err != nil {
					once.Do(func() { first = err })
					slog.WarnContext(ctx, "critical delivery will retry", "event_id", e.EventID, "correlation_id", e.EventID, "error", err)
				}
			}
		}()
	}
	for _, e := range page.Events {
		select {
		case jobs <- e:
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return cursor, ctx.Err()
		}
	}
	close(jobs)
	workers.Wait()
	if page.Partial {
		return cursor, errors.New("critical storage is partial")
	}
	return page.Next, first
}

func (w *Worker) Run(ctx context.Context) {
	if w.Client == nil {
		w.Client = NewHTTPClient()
	}
	cursor := ""
	backoff := 250 * time.Millisecond
	for ctx.Err() == nil {
		next, err := w.Cycle(ctx, cursor)
		cursor = next
		if err != nil {
			backoff = min(2*time.Second, backoff*2)
			slog.WarnContext(ctx, "critical reconciliation retry", "error", err, "correlation_id", "critical-reconcile")
		} else {
			backoff = 250 * time.Millisecond
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
