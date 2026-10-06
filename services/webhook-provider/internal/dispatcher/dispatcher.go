package dispatcher

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// Result holds the outcome of a single dispatch attempt.
type Result struct {
	StatusCode int
	DurationMS int64
	Err        error
}

// Dispatcher sends JSON payloads to external webhook URLs with HMAC signatures
// and exponential-backoff retries.
type Dispatcher struct {
	client     *http.Client
	maxRetries int
}

// New creates a Dispatcher with the given HTTP timeout and max retry count.
func New(timeout time.Duration, maxRetries int) *Dispatcher {
	return &Dispatcher{
		client:     &http.Client{Timeout: timeout},
		maxRetries: maxRetries,
	}
}

// Dispatch attempts to POST payload to url, signing with secret.
// It retries up to maxRetries times with exponential backoff.
// Returns the final Result (may be a failure after all attempts).
func (d *Dispatcher) Dispatch(ctx context.Context, url, secret string, payload []byte) Result {
	var last Result
	for attempt := 1; attempt <= d.maxRetries; attempt++ {
		last = d.doPost(ctx, url, secret, payload)
		if last.Err == nil && last.StatusCode >= 200 && last.StatusCode < 300 {
			return last
		}
		log.Printf("[Dispatcher] Attempt %d/%d to %s failed (status=%d, err=%v)",
			attempt, d.maxRetries, url, last.StatusCode, last.Err)

		if attempt < d.maxRetries {
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second // 1s, 2s, 4s, 8s, 16s
			select {
			case <-ctx.Done():
				last.Err = ctx.Err()
				return last
			case <-time.After(backoff):
			}
		}
	}
	return last
}

// DispatchOnce attempts exactly one HTTP POST — used by PingWebhook.
func (d *Dispatcher) DispatchOnce(ctx context.Context, url, secret string, payload []byte) Result {
	return d.doPost(ctx, url, secret, payload)
}

func (d *Dispatcher) doPost(ctx context.Context, url, secret string, body []byte) Result {
	sig := computeSignature(secret, body)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return Result{Err: fmt.Errorf("building request: %w", err)}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-BNPB-Signature", sig)
	req.Header.Set("User-Agent", "BNPB-Webhook-Provider/1.0")

	start := time.Now()
	resp, err := d.client.Do(req)
	durationMS := time.Since(start).Milliseconds()

	if err != nil {
		return Result{DurationMS: durationMS, Err: fmt.Errorf("http post: %w", err)}
	}
	defer resp.Body.Close()
	// Drain body so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)

	return Result{StatusCode: resp.StatusCode, DurationMS: durationMS}
}

// computeSignature returns "sha256=<hex>" for the given secret and body.
func computeSignature(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
