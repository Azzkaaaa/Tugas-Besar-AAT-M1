package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"
)

const streamWriteTimeout = 5 * time.Second

// Bound a stalled write/flush without placing a deadline on an idle SSE stream.
// Request cancellation closes/joins the owned body before returning its credits.
func streamWrites(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/v1/feeds/") {
			next.ServeHTTP(w, r)
			return
		}
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		next.ServeHTTP(&streamWriter{ResponseWriter: w, cancel: cancel}, r.WithContext(ctx))
	})
}

type streamWriter struct {
	http.ResponseWriter
	cancel context.CancelFunc
}

func (w *streamWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *streamWriter) Write(p []byte) (int, error) {
	controller := http.NewResponseController(w.ResponseWriter)
	if err := controller.SetWriteDeadline(time.Now().Add(streamWriteTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		w.cancel()
		return 0, err
	}
	defer func() { _ = controller.SetWriteDeadline(time.Time{}) }()
	n, err := w.ResponseWriter.Write(p)
	if err != nil {
		w.cancel()
	}
	return n, err
}

func (w *streamWriter) Flush() {
	controller := http.NewResponseController(w.ResponseWriter)
	if err := controller.SetWriteDeadline(time.Now().Add(streamWriteTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		w.cancel()
		return
	}
	defer func() { _ = controller.SetWriteDeadline(time.Time{}) }()
	if err := controller.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		w.cancel()
	}
}
