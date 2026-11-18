package criticaldelivery

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/telemetry"
)

func LoadAPI() (API, error) {
	a := API{Token: os.Getenv("CRITICAL_DELIVERY_TOKEN"), ReceiverID: os.Getenv("CRITICAL_RECEIVER_ID")}
	encoded := os.Getenv("CRITICAL_RECEIVER_PUBLIC_KEY")
	if a.Token == "" && a.ReceiverID == "" && encoded == "" {
		return a, nil
	}
	for _, name := range []string{"AUDIT_API_TOKEN", "AUDIT_TOKEN", "AGGREGATOR_SERVICE_TOKEN"} {
		if value := os.Getenv(name); value != "" && value == a.Token {
			return a, errors.New("critical delivery requires a dedicated credential")
		}
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if len(a.Token) < 16 || a.ReceiverID == "" || err != nil || len(key) != ed25519.PublicKeySize {
		return a, errors.New("critical delivery requires a dedicated token, receiver ID and Ed25519 public key")
	}
	a.PublicKey = ed25519.PublicKey(key)
	return a, nil
}
func LoadWorker() (*Worker, error) {
	a, err := LoadAPI()
	if err != nil {
		return nil, err
	}
	w := &Worker{Token: a.Token, ReceiverID: a.ReceiverID, PublicKey: a.PublicKey, APIURL: strings.TrimRight(os.Getenv("CRITICAL_DELIVERY_API_URL"), "/"), ReceiverURL: os.Getenv("CRITICAL_RECEIVER_URL"), ReceiverSecret: os.Getenv("CRITICAL_RECEIVER_SECRET"), Concurrency: 4, Client: NewHTTPClient()}
	if len(w.Token) < 16 || len(w.ReceiverSecret) < 16 || len(w.PublicKey) != ed25519.PublicKeySize {
		return nil, errors.New("critical delivery worker credentials are required")
	}
	for _, raw := range []string{w.APIURL, w.ReceiverURL} {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, errors.New("invalid critical delivery endpoint")
		}
	}
	if raw := os.Getenv("CRITICAL_DELIVERY_CONCURRENCY"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 8 {
			return nil, errors.New("critical delivery concurrency must be 1..8")
		}
		w.Concurrency = n
	}
	return w, nil
}
func RunWorkerMain() error {
	w, err := LoadWorker()
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	shutdown, err := telemetry.Init(ctx, "critical-notifier")
	if err != nil {
		return err
	}
	defer func() {
		stop, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = shutdown(stop)
	}()
	address := os.Getenv("CRITICAL_WORKER_HEALTH_ADDRESS")
	if address == "" {
		address = ":9092"
	}
	health := http.NewServeMux()
	health.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })
	server := &http.Server{Addr: address, Handler: health, ReadHeaderTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("critical worker health listener failed", "error", err, "correlation_id", "critical-startup")
			cancel()
		}
	}()
	w.Run(ctx)
	stop, done := context.WithTimeout(context.Background(), 3*time.Second)
	defer done()
	_ = server.Shutdown(stop)
	return nil
}
