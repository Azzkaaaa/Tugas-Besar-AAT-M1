package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/criticaldelivery/receiver"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/telemetry"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type keys struct {
	ReceiverID     string `json:"receiver_id"`
	PublicKey      string `json:"public_key"`
	PrivateKey     string `json:"private_key"`
	ReceiverSecret string `json:"receiver_secret"`
	DeliveryToken  string `json:"delivery_token"`
}

func run() error {
	dbPath := flag.String("db", "/data/critical-receipts.db", "durable Field Team receipt database")
	listen := flag.String("listen", "127.0.0.1:8099", "receiver HTTP address")
	configPath := flag.String("config", "", "local generated receiver key file (never commit)")
	keygen := flag.String("keygen-dir", "", "create a new private key directory and exit")
	identity := flag.String("receiver-id", os.Getenv("CRITICAL_RECEIVER_ID"), "designated Field Team receiver identity")
	export := flag.Bool("export", false, "export signed receipts as JSONL and exit")
	flag.Parse()
	if *keygen != "" {
		if *identity == "" {
			return errors.New("receiver-id is required for key generation")
		}
		public, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return err
		}
		random := make([]byte, 64)
		if _, err = rand.Read(random); err != nil {
			return err
		}
		k := keys{ReceiverID: *identity, PublicKey: base64.StdEncoding.EncodeToString(public), PrivateKey: base64.StdEncoding.EncodeToString(private), ReceiverSecret: hex.EncodeToString(random[:32]), DeliveryToken: hex.EncodeToString(random[32:])}
		if err = os.Mkdir(*keygen, 0700); err != nil {
			return err
		}
		encoded, err := json.MarshalIndent(k, "", "  ")
		if err != nil {
			return err
		}
		if err = os.WriteFile(filepath.Join(*keygen, "receiver.keys.json"), encoded, 0600); err != nil {
			return err
		}
		fmt.Println("Receiver keys created; private values were written only to the key file.")
		return nil
	}
	k := keys{ReceiverID: *identity, PrivateKey: os.Getenv("CRITICAL_RECEIVER_PRIVATE_KEY"), ReceiverSecret: os.Getenv("CRITICAL_RECEIVER_SECRET")}
	if *configPath != "" {
		raw, err := os.ReadFile(*configPath)
		if err != nil {
			return err
		}
		if json.Unmarshal(raw, &k) != nil {
			return errors.New("invalid receiver key file")
		}
	}
	private, err := base64.StdEncoding.DecodeString(strings.TrimSpace(k.PrivateKey))
	if err != nil {
		return errors.New("invalid receiver private key encoding")
	}
	r, err := receiver.Open(*dbPath, k.ReceiverID, k.ReceiverSecret, ed25519.PrivateKey(private))
	if err != nil {
		return err
	}
	defer r.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if *export {
		return r.Export(ctx, os.Stdout)
	}
	shutdown, err := telemetry.Init(ctx, "field-team-receiver")
	if err != nil {
		return err
	}
	defer func() {
		stop, done := context.WithTimeout(context.Background(), 3*time.Second)
		defer done()
		_ = shutdown(stop)
	}()
	server := &http.Server{Addr: *listen, Handler: otelhttp.NewHandler(r.Handler(), "field-team.receive"), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second}
	stop := context.AfterFunc(ctx, func() {
		closed, done := context.WithTimeout(context.Background(), 3*time.Second)
		defer done()
		_ = server.Shutdown(closed)
	})
	defer stop()
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err := run(); err != nil {
		slog.Error("critical receiver failed", "error", err, "correlation_id", "receiver-startup")
		os.Exit(1)
	}
}
