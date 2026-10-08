// api-gateway serves contracts/openapi/gateway.yaml behind Envoy and calls the gRPC backends.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	feedv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/feed/v1"
	notificationv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/notification/v1"
	webhookv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/webhook/v1"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/contract"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/handlers"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/server"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/telemetry"
)

// Open SSE streams get this long after SIGTERM before the process exits anyway.
const shutdownGrace = 5 * time.Second

func main() {
	cfg, err := config.Load()
	if len(os.Args) > 1 && os.Args[1] == "--healthcheck" {
		os.Exit(healthcheck(cfg.Port))
	}
	var level slog.Level
	_ = level.UnmarshalText([]byte(cfg.LogLevel))
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))
	if err != nil {
		slog.Error("configuration", "error", err)
		os.Exit(1)
	}
	if err := run(cfg); err != nil {
		slog.Error("api-gateway stopped", "error", err)
		os.Exit(1)
	}
}

func run(cfg config.Config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTelemetry, err := telemetry.Init(ctx, "api-gateway")
	if err != nil {
		return err
	}
	defer func() { _ = shutdownTelemetry(context.Background()) }()

	spec, err := contract.Load()
	if err != nil {
		return err
	}
	var conns []*grpc.ClientConn
	defer func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}()
	dial := func(address string) (*grpc.ClientConn, error) {
		// Connections are lazy: a missing backend yields 503 on its routes only.
		conn, err := grpc.NewClient(address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
		if err == nil {
			conns = append(conns, conn)
		}
		return conn, err
	}
	var clients handlers.Clients
	for address, assign := range map[string]func(*grpc.ClientConn){
		cfg.AggregatorAddress:   func(c *grpc.ClientConn) { clients.Aggregator = aggregatorv1.NewAggregatorServiceClient(c) },
		cfg.NotificationAddress: func(c *grpc.ClientConn) { clients.Notification = notificationv1.NewDangerNotificationServiceClient(c) },
		cfg.WebhookAddress:      func(c *grpc.ClientConn) { clients.Webhook = webhookv1.NewWebhookServiceClient(c) },
		cfg.FeedAddress:         func(c *grpc.ClientConn) { clients.Feed = feedv1.NewEventFeedServiceClient(c) },
	} {
		conn, err := dial(address)
		if err != nil {
			return err
		}
		assign(conn)
	}

	handler := server.New(server.Options{
		Clients:           clients,
		Contract:          spec,
		RequestTimeout:    cfg.RequestTimeout,
		MaxInFlight:       cfg.MaxInFlight,
		RetryAfterSeconds: cfg.RetryAfterSeconds,
		Heartbeat:         cfg.Heartbeat,
	})
	httpServer := &http.Server{
		Addr:              net.JoinHostPort("", cfg.Port),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout: SSE responses stay open until the feed closes them.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	errs := make(chan error, 1)
	go func() { errs <- httpServer.ListenAndServe() }()
	slog.Info("api-gateway listening", "port", cfg.Port)

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
	}
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

func healthcheck(port string) int {
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		return 1
	}
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return 1
	}
	return 0
}
