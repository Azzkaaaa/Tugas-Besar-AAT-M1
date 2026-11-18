package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/auth"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/broker"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/criticaldelivery"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/grpc"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/ingest"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader/elect"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/m2"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/problem3"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/open"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/telemetry"
)

func main() {
	slog.SetDefault(telemetry.NewLogger(os.Stdout, "aggregator"))
	if os.Getenv("AGGREGATOR_MODE") == "critical-delivery-worker" {
		if err := criticaldelivery.RunWorkerMain(); err != nil {
			telemetry.Fatal("startup failed", "error", err)
		}
		return
	}
	slog.Info("starting aggregator")

	workerMode := os.Getenv("AGGREGATOR_MODE") == "field-report-worker"
	var cfg *config.Config
	var err error
	if workerMode {
		var cassandra config.CassandraConfig
		cassandra, err = config.LoadCassandra()
		cfg = &config.Config{CanonicalStoreBackend: "cassandra", Cassandra: cassandra, OutboxRetention: 168 * time.Hour}
	} else {
		cfg, err = config.Load()
	}
	if err != nil {
		telemetry.Fatal("invalid configuration", "error", err)
	}
	slog.Info("configuration loaded", "grpc_port", cfg.GRPCPort, "bmkg_url", cfg.BMKGBaseURL, "pvmbg_url", cfg.PVMBGBaseURL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdownTele, err := telemetry.Init(ctx, "aggregator-service")
	if err != nil {
		slog.Warn("telemetry init failed", "error", err)
	} else {
		defer func() {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			if err := shutdownTele(shutdownCtx); err != nil {
				slog.Warn("telemetry shutdown failed", "error", err)
			}
		}()
	}

	// Handle graceful shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		slog.Info("shutdown signal received", "signal", sig.String())
		cancel()
	}()

	// 1. Open the canonical store (CANONICAL_STORE_BACKEND: postgres or cassandra).
	slog.Info("connecting to canonical store", "backend", cfg.CanonicalStoreBackend)
	store, err := open.Open(ctx, cfg)
	if err != nil {
		telemetry.Fatal("opening canonical store failed", "error", err)
	}
	defer store.Close()
	slog.Info("canonical store ready")
	if workerMode {
		if store.Reports == nil {
			telemetry.Fatal("field-report-worker requires Cassandra")
		}
		go serveProblem3HTTP(ctx, cancel, store, nil)
		problem3.RunWorker(ctx, store.Reports)
		return
	}
	m2cfg, err := m2.LoadConfig()
	if err != nil {
		telemetry.Fatal("invalid M2 configuration", "error", err)
	}
	m2runner := m2.NewRunner(m2cfg, store.Backend)
	var pipeline *ingest.Pipeline
	if store.Inbox != nil {
		const defaultLimit = 16 << 20
		limit := int64(defaultLimit)
		if value := os.Getenv("INGEST_MAX_RESPONSE_BYTES"); value != "" {
			limit, err = strconv.ParseInt(value, 10, 32)
			if err != nil || limit < 1 || limit > 64<<20 {
				telemetry.Fatal("INGEST_MAX_RESPONSE_BYTES must be between 1 and 67108864")
			}
		}
		pipeline = ingest.NewPipeline(store.Inbox, store.Hazards, os.Getenv("PVMBG_SCHEMA_ADAPTERS") == "true", limit)
	}

	// 2. Leader election (LEADER_ELECTION; default: the store backend). Each leader-only job has
	// its own lease, so jobs fail over independently and spread across replicas; every replica
	// serves gRPC.
	electors, err := elect.Open(cfg, store)
	if err != nil {
		telemetry.Fatal("invalid leader election configuration", "error", err)
	}
	slog.Info("leader election configured", "election", cfg.Leader.Election, "lease_prefix", cfg.Leader.LeasePrefix)

	// 3. RabbitMQ transactional outbox publisher: on every replica, or as a leader job when the
	// store allows a single outbox consumer.
	pub := broker.NewOutboxPublisher(cfg, store.Outbox)
	jobs := map[string]func(ctx context.Context){
		elect.JobBMKGPoller: func(ctx context.Context) {
			poller := ingest.NewBMKGPoller(cfg, store.Hazards, store.Sources)
			poller.SetPipeline(pipeline)
			poller.Start(ctx)
		},
		elect.JobPVMBGPoller: func(ctx context.Context) {
			poller := ingest.NewPVMBGPoller(cfg, store.Hazards, store.Sources)
			poller.SetPipeline(pipeline)
			poller.Start(ctx)
		},
		elect.JobOutboxPruner: func(ctx context.Context) { broker.RunOutboxPruner(ctx, store.Outbox, cfg.OutboxRetention) },
	}
	if store.OutboxSingleConsumer {
		jobs[elect.JobOutboxPublisher] = pub.Run
	} else {
		slog.Info("starting outbox publisher")
		pub.Start(ctx)
	}
	if m2cfg.SatelliteURL != "" {
		jobs["satellite-stream"] = m2runner.Satellite
	}
	if m2cfg.CoastalURL != "" {
		jobs["coastal-poller"] = m2runner.Coastal
	}
	var leaders sync.WaitGroup
	for job, run := range jobs {
		leaders.Add(1)
		go func() { defer leaders.Done(); electors.For(job).Run(ctx, run) }()
	}
	// On shutdown, give the electors a moment to stop their jobs and release their leases.
	defer func() {
		done := make(chan struct{})
		go func() { leaders.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			slog.Warn("leader jobs did not stop within 5s; their leases expire on their own")
		}
	}()
	slog.Info("campaigning for leader jobs", "jobs", len(jobs))

	// 4. Initialize Auth JWT Verifier. Discovery and JWKS load in the background (retried until the
	// issuer behind Envoy answers), so ingestion never waits on auth; only bad config is fatal.
	verifier, err := auth.NewJWTVerifier(ctx, auth.JWTVerifierConfig{
		AuthAuthority:      cfg.AuthAuthority,
		Audience:           cfg.AuthAudience,
		ServiceToken:       cfg.ServiceToken,
		InsecureSkipVerify: cfg.AuthInsecureSkipVerify,
	})
	if err != nil {
		telemetry.Fatal("invalid JWT verifier configuration", "error", err)
	}
	defer verifier.Close()
	address := m2cfg.HTTPAddress
	if address == "" && store.Inbox != nil {
		address = ":9092"
	}
	if address != "" {
		// One listener serves P3 operations and P4/P5 audit. Cassandra adapters
		// park into P3's inbox; PostgreSQL keeps the standalone M2 fallback.
		handler := problem3.Handler(store.Backend, verifier, m2cfg.AuditToken, m2runner.AuditHandler(verifier))
		criticalAPI, err := criticaldelivery.LoadAPI()
		if err != nil {
			telemetry.Fatal("startup failed", "error", err)
		}
		criticalAPI.Journal = store.Critical
		handler = criticalAPI.Handler(handler)
		httpServer := &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second}
		go func() {
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("M2 HTTP server failed", "error", err)
				cancel()
			}
		}()
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = httpServer.Shutdown(ctx)
		}()
	}

	// 5. Initialize & Start gRPC Server
	srv := grpc.NewServer(cfg, store.Hazards, store.Sources, verifier)
	srv.SetFieldReports(store.Reports)
	srv.SetAnalytics(store.Analytics)
	slog.Info("starting gRPC server", "grpc_port", cfg.GRPCPort)
	if err := srv.Start(ctx); err != nil {
		telemetry.Fatal("gRPC server failed", "error", err)
	}

	slog.Info("aggregator stopped")
}

func serveProblem3HTTP(ctx context.Context, cancel context.CancelFunc, store *open.Store, verifier *auth.JWTVerifier) {
	port := os.Getenv("AGGREGATOR_INTERNAL_HTTP_PORT")
	if port == "" {
		port = "9092"
	}
	if err := problem3.StartHTTP(ctx, port, store.Backend, verifier, os.Getenv("AUDIT_API_TOKEN")); err != nil {
		slog.Error("internal HTTP server failed", "error", err)
		cancel()
	}
}
