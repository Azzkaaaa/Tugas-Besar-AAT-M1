package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/auth"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/broker"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/grpc"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/ingest"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader/elect"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/m2"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/open"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/telemetry"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	log.Println("[Aggregator] Starting BNPB Aggregator Service...")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Fatal configuration error: %v", err)
	}
	log.Printf("[Aggregator] Loaded configuration: GRPCPort=%s, BMKG=%s, PVMBG=%s", cfg.GRPCPort, cfg.BMKGBaseURL, cfg.PVMBGBaseURL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdownTele, err := telemetry.Init(ctx, "aggregator-service")
	if err != nil {
		log.Printf("[Aggregator] Warning: telemetry init failed: %v", err)
	} else {
		defer func() {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			if err := shutdownTele(shutdownCtx); err != nil {
				log.Printf("[Aggregator] Error shutting down telemetry: %v", err)
			}
		}()
	}

	// Handle graceful shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("[Aggregator] Received signal %v, initiating graceful shutdown...", sig)
		cancel()
	}()

	// 1. Open the canonical store (CANONICAL_STORE_BACKEND: postgres or cassandra).
	log.Printf("[Aggregator] Connecting to canonical %s store...", cfg.CanonicalStoreBackend)
	store, err := open.Open(ctx, cfg)
	if err != nil {
		log.Fatalf("[Aggregator] FATAL: Failed opening canonical store: %v", err)
	}
	defer store.Close()
	log.Println("[Aggregator] Canonical store connected and migrations applied successfully.")
	m2cfg, err := m2.LoadConfig()
	if err != nil {
		log.Fatalf("[Aggregator] M2 configuration: %v", err)
	}
	m2runner := m2.NewRunner(m2cfg, store.Backend)

	// 2. Leader election (LEADER_ELECTION; default: the store backend). Each leader-only job has
	// its own lease, so jobs fail over independently and spread across replicas; every replica
	// serves gRPC.
	electors, err := elect.Open(cfg, store)
	if err != nil {
		log.Fatalf("[Aggregator] FATAL: Invalid leader election configuration: %v", err)
	}
	log.Printf("[Aggregator] Leader election: %s (one lease per job, prefix %q)", cfg.Leader.Election, cfg.Leader.LeasePrefix)

	// 3. RabbitMQ transactional outbox publisher: on every replica, or as a leader job when the
	// store allows a single outbox consumer.
	pub := broker.NewOutboxPublisher(cfg, store.Outbox)
	jobs := map[string]func(ctx context.Context){
		elect.JobBMKGPoller:   func(ctx context.Context) { ingest.NewBMKGPoller(cfg, store.Hazards, store.Sources).Start(ctx) },
		elect.JobPVMBGPoller:  func(ctx context.Context) { ingest.NewPVMBGPoller(cfg, store.Hazards, store.Sources).Start(ctx) },
		elect.JobOutboxPruner: func(ctx context.Context) { broker.RunOutboxPruner(ctx, store.Outbox, cfg.OutboxRetention) },
	}
	if store.OutboxSingleConsumer {
		jobs[elect.JobOutboxPublisher] = pub.Run
	} else {
		log.Println("[Aggregator] Starting RabbitMQ Outbox Publisher...")
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
			log.Println("[Aggregator] Leader jobs did not stop within 5s; their leases expire on their own")
		}
	}()
	log.Printf("[Aggregator] Campaigning for %d leader jobs...", len(jobs))

	// 4. Initialize Auth JWT Verifier. Discovery and JWKS load in the background (retried until the
	// issuer behind Envoy answers), so ingestion never waits on auth; only bad config is fatal.
	verifier, err := auth.NewJWTVerifier(ctx, auth.JWTVerifierConfig{
		AuthAuthority:      cfg.AuthAuthority,
		Audience:           cfg.AuthAudience,
		ServiceToken:       cfg.ServiceToken,
		InsecureSkipVerify: cfg.AuthInsecureSkipVerify,
	})
	if err != nil {
		log.Fatalf("[Aggregator] FATAL: Invalid JWT verifier configuration: %v", err)
	}
	defer verifier.Close()
	if m2cfg.HTTPAddress != "" {
		httpServer := &http.Server{Addr: m2cfg.HTTPAddress, Handler: m2runner.AuditHandler(verifier), ReadHeaderTimeout: 3 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
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
	log.Printf("[Aggregator] Starting gRPC server on port %s...", cfg.GRPCPort)
	if err := srv.Start(ctx); err != nil {
		log.Fatalf("[Aggregator] gRPC server error: %v", err)
	}

	log.Println("[Aggregator] Service stopped cleanly.")
}
