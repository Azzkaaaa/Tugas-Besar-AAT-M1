package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/auth"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/broker"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/grpc"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/ingest"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/store"
)

func main() {
	log.Println("[Aggregator] Starting BNPB Aggregator Service...")

	cfg := config.Load()
	log.Printf("[Aggregator] Loaded configuration: GRPCPort=%s, BMKG=%s, PVMBG=%s", cfg.GRPCPort, cfg.BMKGBaseURL, cfg.PVMBGBaseURL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("[Aggregator] Received signal %v, initiating graceful shutdown...", sig)
		cancel()
	}()

	// 1. Initialize Canonical Store (PostgreSQL)
	log.Println("[Aggregator] Connecting to canonical PostgreSQL store...")
	db, err := store.Connect(ctx, cfg.CanonicalStoreDSN)
	if err != nil {
		log.Fatalf("[Aggregator] FATAL: Failed connecting to database: %v", err)
	}
	defer db.Pool.Close()
	log.Println("[Aggregator] Database connected and migrations applied successfully.")

	// 2. Initialize RabbitMQ Transactional Outbox Publisher
	log.Println("[Aggregator] Starting RabbitMQ Outbox Publisher...")
	pub := broker.NewOutboxPublisher(cfg, db)
	pub.Start(ctx)

	// 3. Initialize & Start Ingestion Pollers
	log.Println("[Aggregator] Starting upstream ingestion pollers (BMKG & PVMBG)...")
	bmkgPoller := ingest.NewBMKGPoller(cfg, db)
	go bmkgPoller.Start(ctx)

	pvmbgPoller := ingest.NewPVMBGPoller(cfg, db)
	go pvmbgPoller.Start(ctx)

	// 4. Initialize Auth JWT Verifier
	verifier, err := auth.NewJWTVerifier(cfg.AuthPublicKeyPEM, cfg.AuthPublicKeyPath, cfg.ServiceToken)
	if err != nil {
		log.Fatalf("[Aggregator] FATAL: Failed initializing JWT verifier: %v", err)
	}

	// 5. Initialize & Start gRPC Server
	srv := grpc.NewServer(cfg, db, verifier)
	log.Printf("[Aggregator] Starting gRPC server on port %s...", cfg.GRPCPort)
	if err := srv.Start(ctx); err != nil {
		log.Fatalf("[Aggregator] gRPC server error: %v", err)
	}

	log.Println("[Aggregator] Service stopped cleanly.")
}
