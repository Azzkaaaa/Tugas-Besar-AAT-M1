package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/auth"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/consumer"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/dispatcher"
	grpcserver "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/grpc"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/store"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/telemetry"
)

func main() {
	log.Println("[WebhookProvider] Starting BNPB Webhook Provider Service...")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[WebhookProvider] Fatal configuration error: %v", err)
	}
	log.Printf("[WebhookProvider] Config loaded: port=%s queue=%s", cfg.GRPCPort, cfg.ConsumerQueue)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdownTele, err := telemetry.Init(ctx, "webhook-provider")
	if err != nil {
		log.Printf("[WebhookProvider] Warning: telemetry init failed: %v", err)
	} else {
		defer func() {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			if err := shutdownTele(shutdownCtx); err != nil {
				log.Printf("[WebhookProvider] Error shutting down telemetry: %v", err)
			}
		}()
	}

	// Graceful shutdown on SIGINT / SIGTERM.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("[WebhookProvider] Received signal %v — shutting down", sig)
		cancel()
	}()

	// 1. Open SQLite store.
	log.Println("[WebhookProvider] Opening SQLite store...")
	db, err := store.Connect(ctx, cfg.StoreDSN)
	if err != nil {
		log.Fatalf("[WebhookProvider] FATAL: Failed to open store: %v", err)
	}
	defer db.Close()
	log.Println("[WebhookProvider] SQLite store ready.")

	// 2. Initialise JWT verifier.
	verifier, err := auth.NewJWTVerifier(cfg.AuthPublicKeyPEM, cfg.AuthPublicKeyPath, cfg.ServiceToken)
	if err != nil {
		log.Fatalf("[WebhookProvider] FATAL: Failed initialising JWT verifier: %v", err)
	}

	// 3. Initialise HTTP dispatcher.
	disp := dispatcher.New(cfg.HTTPTimeoutMS, cfg.MaxDeliveryRetries)

	// 4. Start RabbitMQ consumer (background goroutine).
	log.Println("[WebhookProvider] Starting RabbitMQ consumer...")
	cons := consumer.New(cfg, db, disp)
	go cons.Start(ctx)

	// 5. Start gRPC server (blocks until ctx done).
	log.Printf("[WebhookProvider] Starting gRPC server on port %s...", cfg.GRPCPort)
	srv := grpcserver.NewServer(cfg, db, verifier, disp)
	if err := srv.Start(ctx); err != nil {
		log.Fatalf("[WebhookProvider] gRPC server error: %v", err)
	}

	log.Println("[WebhookProvider] Service stopped cleanly.")
}
