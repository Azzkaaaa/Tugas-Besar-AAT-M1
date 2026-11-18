package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/criticaldelivery/reconcile"
)

func run() (bool, error) {
	expectedPath := flag.String("expected", "", "source expected observations JSONL")
	receiptsPath := flag.String("receipts", "", "signed receiver receipts JSONL")
	journalPath := flag.String("journal", "", "complete canonical journal snapshot JSONL")
	public := flag.String("public-key", "", "base64 Ed25519 public key")
	identity := flag.String("receiver-id", "", "designated Field Team receiver")
	output := flag.String("output", "", "report JSON path")
	cutoff := flag.String("cutoff", "", "RFC3339 cutoff after the source window and receipt grace")
	target := flag.Float64("timely-target", .95, "fraction received within the latency limit")
	latency := flag.Duration("latency", 5*time.Second, "maximum timely end-to-end latency")
	flag.Parse()
	if *expectedPath == "" || *receiptsPath == "" || *journalPath == "" || *output == "" {
		return false, fmt.Errorf("expected, receipts, journal and output paths are required")
	}
	key, err := base64.StdEncoding.DecodeString(*public)
	if err != nil {
		return false, err
	}
	end, err := time.Parse(time.RFC3339Nano, *cutoff)
	if err != nil {
		return false, fmt.Errorf("explicit cutoff evidence required")
	}
	expected, err := os.Open(*expectedPath)
	if err != nil {
		return false, err
	}
	defer expected.Close()
	receipts, err := os.Open(*receiptsPath)
	if err != nil {
		return false, err
	}
	defer receipts.Close()
	journal, err := os.Open(*journalPath)
	if err != nil {
		return false, err
	}
	defer journal.Close()
	directory := filepath.Dir(*output)
	if err = os.MkdirAll(directory, 0700); err != nil {
		return false, err
	}
	report, err := reconcile.Run(context.Background(), expected, receipts, reconcile.Options{PublicKey: ed25519.PublicKey(key), ReceiverID: *identity, Cutoff: end, Latency: *latency, TimelyTarget: *target, TempDir: directory, Journal: journal})
	if err != nil {
		return false, err
	}
	file, err := os.CreateTemp(directory, ".critical-report-*.json")
	if err != nil {
		return false, err
	}
	defer os.Remove(file.Name())
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(report); err != nil {
		file.Close()
		return false, err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return false, err
	}
	if err = file.Close(); err != nil {
		return false, err
	}
	if err = os.Rename(file.Name(), *output); err != nil {
		return false, err
	}
	fmt.Printf("Expected=%d received=%d missing=%d late=%d target_met=%v\n", report.Expected, report.Received, report.Missing, report.Late, report.TargetMet)
	return report.TargetMet, nil
}
func main() {
	passed, err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Reconciliation failed:", err)
		os.Exit(1)
	}
	if !passed {
		os.Exit(2)
	}
}
