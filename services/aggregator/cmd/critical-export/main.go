package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/criticaldelivery"
)

func run() error {
	base := flag.String("api", "", "Aggregator internal critical API base URL")
	output := flag.String("output", "", "complete canonical journal JSONL path")
	flag.Parse()
	if *base == "" || *output == "" || len(os.Getenv("CRITICAL_DELIVERY_TOKEN")) < 16 {
		return fmt.Errorf("api, output and CRITICAL_DELIVERY_TOKEN are required")
	}
	directory := filepath.Dir(*output)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, ".critical-journal-*.jsonl")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	w := criticaldelivery.Worker{APIURL: *base, Token: os.Getenv("CRITICAL_DELIVERY_TOKEN"), Client: criticaldelivery.NewHTTPClient()}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	if err = w.ExportJournal(ctx, file); err != nil {
		file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), *output)
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Critical export failed:", err)
		os.Exit(1)
	}
}
