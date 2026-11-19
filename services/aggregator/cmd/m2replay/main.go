// m2replay checks a captured feed with the production disk processor.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/m2"
)

func run() error {
	input := flag.String("input", "", "captured journal JSONL, starting at sequence 1")
	output := flag.String("output", "", "write report JSON (default stdout)")
	workDir := flag.String("work-dir", "", "parent for temporary disk state (default OS temporary directory)")
	lateness := flag.Duration("lateness", 24*time.Hour, "allowed event-time lateness")
	retention := flag.Duration("retention", m2.DefaultRetention, "correction horizon")
	maxPoints := flag.Int("max-detections", 200000, "soft detection pressure threshold; state remains on disk")
	flag.Parse()
	f, err := os.Open(*input)
	if err != nil {
		return err
	}
	defer f.Close()
	var out io.Writer = os.Stdout
	var temporary *os.File
	if *output != "" {
		temporary, err = os.CreateTemp(filepath.Dir(*output), ".m2replay-*.json")
		if err != nil {
			return err
		}
		defer func() { temporary.Close(); os.Remove(temporary.Name()) }()
		out = temporary
	}
	summary, err := m2.ReplayJournal(context.Background(), f, out, m2.ReplayOptions{WorkDir: *workDir, Lateness: *lateness, Retention: *retention, MaxDetections: *maxPoints})
	if err != nil {
		return err
	}
	if temporary != nil {
		if err := temporary.Close(); err != nil {
			return err
		}
		if err := os.Rename(temporary.Name(), *output); err != nil {
			return err
		}
	}
	return json.NewEncoder(os.Stderr).Encode(summary)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
