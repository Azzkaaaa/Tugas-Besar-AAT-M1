// m2replay checks a captured official feed without credentials or external services.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/m2"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

func main() {
	input := flag.String("input", "", "captured journal JSONL")
	output := flag.String("output", "", "write report JSON")
	flag.Parse()
	f, err := os.Open(*input)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	e := m2.NewFireEngine(24*time.Hour, 200000)
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 4096), 4<<20)
	errors := map[string]int{}
	var durations []float64
	count := 0
	started := time.Now()
	for s.Scan() {
		var row storage.StreamEntry
		if err := json.Unmarshal(s.Bytes(), &row); err != nil {
			panic(err)
		}
		t := time.Now()
		_, err := e.Apply(row.Kind, row.Payload, row.Sequence, row.ReceivedAt)
		durations = append(durations, time.Since(t).Seconds())
		if err != nil {
			errors[err.Error()]++
		}
		count++
	}
	if err := s.Err(); err != nil {
		panic(err)
	}
	sort.Float64s(durations)
	p95 := 0.0
	if len(durations) > 0 {
		p95 = durations[(len(durations)-1)*95/100]
	}
	fires, singletons := e.Counts()
	report := map[string]any{"input_events": count, "active_fires": fires, "singletons": singletons, "excluded_low": e.Low, "late": e.Late, "parked_reasons": errors, "processing_seconds": time.Since(started).Seconds(), "p95_compute_seconds": p95, "items": e.Snapshot(), "measurement": "offline CPU processing only; not ingest-to-storage or notification latency"}
	b, _ := json.MarshalIndent(report, "", "  ")
	if *output != "" {
		if err := os.WriteFile(*output, b, 0600); err != nil {
			panic(err)
		}
	} else {
		fmt.Println(string(b))
	}
	delete(report, "items")
	summary, _ := json.Marshal(report)
	fmt.Fprintln(os.Stderr, string(summary))
}
