package server

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/backend"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/contract"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/handlers"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type largeHTTPUpstream struct {
	aggregatorv1.UnimplementedAggregatorServiceServer
	fixture  lifetimeClient
	response *aggregatorv1.GetHazardResponse
	full     atomic.Int32
}

func (s *largeHTTPUpstream) GetHazard(_ context.Context, req *aggregatorv1.GetHazardRequest) (*aggregatorv1.GetHazardResponse, error) {
	if req.GetView() == commonv1.View_VIEW_FULL {
		s.full.Add(1)
		return s.response, nil
	}
	return &aggregatorv1.GetHazardResponse{Item: s.fixture.item(req.GetView())}, nil
}

type checkingHTTPWriter struct {
	*stalledHTTPWriter
	want  []*structpb.Value
	t     *testing.T
	bytes int
}

func (w *checkingHTTPWriter) Write(body []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	w.bytes += len(body)
	pos := bytes.Index(body, []byte(`"detection_keys":[`))
	if pos < 0 {
		w.t.Error("complete HTTP attributes are missing")
		return len(body), nil
	}
	pos += len(`"detection_keys":[`)
	for i, want := range w.want {
		if i > 0 {
			if pos >= len(body) || body[pos] != ',' {
				w.t.Error("HTTP key array ended early", i)
				break
			}
			pos++
		}
		key := want.GetStringValue()
		if len(body)-pos < len(key)+2 || body[pos] != '"' || string(body[pos+1:pos+1+len(key)]) != key || body[pos+1+len(key)] != '"' {
			w.t.Error("HTTP detection key changed or truncated", i)
			break
		}
		pos += len(key) + 2
	}
	if pos >= len(body) || body[pos] != ']' {
		w.t.Error("HTTP key count differs")
	}
	return len(body), nil
}

// This opt-in fixture includes the immutable upstream reply and its real gRPC
// server in the same capped process as the gateway. It measures transport,
// mapping and generated HTTP serialization, not production ingest/storage.
func TestFullHTTPLargeFireMemory(t *testing.T) {
	if os.Getenv("AGG_TEST_HTTP_STRESS") != "1" {
		t.Skip("set AGG_TEST_HTTP_STRESS=1 inside a 256MiB-capped native process")
	}
	const count = 200000
	started := time.Now()
	upstream := &largeHTTPUpstream{}
	item := upstream.fixture.item(commonv1.View_VIEW_FULL)
	values := make([]*structpb.Value, count)
	for i := range values {
		values[i] = structpb.NewStringValue(fmt.Sprintf("N20|2026-11-19|0600|%.8f|106.12345678", -6.1+float64(i)*.00000001))
	}
	item.GetFull().Attributes = &structpb.Struct{Fields: map[string]*structpb.Value{
		"detection_keys": structpb.NewListValue(&structpb.ListValue{Values: values}), "detection_count": structpb.NewNumberValue(count)}}
	upstream.response = &aggregatorv1.GetHazardResponse{Item: item}
	listen, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	defer server.Stop()
	aggregatorv1.RegisterAggregatorServiceServer(server, upstream)
	go func() { _ = server.Serve(listen) }()
	conn, err := backend.Dial(listen.Addr().String(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	doc, err := contract.Load()
	if err != nil {
		t.Fatal(err)
	}
	handler := New(Options{Contract: doc, Clients: handlers.Clients{Aggregator: aggregatorv1.NewAggregatorServiceClient(conn)},
		MaxInFlight: 16, ReservedInFlight: 2, RequestTimeout: 10 * time.Second})
	writer := &checkingHTTPWriter{stalledHTTPWriter: &stalledHTTPWriter{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{})}, want: values, t: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(writer, httptest.NewRequest("GET", "/api/v1/hazards/fire?view=full", nil))
	}()
	select {
	case <-writer.entered:
	case <-time.After(15 * time.Second):
		t.Fatal("large HTTP response never reached writer")
	}
	var burst sync.WaitGroup
	for range 24 {
		burst.Go(func() {
			reply := httptest.NewRecorder()
			handler.ServeHTTP(reply, httptest.NewRequest("GET", "/api/v1/hazards/fire?view=full", nil))
			if reply.Code != http.StatusTooManyRequests {
				t.Error("another large response admitted", reply.Code)
			}
		})
	}
	burst.Wait()
	reply := httptest.NewRecorder()
	handler.ServeHTTP(reply, httptest.NewRequest("GET", "/api/v1/hazards/fire?view=summary", nil))
	if reply.Code != 200 || upstream.full.Load() != 1 {
		t.Error("shedding or summary isolation failed", reply.Code, upstream.full.Load())
	}
	close(writer.release)
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("large HTTP response failed to finish")
	}
	var rss, peak string
	if value, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(value), "\n") {
			if strings.HasPrefix(line, "VmHWM:") {
				rss = strings.TrimSpace(strings.TrimPrefix(line, "VmHWM:"))
			}
		}
	}
	if value, err := os.ReadFile("/sys/fs/cgroup/memory.peak"); err == nil {
		peak = strings.TrimSpace(string(value))
	}
	t.Logf("keys=%d rpc_bytes=%d http_bytes=%d shed_full_requests=24 upstream_full_reads=%d peak_rss=%s container_peak_bytes=%s elapsed=%s", count, proto.Size(upstream.response), writer.bytes, upstream.full.Load(), rss, peak, time.Since(started))
}
