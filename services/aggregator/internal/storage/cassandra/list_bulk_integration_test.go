//go:build integration

package cassandra

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/auth"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	apigrpc "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/grpc"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// A 512MiB process runs the real storage List, RPC handler, protobuf serializer,
// socket and client decoder. The fixture isolates read projections (canonical
// ingest/outbox and the public HTTP gateway are separate acceptance work).
func TestListLargeSatelliteRPCBudget(t *testing.T) {
	if os.Getenv("AGG_TEST_LIST_STRESS") != "1" {
		t.Skip("set AGG_TEST_LIST_STRESS=1 in a memory-capped disposable process")
	}
	db, err := openTest(t, "agg_test_list_bulk_"+uuid.NewString()[:8], time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer dropKeyspace(db)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	started := time.Now()
	template := largeSatelliteRecord(t)
	template.Attributes = domain.NormalizeAttributes(template.Attributes)
	keys := template.Attributes["detection_keys"].([]any)
	if domain.FullProjectionSize(template) < 8<<20 {
		t.Fatal("set AGG_TEST_LARGE_FIRE_KEYS=200000")
	}
	base := time.Now().UTC().Add(-4 * time.Hour).Truncate(time.Hour)
	perShard := [projectionShards]int{}
	var want []*domain.HazardRecord
	prefix := uuid.NewString()
	for attempt := 0; len(want) < 32; attempt++ {
		h := *template
		h.SourceRefID = fmt.Sprintf("list-%s-%d", prefix, attempt)
		h.HazardID = domain.HazardID(h.Source, h.SourceRefID)
		shard := projectionShard(h.HazardID)
		if perShard[shard] >= 4 {
			continue
		}
		h.OccurredAt = base.Add(time.Duration(len(want)%3) * time.Minute)
		h.Revision = 1
		batch := db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
		hazards{db}.addProjection(batch, &h, db.bucketOf(h.OccurredAt), time.Time{}, time.Time{})
		if err := db.execRecoverableBatch(ctx, batch); err != nil {
			t.Fatal("seed projection", len(want), err)
		}
		perShard[shard]++
		want = append(want, storage.HazardSummaryHeader(&h))
	}
	sort.Slice(want, func(i, j int) bool { return listOrderBefore(want[i], want[j]) })
	backend := db.Backend()
	if err := backend.Sources.RecordSuccess(ctx, domain.SourceKeySatellite, base); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		return next(context.WithValue(ctx, auth.CallerContextKey, &auth.CallerContextData{Scopes: []string{"hazards:read:full", "hazards:read:summary"}}), req)
	}))
	aggregatorv1.RegisterAggregatorServiceServer(server, apigrpc.NewServer(&config.Config{SourceStaleThreshold: time.Hour}, backend.Hazards, backend.Sources, nil))
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	clientConn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20)))
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()
	client := aggregatorv1.NewAggregatorServiceClient(clientConn)
	request := &aggregatorv1.ListHazardsRequest{View: commonv1.View_VIEW_SUMMARY, Filter: &commonv1.HazardFilter{Source: domain.SourceToProto(domain.SourceSatellite)}, Page: &commonv1.PageRequest{PageSize: 100}}
	summary, err := client.ListHazards(ctx, request)
	if err != nil || summary.GetPartial() || len(summary.GetItems()) != len(want) || summary.GetNextPageToken() != "" {
		t.Fatal("large summaries", err, summary)
	}
	for i, item := range summary.Items {
		if item.GetSummary().GetHazardId() != want[i].HazardID || item.GetFull() != nil {
			t.Fatal("summary order/detail leak")
		}
	}
	summaryBytes := proto.Size(summary)
	summary = nil
	request.View = commonv1.View_VIEW_FULL
	maxWire, pages := 0, 0
	for {
		response, err := client.ListHazards(ctx, request)
		if err != nil || response.GetPartial() || len(response.GetItems()) != 1 {
			t.Fatalf("full page %d: %v partial=%v items=%d", pages, err, response.GetPartial(), len(response.GetItems()))
		}
		if pages >= len(want) {
			t.Fatal("extra/duplicate full page")
		}
		full := response.Items[0].GetFull()
		if full.GetHazardId() != want[pages].HazardID {
			t.Fatal("global order/cursor changed", pages, full.GetHazardId(), want[pages].HazardID)
		}
		values := full.GetAttributes().GetFields()["detection_keys"].GetListValue().GetValues()
		if len(values) != len(keys) {
			t.Fatal("full detection keys truncated", len(values))
		}
		for i, value := range values {
			if value.GetStringValue() != keys[i].(string) {
				t.Fatal("detection key changed", i)
			}
		}
		maxWire = max(maxWire, proto.Size(response))
		pages++
		next := response.GetNextPageToken()
		if next == "" {
			break
		}
		if next == request.Page.PageToken {
			t.Fatal("byte-limited cursor did not advance")
		}
		request.Page.PageToken = next
	}
	if pages != len(want) {
		t.Fatal("early terminal cursor", pages, len(want))
	}
	var rss, peak string
	if status, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(status), "\n") {
			if strings.HasPrefix(line, "VmHWM:") {
				rss = strings.TrimSpace(strings.TrimPrefix(line, "VmHWM:"))
			}
		}
	}
	if value, err := os.ReadFile("/sys/fs/cgroup/memory.peak"); err == nil {
		peak = strings.TrimSpace(string(value))
	}
	t.Logf("records=%d keys_per_record=%d shards=%v summary_bytes=%d full_pages=%d max_rpc_bytes=%d peak_rss=%s container_peak_bytes=%s elapsed=%s", len(want), len(keys), perShard, summaryBytes, pages, maxWire, rss, peak, time.Since(started))
}
