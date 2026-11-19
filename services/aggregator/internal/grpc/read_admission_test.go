package grpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/auth"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestFullReadCreditFollowsFinalBufferReference(t *testing.T) {
	for _, size := range []int{10, 2 << 20} {
		a := newReadAdmission()
		ctx := a.TagRPC(context.Background(), nil)
		rpc := ctx.Value(readRPCKey{}).(*readRPC)
		a.slot <- struct{}{}
		rpc.credit = &readCredit{release: sync.OnceFunc(func() { <-a.slot })}
		response := &aggregatorv1.GetHazardResponse{UnavailableUnits: []string{strings.Repeat("x", size)}}
		rpc.response = response
		a.pending.Store(response, rpc.credit)
		data, err := a.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		view := data[0].Slice(0, data[0].Len()/2)
		a.HandleRPC(ctx, &stats.End{})
		data.Free()
		if len(a.slot) != 1 {
			t.Fatal("credit released before final transport slice", size)
		}
		view.Free()
		if len(a.slot) != 0 {
			t.Fatal("final slice leaked credit", size)
		}
		// End may also run after a successful final free.
		a.HandleRPC(ctx, &stats.End{})
	}
}

func TestFullReadAdmissionChecksCallerBeforeCapacity(t *testing.T) {
	a := newReadAdmission()
	a.slot <- struct{}{}
	for _, test := range []struct {
		caller *auth.CallerContextData
		want   codes.Code
	}{{nil, codes.Unauthenticated}, {&auth.CallerContextData{Scopes: []string{"hazards:read:summary"}}, codes.PermissionDenied}} {
		ctx := context.Background()
		if test.caller != nil {
			ctx = context.WithValue(ctx, auth.CallerContextKey, test.caller)
		}
		_, err := a.intercept(ctx, &aggregatorv1.GetHazardRequest{View: commonv1.View_VIEW_FULL}, nil, func(context.Context, any) (any, error) {
			t.Error("unauthorized request reached storage")
			return nil, nil
		})
		if status.Code(err) != test.want || len(a.slot) != 1 {
			t.Fatal("caller validation bypassed or changed credit", err)
		}
	}
	<-a.slot
}

type admissionStore struct {
	storage.HazardStore
	mu  sync.Mutex
	get func(context.Context) (*domain.HazardRecord, error)
}

func (s *admissionStore) Get(ctx context.Context, _ string) (*domain.HazardRecord, error) {
	s.mu.Lock()
	get := s.get
	s.mu.Unlock()
	return get(ctx)
}
func (*admissionStore) GetSummary(context.Context, string) (*domain.HazardRecord, error) {
	return &domain.HazardRecord{HazardID: "fire", Source: domain.SourceSatellite}, nil
}
func (*admissionStore) HasData(context.Context, string) (bool, error) { return true, nil }
func (s *admissionStore) set(get func(context.Context) (*domain.HazardRecord, error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.get = get
}

type readEndProbe struct {
	stats.Handler
	end chan struct{}
}

func (*readEndProbe) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context   { return ctx }
func (*readEndProbe) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (*readEndProbe) HandleConn(context.Context, stats.ConnStats)                       {}
func (p *readEndProbe) HandleRPC(_ context.Context, event stats.RPCStats) {
	if _, ok := event.(*stats.End); ok {
		select {
		case p.end <- struct{}{}:
		default:
		}
	}
}

func admissionRPC(t *testing.T, store *admissionStore) (*readAdmission, *readEndProbe, string, aggregatorv1.AggregatorServiceClient) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a := newReadAdmission()
	probe := &readEndProbe{end: make(chan struct{}, 100)}
	options := []grpc.ServerOption{grpc.StatsHandler(probe), grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		return next(context.WithValue(ctx, auth.CallerContextKey, &auth.CallerContextData{Scopes: []string{"hazards:read:full", "hazards:read:summary"}}), req)
	})}
	options = append(options, a.options()...)
	server := grpc.NewServer(options...)
	aggregatorv1.RegisterAggregatorServiceServer(server, NewServer(&config.Config{}, store, unavailableSources{}, nil))
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return a, probe, lis.Addr().String(), aggregatorv1.NewAggregatorServiceClient(cc)
}

func waitReadSlot(t *testing.T, a *readAdmission, want int) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for len(a.slot) != want {
		select {
		case <-deadline.C:
			t.Fatalf("full credit count %d, want %d", len(a.slot), want)
		case <-tick.C:
		}
	}
}

func TestFullRPCBackpressureHoldsCreditAfterHandlerEnds(t *testing.T) {
	store := &admissionStore{get: func(context.Context) (*domain.HazardRecord, error) {
		return &domain.HazardRecord{HazardID: "fire", Source: domain.SourceSatellite, Attributes: map[string]any{"body": strings.Repeat("x", 4<<20)}}, nil
	}}
	a, probe, addr, client := admissionRPC(t, store)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write([]byte(http2.ClientPreface)); err != nil {
		t.Fatal(err)
	}
	frames := http2.NewFramer(conn, conn)
	// A peer that provides no response window forces the full encoded body to
	// stay in gRPC's queue after the handler and stats.End have completed.
	if err := frames.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 0}); err != nil {
		t.Fatal(err)
	}
	var headers bytes.Buffer
	encoder := hpack.NewEncoder(&headers)
	for _, field := range []hpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "http"}, {Name: ":authority", Value: addr},
		{Name: ":path", Value: aggregatorv1.AggregatorService_GetHazard_FullMethodName}, {Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"}} {
		if err := encoder.WriteField(field); err != nil {
			t.Fatal(err)
		}
	}
	if err := frames.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: headers.Bytes(), EndHeaders: true}); err != nil {
		t.Fatal(err)
	}
	request, err := proto.Marshal(&aggregatorv1.GetHazardRequest{HazardId: "fire", View: commonv1.View_VIEW_FULL})
	if err != nil {
		t.Fatal(err)
	}
	message := make([]byte, 5+len(request))
	binary.BigEndian.PutUint32(message[1:5], uint32(len(request)))
	copy(message[5:], request)
	if err := frames.WriteData(1, true, message); err != nil {
		t.Fatal(err)
	}
	select {
	case <-probe.end:
	case <-ctx.Done():
		t.Fatal("stalled RPC did not reach handler completion", ctx.Err())
	}
	if len(a.slot) != 1 {
		t.Fatal("handler completion returned credit for queued response")
	}
	var burst sync.WaitGroup
	for range 12 {
		burst.Go(func() {
			if _, err := client.GetHazard(ctx, &aggregatorv1.GetHazardRequest{HazardId: "fire", View: commonv1.View_VIEW_FULL}); status.Code(err) != codes.ResourceExhausted {
				t.Error("second full request admitted behind queued body", err)
			}
		})
	}
	burst.Wait()
	if reply, err := client.GetHazard(ctx, &aggregatorv1.GetHazardRequest{HazardId: "fire", View: commonv1.View_VIEW_SUMMARY}); err != nil || reply.GetItem().GetSummary().GetHazardId() != "fire" {
		t.Fatal("full backpressure blocked summary", err)
	}
	if err := frames.WriteRSTStream(1, http2.ErrCodeCancel); err != nil {
		t.Fatal(err)
	}
	waitReadSlot(t, a, 0)
	// Requests using gzip must still get a complete response, with identity
	// output ensuring the tracked buffer remains the actual queued payload.
	reply, err := client.GetHazard(ctx, &aggregatorv1.GetHazardRequest{HazardId: "fire", View: commonv1.View_VIEW_FULL}, grpc.UseCompressor("gzip"))
	if err != nil || len(reply.GetItem().GetFull().GetAttributes().GetFields()["body"].GetStringValue()) != 4<<20 {
		t.Fatal("full response failed after cancelled peer", err)
	}
	waitReadSlot(t, a, 0)
}

func TestFullRPCErrorsAndCancellationReturnCredit(t *testing.T) {
	store := &admissionStore{}
	a, _, _, client := admissionRPC(t, store)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &aggregatorv1.GetHazardRequest{HazardId: "fire", View: commonv1.View_VIEW_FULL}
	// Marshal failure happens after the handler has returned its response.
	store.set(func(context.Context) (*domain.HazardRecord, error) {
		return &domain.HazardRecord{HazardID: "fire", Source: domain.SourceSatellite, AreaName: "\xff"}, nil
	})
	if _, err := client.GetHazard(ctx, req); status.Code(err) != codes.Internal {
		t.Fatal("expected protobuf marshal failure", err)
	}
	waitReadSlot(t, a, 0)
	store.set(func(context.Context) (*domain.HazardRecord, error) { return nil, nil })
	if _, err := client.GetHazard(ctx, req); status.Code(err) != codes.NotFound {
		t.Fatal("not-found call failed", err)
	}
	waitReadSlot(t, a, 0)
	entered := make(chan struct{})
	store.set(func(ctx context.Context) (*domain.HazardRecord, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	callCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { _, err := client.GetHazard(callCtx, req); done <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stop()
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatal("expected cancellation", err)
	}
	waitReadSlot(t, a, 0)
	store.set(func(context.Context) (*domain.HazardRecord, error) { return nil, context.DeadlineExceeded })
	for range 3 {
		if reply, err := client.GetHazard(ctx, req); err != nil || !reply.GetPartial() {
			t.Fatal("small partial response leaked capacity", err)
		}
		waitReadSlot(t, a, 0)
	}
}
