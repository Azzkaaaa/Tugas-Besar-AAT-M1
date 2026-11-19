package handlers

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	feedv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/feed/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/problem"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ownedFeed struct {
	feedv1.EventFeedServiceClient
	blockHeader bool
	sub         *ownedSubscription
}

func (f *ownedFeed) SubscribeEvents(ctx context.Context, _ *feedv1.SubscribeEventsRequest, _ ...grpc.CallOption) (feedv1.EventFeedService_SubscribeEventsClient, error) {
	f.sub = &ownedSubscription{ctx: ctx, blockHeader: f.blockHeader, first: make(chan struct{})}
	return f.sub, nil
}

type ownedSubscription struct {
	grpc.ClientStream
	ctx         context.Context
	blockHeader bool
	calls       atomic.Int32
	first       chan struct{}
}

func (s *ownedSubscription) Header() (metadata.MD, error) {
	if s.blockHeader {
		<-s.ctx.Done()
		return nil, s.ctx.Err()
	}
	return metadata.Pairs("accepted", "true"), nil
}

func (s *ownedSubscription) Recv() (*feedv1.SubscribeEventsResponse, error) {
	if s.calls.Add(1) == 1 {
		close(s.first)
		now := timestamppb.Now()
		return &feedv1.SubscribeEventsResponse{Event: &feedv1.FeedEvent{
			EventId: "event-1", Sequence: 1, ResumeCursor: "cursor-1", PublishedAt: now,
			Hazard: &commonv1.HazardProjection{Data: &commonv1.HazardProjection_Summary{Summary: &commonv1.HazardSummary{
				HazardId: "hazard-1", Source: commonv1.Source_SOURCE_SATELITE, HazardType: commonv1.HazardType_HAZARD_TYPE_WILDFIRE,
				Severity: commonv1.Severity_SEVERITY_AWAS, OccurredAt: now, IngestedAt: now,
			}}},
		}}, nil
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func TestStreamDoesNotPrefetchWhileOutputIsBlockedAndCloseJoins(t *testing.T) {
	feed := &ownedFeed{}
	h := &Handlers{Clients: Clients{Feed: feed}, Timeout: time.Second, Heartbeat: time.Hour}
	response, err := h.stream(context.Background(), nil, commonv1.View_VIEW_SUMMARY, nil)
	if err != nil {
		t.Fatal(err)
	}
	body := response.Body.(*streamBody)
	defer body.Close()
	select {
	case <-feed.sub.first:
	case <-time.After(time.Second):
		t.Fatal("first event was not received")
	}
	time.Sleep(30 * time.Millisecond)
	if calls := feed.sub.calls.Load(); calls != 1 {
		t.Fatalf("decoded another message while the first frame is blocked: %d", calls)
	}
	closed := make(chan struct{})
	go func() { _ = body.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("body close did not join blocked workers")
	}
	select {
	case <-body.done:
	default:
		t.Fatal("body released before pump completed")
	}
	if feed.sub.ctx.Err() == nil {
		t.Fatal("subscription was not cancelled")
	}
}

func TestClientCancellationJoinsIdleReceive(t *testing.T) {
	feed := &ownedFeed{}
	h := &Handlers{Clients: Clients{Feed: feed}, Timeout: time.Second, Heartbeat: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	response, err := h.stream(ctx, nil, commonv1.View_VIEW_SUMMARY, nil)
	if err != nil {
		t.Fatal(err)
	}
	body := response.Body.(*streamBody)
	defer body.Close()
	// Drain the first frame so the worker enters the next, idle Recv.
	buffer := make([]byte, 4096)
	if _, err := body.Read(buffer); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for feed.sub.calls.Load() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if feed.sub.calls.Load() != 2 {
		t.Fatal("receiver did not become idle")
	}
	cancel()
	select {
	case <-body.done:
	case <-time.After(time.Second):
		t.Fatal("request cancellation left an idle receive alive")
	}
	if _, err := body.Read(buffer); !errors.Is(err, io.EOF) {
		t.Fatalf("cancelled body should end cleanly: %v", err)
	}
}

func TestFeedOpeningTimeoutIsRetryable(t *testing.T) {
	feed := &ownedFeed{blockHeader: true}
	h := &Handlers{Clients: Clients{Feed: feed}, Timeout: 20 * time.Millisecond}
	_, err := h.stream(context.Background(), nil, commonv1.View_VIEW_SUMMARY, nil)
	var failure *problem.Error
	if !errors.As(err, &failure) || failure.Status != 503 || feed.sub.ctx.Err() == nil {
		t.Fatalf("opening timeout did not cancel and return 503: %v", err)
	}
}
