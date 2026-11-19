package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	feedv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/feed/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/contract"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/handlers"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type admittedFeed struct {
	feedv1.EventFeedServiceClient
	calls atomic.Int32
}

func (f *admittedFeed) SubscribeEvents(ctx context.Context, request *feedv1.SubscribeEventsRequest, _ ...grpc.CallOption) (feedv1.EventFeedService_SubscribeEventsClient, error) {
	f.calls.Add(1)
	return &admittedSubscription{ctx: ctx, event: &feedv1.SubscribeEventsResponse{Event: &feedv1.FeedEvent{
		EventId: "event-1", Sequence: 1, ResumeCursor: "cursor-1", PublishedAt: timestamppb.Now(), Hazard: (&lifetimeClient{}).item(request.GetView()),
	}}}, nil
}

type admittedSubscription struct {
	grpc.ClientStream
	ctx   context.Context
	event *feedv1.SubscribeEventsResponse
}

func (*admittedSubscription) Header() (metadata.MD, error) {
	return metadata.Pairs("accepted", "true"), nil
}
func (s *admittedSubscription) Recv() (*feedv1.SubscribeEventsResponse, error) {
	if s.event != nil {
		event := s.event
		s.event = nil
		return event, nil
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func streamHandler(t *testing.T, capacity int) (http.Handler, *admittedFeed) {
	t.Helper()
	doc, err := contract.Load()
	if err != nil {
		t.Fatal(err)
	}
	feed := &admittedFeed{}
	return New(Options{Contract: doc, Clients: handlers.Clients{Feed: feed, Aggregator: &lifetimeClient{}},
		MaxInFlight: capacity, ReservedInFlight: 1, RequestTimeout: time.Second, Heartbeat: time.Hour,
		PriorityAllowed: func(r *http.Request) bool { return r.Header.Get("Authorization") == "Bearer verified-field" }}), feed
}

type stopStreamWriter struct{ *httptest.ResponseRecorder }

func (*stopStreamWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestFullStreamsShareCapacityAcrossAliasesUntilOutputCleanup(t *testing.T) {
	handler, feed := streamHandler(t, 4)
	writer := &stalledHTTPWriter{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{}), failure: "error"}
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(writer, httptest.NewRequest("GET", "/api/v1/feeds/events?view=full", nil))
	}()
	select {
	case <-writer.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not reach HTTP output")
	}
	for _, alias := range []string{"events", "seismic", "volcanic"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", "/api/v1/feeds/"+alias+"?view=full", nil))
		if response.Code != 429 {
			t.Fatalf("FULL %s bypassed shared admission: %d %s", alias, response.Code, response.Body.String())
		}
	}
	if feed.calls.Load() != 1 {
		t.Fatal("rejected requests opened upstream subscriptions")
	}
	// An occupied FULL stream does not consume unary or SUMMARY capacity.
	for _, path := range []string{"/api/v1/hazards/fire?view=full", "/api/v1/hazards/fire?view=summary"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		if response.Code != 200 {
			t.Fatal("stream blocked unary request", response.Code)
		}
	}
	summary := &stopStreamWriter{httptest.NewRecorder()}
	handler.ServeHTTP(summary, httptest.NewRequest("GET", "/api/v1/feeds/events?view=summary", nil))
	if summary.Code != 200 || feed.calls.Load() != 2 {
		t.Fatal("FULL stream blocked SUMMARY")
	}
	close(writer.release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("failed HTTP output did not terminate workers")
	}
	retry := &stopStreamWriter{httptest.NewRecorder()}
	handler.ServeHTTP(retry, httptest.NewRequest("GET", "/api/v1/feeds/seismic?view=full", nil))
	if retry.Code != 200 || feed.calls.Load() != 3 {
		t.Fatal("closed stream leaked admission", retry.Code)
	}
}

func TestStreamsReserveVerifiedPriorityAndLeaveUnaryCapacity(t *testing.T) {
	handler, feed := streamHandler(t, 2)
	writer := &stalledHTTPWriter{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{}), failure: "error"}
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(writer, httptest.NewRequest("GET", "/api/v1/feeds/events", nil))
	}()
	select {
	case <-writer.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not reach HTTP output")
	}
	defer func() { close(writer.release); <-done }()
	for _, test := range []struct {
		bearer string
		want   int
	}{{"Bearer media", 429}, {"Bearer verified-field", 200}, {"Bearer media", 429}} {
		request := httptest.NewRequest("GET", "/api/v1/feeds/events", nil)
		request.Header.Set("Authorization", test.bearer)
		response := &stopStreamWriter{httptest.NewRecorder()}
		handler.ServeHTTP(response, request)
		if response.Code != test.want {
			t.Fatal("incorrect stream reserve", test.bearer, response.Code)
		}
	}
	if feed.calls.Load() != 2 {
		t.Fatal("shed streams reached the backend")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/api/v1/hazards/fire", nil))
	if response.Code != 200 {
		t.Fatal("stream exhausted unary capacity", response.Code)
	}
}

func TestStalledNetworkStreamWriteCancelsRequest(t *testing.T) {
	done := make(chan error, 1)
	server := httptest.NewServer(streamWrites(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buffer := make([]byte, 64<<10)
		for {
			if _, err := w.Write(buffer); err != nil {
				done <- r.Context().Err()
				return
			}
		}
	})))
	defer server.Close()
	connection, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := io.WriteString(connection, "GET /api/v1/feeds/events HTTP/1.1\r\nHost: local\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	// Never read the response: the real socket must eventually stop accepting writes.
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("failed write did not cancel the request")
		}
	case <-time.After(streamWriteTimeout + 4*time.Second):
		t.Fatal("stalled write kept stream workers indefinitely")
	}
}

type deadlineWriter struct {
	*httptest.ResponseRecorder
	deadline time.Time
	sets     int
}

func (w *deadlineWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadline = deadline
	w.sets++
	return nil
}

func TestStreamWriteDeadlinesAreClearedBetweenWrites(t *testing.T) {
	writer := &deadlineWriter{ResponseRecorder: httptest.NewRecorder()}
	streamWrites(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := w.Write([]byte("data: ready\n\n")); err != nil {
			t.Fatal(err)
		}
		if !writer.deadline.IsZero() || r.Context().Err() != nil {
			t.Fatal("write left an idle stream deadline/cancellation")
		}
		w.(http.Flusher).Flush()
		if !writer.deadline.IsZero() || r.Context().Err() != nil {
			t.Fatal("flush left an idle stream deadline/cancellation")
		}
	})).ServeHTTP(writer, httptest.NewRequest("GET", "/api/v1/feeds/events", nil))
	if writer.sets != 4 {
		t.Fatal("write and flush did not each set and clear their deadline", writer.sets)
	}
}
