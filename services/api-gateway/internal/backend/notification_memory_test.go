package backend

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	notificationv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/notification/v1"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type notificationFixtureStats struct {
	References   int  `json:"references"`
	RootDisposed bool `json:"root_disposed"`
	BodyReads    int  `json:"body_reads"`
	Capacity     int  `json:"capacity"`
}

func TestNotificationFullRPCMemory(t *testing.T) {
	target := os.Getenv("NOTIFICATION_RPC_TARGET")
	if target == "" {
		t.Skip("requires the disposable native .NET/PostgreSQL RPC fixture")
	}
	statsURL := os.Getenv("NOTIFICATION_RPC_STATS")
	httpClient := &http.Client{Timeout: time.Second}
	defer httpClient.CloseIdleConnections()
	readStats := func() (notificationFixtureStats, error) {
		var stats notificationFixtureStats
		response, err := httpClient.Get(statsURL)
		if err != nil {
			return stats, err
		}
		defer response.Body.Close()
		err = json.NewDecoder(response.Body).Decode(&stats)
		return stats, err
	}
	waitStats := func(ready func(notificationFixtureStats) bool) notificationFixtureStats {
		until := time.Now().Add(8 * time.Second)
		for time.Now().Before(until) {
			stats, err := readStats()
			if err == nil && ready(stats) {
				return stats
			}
			time.Sleep(10 * time.Millisecond)
		}
		stats, err := readStats()
		t.Fatalf("fixture did not reach expected state: %+v %v", stats, err)
		return stats
	}
	waitStats(func(s notificationFixtureStats) bool { return s.Capacity == 1 })
	connection, err := Dial(target, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := notificationv1.NewDangerNotificationServiceClient(connection)
	ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer field")), 50*time.Second)
	defer cancel()
	var heldReferences int
	for cycle := 0; cycle < 3; cycle++ {
		before, err := readStats()
		if err != nil {
			t.Fatal(err)
		}
		raw, reset := zeroWindowNotification(t, target)
		queued := waitStats(func(s notificationFixtureStats) bool { return s.BodyReads == before.BodyReads+1 && s.References > 1 })
		heldReferences = max(heldReferences, queued.References)
		if cycle == 0 {
			for range 24 {
				_, err := client.ListNotifications(ctx, &notificationv1.ListNotificationsRequest{View: commonv1.View_VIEW_FULL})
				if status.Code(err) != codes.ResourceExhausted {
					t.Fatal("queued response released FULL admission", err)
				}
			}
			media := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer media"))
			_, err = client.GetNotification(media, &notificationv1.GetNotificationRequest{NotificationId: "event-9", View: commonv1.View_VIEW_FULL})
			if status.Code(err) != codes.PermissionDenied {
				t.Fatal("capacity hid scope rejection", err)
			}
			summary, err := client.GetNotification(media, &notificationv1.GetNotificationRequest{NotificationId: "event-9"})
			if err != nil || summary.GetItem().GetHazard().GetSummary() == nil || summary.GetItem().GetHazard().GetFull() != nil {
				t.Fatal("queued FULL blocked/leaked SUMMARY", err)
			}
			after, err := readStats()
			if err != nil || after.BodyReads != queued.BodyReads {
				t.Fatal("shed requests hydrated more bodies", after, err)
			}
		}
		reset()
		_ = raw.Close()
		waitStats(func(s notificationFixtureStats) bool { return s.Capacity == 1 && s.References == 0 })
	}
	seen := make(map[string]bool)
	cursor := ""
	for pageNumber := 0; pageNumber < 10; pageNumber++ {
		response, err := client.ListNotifications(ctx, &notificationv1.ListNotificationsRequest{View: commonv1.View_VIEW_FULL,
			Page: &commonv1.PageRequest{PageSize: 100, PageToken: cursor}})
		if err != nil {
			t.Fatal("FULL page failed after transport recovery", err)
		}
		if len(response.Items) != 1 || proto.Size(response) > 16<<20 {
			t.Fatal("page did not obey byte boundary", len(response.Items), proto.Size(response))
		}
		item := response.Items[0]
		if seen[item.NotificationId] {
			t.Fatal("continuation repeated a notification", item.NotificationId)
		}
		seen[item.NotificationId] = true
		keys := item.GetHazard().GetFull().GetAttributes().GetFields()["detection_keys"].GetListValue().GetValues()
		if len(keys) != 200000 {
			t.Fatal("FULL projection was truncated", len(keys))
		}
		for i, key := range keys {
			want := fmt.Sprintf("N20|2026-11-19|0600|%.8f|106.12345678", -6.1+float64(i)*0.00000001)
			if key.GetStringValue() != want {
				t.Fatalf("detection %d changed", i)
			}
		}
		cursor = response.NextPageToken
		if (pageNumber == 9) != (cursor == "") {
			t.Fatal("byte continuation lost a record", pageNumber, cursor)
		}
		response.Items = nil
		waitStats(func(s notificationFixtureStats) bool { return s.Capacity == 1 })
	}
	stats, err := readStats()
	if err != nil || stats.BodyReads != 13 {
		t.Fatal("lookahead or rejected calls hydrated extra bodies", stats, err)
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	peak, _ := os.ReadFile("/sys/fs/cgroup/memory.peak")
	t.Logf("NOTIFICATION_RPC pages=10 exact_keys=2000000 shed_full=24 reset_cycles=3 held_pool_references=%d body_reads=%d heap_bytes=%d container_peak_bytes=%s",
		heldReferences, stats.BodyReads, memory.HeapAlloc, strings.TrimSpace(string(peak)))
}

func zeroWindowNotification(t *testing.T, target string) (net.Conn, func()) {
	t.Helper()
	connection, err := net.Dial("tcp", target)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	framer := http2.NewFramer(connection, connection)
	var writes sync.Mutex
	if _, err := connection.Write([]byte(http2.ClientPreface)); err != nil {
		t.Fatal(err)
	}
	if err := framer.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 0}); err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			frame, err := framer.ReadFrame()
			if err != nil {
				return
			}
			writes.Lock()
			switch frame := frame.(type) {
			case *http2.SettingsFrame:
				if !frame.IsAck() {
					_ = framer.WriteSettingsAck()
				}
			case *http2.PingFrame:
				if !frame.Flags.Has(http2.FlagPingAck) {
					_ = framer.WritePing(true, frame.Data)
				}
			}
			writes.Unlock()
		}
	}()
	var headers bytes.Buffer
	encoder := hpack.NewEncoder(&headers)
	for _, field := range []hpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "http"},
		{Name: ":authority", Value: target}, {Name: ":path", Value: "/bnpb.notification.v1.DangerNotificationService/GetNotification"},
		{Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"}, {Name: "authorization", Value: "Bearer field"}} {
		if err := encoder.WriteField(field); err != nil {
			t.Fatal(err)
		}
	}
	payload, err := proto.Marshal(&notificationv1.GetNotificationRequest{NotificationId: "event-9", View: commonv1.View_VIEW_FULL})
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, len(payload)+5)
	binary.BigEndian.PutUint32(data[1:5], uint32(len(payload)))
	copy(data[5:], payload)
	writes.Lock()
	err = framer.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: headers.Bytes(), EndHeaders: true})
	if err == nil {
		err = framer.WriteData(1, true, data)
	}
	writes.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	return connection, func() {
		writes.Lock()
		defer writes.Unlock()
		_ = framer.WriteRSTStream(1, http2.ErrCodeCancel)
	}
}
