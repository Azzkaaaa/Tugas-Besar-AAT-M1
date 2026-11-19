package backend

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type largeRecordAggregator struct {
	aggregatorv1.UnimplementedAggregatorServiceServer
	response *aggregatorv1.GetHazardResponse
}

func TestDialStillRejectsResponsesBeyondItsFiniteBudget(t *testing.T) {
	attributes, err := structpb.NewStruct(map[string]any{"oversized": strings.Repeat("x", maxBackendResponseBytes)})
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	defer server.Stop()
	aggregatorv1.RegisterAggregatorServiceServer(server, &largeRecordAggregator{response: &aggregatorv1.GetHazardResponse{Item: &commonv1.HazardProjection{Data: &commonv1.HazardProjection_Full{Full: &commonv1.HazardEvent{HazardId: "too-large", Attributes: attributes}}}}})
	go func() { _ = server.Serve(lis) }()
	conn, err := Dial(lis.Addr().String(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = aggregatorv1.NewAggregatorServiceClient(conn).GetHazard(ctx, &aggregatorv1.GetHazardRequest{HazardId: "too-large", View: commonv1.View_VIEW_FULL})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatal("backend response budget is unbounded", err)
	}
}

func (s *largeRecordAggregator) GetHazard(context.Context, *aggregatorv1.GetHazardRequest) (*aggregatorv1.GetHazardResponse, error) {
	return s.response, nil
}

func TestDialReceivesCompleteLargeFire(t *testing.T) {
	const count = 110000
	keys := make([]any, count)
	for i := range keys {
		keys[i] = fmt.Sprintf("N20|2026-11-19|0600|%.8f|106.12345678", -6.1+float64(i)*.00000001)
	}
	attributes, err := structpb.NewStruct(map[string]any{"detection_keys": keys, "detection_count": count})
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	defer server.Stop()
	aggregatorv1.RegisterAggregatorServiceServer(server, &largeRecordAggregator{response: &aggregatorv1.GetHazardResponse{Item: &commonv1.HazardProjection{Data: &commonv1.HazardProjection_Full{Full: &commonv1.HazardEvent{HazardId: "large-fire", Attributes: attributes}}}}})
	go func() { _ = server.Serve(lis) }()
	// Exercise the real Dial options, rather than a test-only connection whose
	// receive limit differs from the deployed gateway's connection.
	conn, err := Dial(lis.Addr().String(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := aggregatorv1.NewAggregatorServiceClient(conn).GetHazard(ctx, &aggregatorv1.GetHazardRequest{HazardId: "large-fire", View: commonv1.View_VIEW_FULL})
	if err != nil {
		t.Fatal("large required detection-key list cannot pass deployed client", err)
	}
	values := got.GetItem().GetFull().GetAttributes().GetFields()["detection_keys"].GetListValue().GetValues()
	if len(values) != count || values[count-1].GetStringValue() != keys[count-1] {
		t.Fatal("large fire response lost keys", len(values))
	}
}
