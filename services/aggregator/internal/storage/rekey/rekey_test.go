package rekey

import (
	"testing"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	eventv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/event/v1"
	"google.golang.org/protobuf/proto"
)

func TestPayload(t *testing.T) {
	in, _ := proto.Marshal(&eventv1.HazardEventPublished{EventId: "pub-1", SchemaVersion: 1,
		Payload: &commonv1.HazardEvent{HazardId: "hz-old", SourceRefId: "eq-1"}})
	out, err := Payload(in, "hz-new")
	if err != nil {
		t.Fatal(err)
	}
	var ev eventv1.HazardEventPublished
	if err := proto.Unmarshal(out, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.EventId != "pub-1" || ev.Payload.HazardId != "hz-new" || ev.Payload.SourceRefId != "eq-1" {
		t.Fatalf("rewritten = %+v", &ev)
	}
	if _, err := Payload([]byte("garbage"), "hz"); err == nil {
		t.Fatal("garbage payload accepted")
	}
}
