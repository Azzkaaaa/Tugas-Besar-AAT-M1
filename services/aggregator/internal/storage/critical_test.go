package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

func TestCriticalEventPreservesAnImmutableFullRevision(t *testing.T) {
	h := &domain.HazardRecord{HazardID: "hazard", Source: domain.SourceCoastal, SourceRefID: "observation", Revision: 2,
		Attributes: map[string]any{"critical": true, "sea_level_anomaly_m": 2.4}}
	e, err := NewCriticalEvent(h, map[string]string{"traceparent": "parent"})
	if err != nil || e == nil {
		t.Fatal(e, err)
	}
	var body struct {
		EventID string              `json:"event_id"`
		Data    domain.HazardRecord `json:"data"`
	}
	if err := json.Unmarshal(e.Payload, &body); err != nil {
		t.Fatal(err)
	}
	if body.EventID != domain.EventID("hazard", 2) || body.Data.SourceRefID != "observation" || body.Data.Attributes["critical"] != true {
		t.Fatal("notification identity/content lost", body)
	}
	hash := sha256.Sum256(e.Payload)
	if e.PayloadSHA256 != hex.EncodeToString(hash[:]) || e.TraceContext["traceparent"] != "parent" {
		t.Fatal("invalid immutable payload hash/trace", e)
	}
	h.Revision++
	h.Attributes["critical"] = false
	if string(e.Payload) == "" || body.Data.Revision != 2 {
		t.Fatal("published revision mutated")
	}
	if next, err := NewCriticalEvent(h, nil); err != nil || next != nil {
		t.Fatal("noncritical revision was enrolled", next, err)
	}
	h.Source = domain.SourceBMKG
	h.Attributes["critical"] = true
	if next, err := NewCriticalEvent(h, nil); err != nil || next != nil {
		t.Fatal("foreign source enrolled", next, err)
	}
}
