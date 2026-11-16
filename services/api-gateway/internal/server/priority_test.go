package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/problem"
)

func TestReservedCapacitySurvivesMediaSaturation(t *testing.T) {
	field := httptest.NewRequest("GET", "/api/v1/hazards", nil)
	media := httptest.NewRequest("GET", "/api/v1/hazards", nil)
	// The verifier's signature/role checks are independently covered in priority.
	g := &gateway{inFlight: make(chan struct{}, 1), reserved: make(chan struct{}, 1), priorityAllowed: func(r *http.Request) bool { return r == field }}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	block := g.limit(func(context.Context, http.ResponseWriter, *http.Request, any) (any, error) {
		close(entered)
		<-release
		return nil, nil
	}, "ListHazards")
	go func() { _, err := block(context.Background(), httptest.NewRecorder(), media, nil); done <- err }()
	<-entered
	defer func() {
		close(release)
		if err := <-done; err != nil {
			t.Error(err)
		}
		if len(g.inFlight) != 0 {
			t.Error("common permit leaked")
		}
	}()
	called := 0
	quick := g.limit(func(context.Context, http.ResponseWriter, *http.Request, any) (any, error) { called++; return nil, nil }, "ListHazards")
	_, err := quick(context.Background(), httptest.NewRecorder(), media, nil)
	var p *problem.Error
	if !errors.As(err, &p) || p.Status != 429 || called != 0 {
		t.Fatalf("media overload: %v, calls=%d", err, called)
	}
	for range 2 {
		if _, err := quick(context.Background(), httptest.NewRecorder(), field, nil); err != nil {
			t.Fatal(err)
		}
	}
	if called != 2 || len(g.reserved) != 0 {
		t.Fatal("field capacity not available or permit leaked")
	}
}
