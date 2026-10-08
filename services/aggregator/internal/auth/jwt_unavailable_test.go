package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// gatedIssuer fronts the test JWKS server and answers 503 until opened, like Envoy or
// Zitadel not being up yet when the aggregator starts.
func gatedIssuer(t *testing.T, srv *testJWKSServer) (*httptest.Server, *atomic.Bool) {
	t.Helper()
	var open atomic.Bool
	gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !open.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		srv.server.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(gate.Close)
	return gate, &open
}

func TestNewJWTVerifier_StartsWhileIssuerDown(t *testing.T) {
	srv := newTestJWKSServer(t)
	defer srv.close()
	gate, open := gatedIssuer(t, srv)

	v, err := NewJWTVerifier(context.Background(), JWTVerifierConfig{AuthAuthority: gate.URL, ServiceToken: "service-token-123"})
	if err != nil {
		t.Fatalf("an unreachable issuer must not fail construction: %v", err)
	}
	t.Cleanup(v.Close)

	token := srv.signToken(t, srv.kid, srv.key, jwt.MapClaims{
		"sub": "user-1", "role": "MEDIA", "iss": gate.URL, "exp": time.Now().Add(time.Hour).Unix(),
	})
	if _, err := v.VerifyToken(token); !errors.Is(err, ErrAuthUnavailable) {
		t.Fatalf("expected ErrAuthUnavailable while the issuer is down, got %v", err)
	}

	// The interceptor reports Unavailable (retryable, 503 at the gateway), not Unauthenticated.
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	_, err = v.UnaryServerInterceptor()(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/bnpb.Test/Call"},
		func(context.Context, any) (any, error) { return "ok", nil })
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected codes.Unavailable, got %v", err)
	}

	// Service tokens do not need the issuer.
	if _, err := v.VerifyToken("service-token-123"); err != nil {
		t.Fatalf("service token must work while the issuer is down: %v", err)
	}

	// Once the issuer answers, the next request loads the keys itself.
	open.Store(true)
	caller, err := v.VerifyToken(token)
	if err != nil {
		t.Fatalf("token should verify once the issuer is up: %v", err)
	}
	if caller.ClientID != "user-1" {
		t.Errorf("expected ClientID=user-1, got %s", caller.ClientID)
	}
}

func TestNewJWTVerifier_RetriesInBackground(t *testing.T) {
	srv := newTestJWKSServer(t)
	defer srv.close()
	gate, open := gatedIssuer(t, srv)

	v, err := NewJWTVerifier(context.Background(), JWTVerifierConfig{AuthAuthority: gate.URL})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(v.Close)

	open.Store(true)
	deadline := time.Now().Add(5 * time.Second)
	for v.jwks.Load() == nil {
		if time.Now().After(deadline) {
			t.Fatal("keys were not loaded in the background after the issuer came up")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
