package auth

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestJWTVerifier_UnverifiedMode(t *testing.T) {
	// Create token with claims
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"client_id": "media-client-1",
		"role":      "MEDIA",
		"scope":     "hazards:read:summary",
		"exp":       time.Now().Add(time.Hour).Unix(),
	})
	tokenStr, err := token.SignedString([]byte("dummy-secret"))
	if err != nil {
		t.Fatalf("failed signing token: %v", err)
	}

	verifier, err := NewJWTVerifier("", "", "service-token-secret")
	if err != nil {
		t.Fatalf("failed creating verifier: %v", err)
	}

	caller, err := verifier.VerifyToken(tokenStr)
	if err != nil {
		t.Fatalf("failed verifying token in dev mode: %v", err)
	}

	if caller.ClientID != "media-client-1" {
		t.Errorf("expected client_id media-client-1, got %s", caller.ClientID)
	}
	if caller.Role != "MEDIA" {
		t.Errorf("expected role MEDIA, got %s", caller.Role)
	}
	if !caller.HasScope("hazards:read:summary") {
		t.Errorf("expected scope hazards:read:summary")
	}
	if caller.HasScope("hazards:read:full") {
		t.Errorf("media client should not have hazards:read:full scope")
	}
}

func TestJWTVerifier_ServiceToken(t *testing.T) {
	verifier, err := NewJWTVerifier("", "", "super-secret-service-token")
	if err != nil {
		t.Fatalf("failed creating verifier: %v", err)
	}

	caller, err := verifier.VerifyToken("super-secret-service-token")
	if err != nil {
		t.Fatalf("failed verifying service token: %v", err)
	}

	if caller.Role != "SERVICE" {
		t.Errorf("expected role SERVICE, got %s", caller.Role)
	}
	if !caller.HasScope("hazards:read:full") {
		t.Errorf("service token should have full read scope")
	}
}
