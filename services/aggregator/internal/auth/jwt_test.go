package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// --------------------------------------------------------------------------
// Test helpers: local OIDC discovery + JWKS server
// --------------------------------------------------------------------------

type testJWKSServer struct {
	server     *httptest.Server
	key        *rsa.PrivateKey
	kid        string
	extraKeys  map[string]*rsa.PrivateKey // for rotation tests
}

func newTestJWKSServer(t *testing.T) *testJWKSServer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate RSA key: %v", err)
	}

	s := &testJWKSServer{
		key:       key,
		kid:       "test-kid-1",
		extraKeys: make(map[string]*rsa.PrivateKey),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		// Determine server URL dynamically.
		base := "http://" + r.Host
		disc := map[string]any{
			"issuer":   base,
			"jwks_uri": base + "/.well-known/jwks.json",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(disc)
	})
	mux.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, r *http.Request) {
		keys := []map[string]any{
			rsaPublicKeyToJWK(s.key, s.kid),
		}
		for kid, k := range s.extraKeys {
			keys = append(keys, rsaPublicKeyToJWK(k, kid))
		}
		jwks := map[string]any{"keys": keys}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jwks)
	})

	s.server = httptest.NewServer(mux)
	return s
}

func (s *testJWKSServer) close() {
	s.server.Close()
}

func (s *testJWKSServer) url() string {
	return s.server.URL
}

// addRotatedKey generates a new RSA key and adds it to the JWKS under the given kid.
// Returns the private key for signing.
func (s *testJWKSServer) addRotatedKey(t *testing.T, kid string) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate rotated RSA key: %v", err)
	}
	s.extraKeys[kid] = key
	return key
}

func (s *testJWKSServer) signToken(t *testing.T, kid string, key *rsa.PrivateKey, claims jwt.MapClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = kid
	tokenStr, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}
	return tokenStr
}

// rsaPublicKeyToJWK converts an RSA private key's public part to a JWK map.
func rsaPublicKeyToJWK(key *rsa.PrivateKey, kid string) map[string]any {
	return map[string]any{
		"kty": "RSA",
		"kid": kid,
		"use": "sig",
		"alg": "RS256",
		"n":   base64urlUint(key.N),
		"e":   base64urlUint(big.NewInt(int64(key.E))),
	}
}

func base64urlUint(n *big.Int) string {
	b := n.Bytes()
	// base64url encode without padding
	const encodeURL = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	buf := make([]byte, 0, (len(b)*8+5)/6)
	for i := 0; i < len(b); {
		var val uint32
		remaining := len(b) - i
		switch {
		case remaining >= 3:
			val = uint32(b[i])<<16 | uint32(b[i+1])<<8 | uint32(b[i+2])
			buf = append(buf, encodeURL[val>>18], encodeURL[(val>>12)&0x3f], encodeURL[(val>>6)&0x3f], encodeURL[val&0x3f])
			i += 3
		case remaining == 2:
			val = uint32(b[i])<<16 | uint32(b[i+1])<<8
			buf = append(buf, encodeURL[val>>18], encodeURL[(val>>12)&0x3f], encodeURL[(val>>6)&0x3f])
			i += 2
		case remaining == 1:
			val = uint32(b[i]) << 16
			buf = append(buf, encodeURL[val>>18], encodeURL[(val>>12)&0x3f])
			i += 1
		}
	}
	return string(buf)
}

// --------------------------------------------------------------------------
// Helper to create a verifier pointed at the test server
// --------------------------------------------------------------------------

func newTestVerifier(t *testing.T, srv *testJWKSServer, serviceToken string) *JWTVerifier {
	t.Helper()
	v, err := NewJWTVerifier(context.Background(), JWTVerifierConfig{
		AuthAuthority:      srv.url(),
		ServiceToken:       serviceToken,
		InsecureSkipVerify: false,
	})
	if err != nil {
		t.Fatalf("failed creating verifier: %v", err)
	}
	t.Cleanup(v.Close)
	return v
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

func TestVerifyToken_ValidWithScopesArray(t *testing.T) {
	srv := newTestJWKSServer(t)
	defer srv.close()

	v := newTestVerifier(t, srv, "real-service-token-12345")

	claims := jwt.MapClaims{
		"sub":    "user-123",
		"role":   "FIELD_TEAM",
		"scopes": []string{"hazards:read:summary", "hazards:read:full"},
		"iss":    srv.url(),
		"exp":    time.Now().Add(time.Hour).Unix(),
		"nbf":    time.Now().Add(-time.Minute).Unix(),
	}
	tokenStr := srv.signToken(t, srv.kid, srv.key, claims)

	caller, err := v.VerifyToken(tokenStr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if caller.ClientID != "user-123" {
		t.Errorf("expected ClientID=user-123, got %s", caller.ClientID)
	}
	if caller.Role != "FIELD_TEAM" {
		t.Errorf("expected Role=FIELD_TEAM, got %s", caller.Role)
	}
	if !caller.HasScope("hazards:read:full") {
		t.Error("expected hazards:read:full scope")
	}
	if !caller.HasScope("hazards:read:summary") {
		t.Error("expected hazards:read:summary scope")
	}
}

func TestVerifyToken_ScopesWinsOverScope(t *testing.T) {
	srv := newTestJWKSServer(t)
	defer srv.close()

	v := newTestVerifier(t, srv, "real-service-token-12345")

	claims := jwt.MapClaims{
		"sub":    "user-456",
		"role":   "BNPB_INTERNAL",
		"scope":  "openid profile",
		"scopes": []string{"hazards:read:summary", "hazards:read:full", "notifications:read"},
		"iss":    srv.url(),
		"exp":    time.Now().Add(time.Hour).Unix(),
	}
	tokenStr := srv.signToken(t, srv.kid, srv.key, claims)

	caller, err := v.VerifyToken(tokenStr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Custom scopes should win over the OIDC scope claim.
	if caller.HasScope("openid") {
		t.Error("OIDC 'scope' should NOT be used when 'scopes' is present")
	}
	if !caller.HasScope("hazards:read:full") {
		t.Error("expected custom scopes to include hazards:read:full")
	}
	if !caller.HasScope("notifications:read") {
		t.Error("expected custom scopes to include notifications:read")
	}
}

func TestVerifyToken_ExpiredToken(t *testing.T) {
	srv := newTestJWKSServer(t)
	defer srv.close()

	v := newTestVerifier(t, srv, "real-service-token-12345")

	claims := jwt.MapClaims{
		"sub": "user-789",
		"iss": srv.url(),
		"exp": time.Now().Add(-time.Hour).Unix(), // expired 1 hour ago
	}
	tokenStr := srv.signToken(t, srv.kid, srv.key, claims)

	_, err := v.VerifyToken(tokenStr)
	if err == nil {
		t.Fatal("expected error for expired token, got nil")
	}
	t.Logf("correctly rejected expired token: %v", err)
}

func TestVerifyToken_WrongIssuer(t *testing.T) {
	srv := newTestJWKSServer(t)
	defer srv.close()

	v := newTestVerifier(t, srv, "real-service-token-12345")

	claims := jwt.MapClaims{
		"sub": "user-wrong-iss",
		"iss": "https://evil.example.com",
		"exp": time.Now().Add(time.Hour).Unix(),
	}
	tokenStr := srv.signToken(t, srv.kid, srv.key, claims)

	_, err := v.VerifyToken(tokenStr)
	if err == nil {
		t.Fatal("expected error for wrong issuer, got nil")
	}
	t.Logf("correctly rejected wrong issuer: %v", err)
}

func TestVerifyToken_HS256Rejected(t *testing.T) {
	srv := newTestJWKSServer(t)
	defer srv.close()

	v := newTestVerifier(t, srv, "real-service-token-12345")

	// Sign with HMAC (symmetric) which should be rejected.
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "attacker",
		"iss": srv.url(),
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	tokenStr, err := token.SignedString([]byte("some-hmac-secret"))
	if err != nil {
		t.Fatalf("failed signing HS256 token: %v", err)
	}

	_, err = v.VerifyToken(tokenStr)
	if err == nil {
		t.Fatal("expected error for HS256 token, got nil")
	}
	t.Logf("correctly rejected HS256 token: %v", err)
}

func TestVerifyToken_KeyRotation(t *testing.T) {
	srv := newTestJWKSServer(t)
	defer srv.close()

	v := newTestVerifier(t, srv, "real-service-token-12345")

	// First, verify a token with the original key works.
	claims1 := jwt.MapClaims{
		"sub":    "user-rotation-1",
		"role":   "MEDIA",
		"scopes": []string{"hazards:read:summary"},
		"iss":    srv.url(),
		"exp":    time.Now().Add(time.Hour).Unix(),
	}
	tokenStr1 := srv.signToken(t, srv.kid, srv.key, claims1)
	_, err := v.VerifyToken(tokenStr1)
	if err != nil {
		t.Fatalf("original key token should be valid: %v", err)
	}

	// Add a rotated key to the JWKS server.
	newKid := "rotated-kid-2"
	newKey := srv.addRotatedKey(t, newKid)

	// Sign a new token with the rotated key.
	claims2 := jwt.MapClaims{
		"sub":    "user-rotation-2",
		"role":   "BNPB_INTERNAL",
		"scopes": []string{"hazards:read:full"},
		"iss":    srv.url(),
		"exp":    time.Now().Add(time.Hour).Unix(),
	}
	tokenStr2 := srv.signToken(t, newKid, newKey, claims2)

	// The verifier should refresh its JWKS and accept the new kid.
	caller, err := v.VerifyToken(tokenStr2)
	if err != nil {
		t.Fatalf("rotated key token should be accepted after JWKS refresh: %v", err)
	}
	if caller.ClientID != "user-rotation-2" {
		t.Errorf("expected ClientID=user-rotation-2, got %s", caller.ClientID)
	}
}

func TestVerifyToken_ServiceToken_Valid(t *testing.T) {
	srv := newTestJWKSServer(t)
	defer srv.close()

	secret := "my-super-secret-service-token"
	v := newTestVerifier(t, srv, secret)

	caller, err := v.VerifyToken(secret)
	if err != nil {
		t.Fatalf("service token should be accepted: %v", err)
	}
	if caller.Role != "SERVICE" {
		t.Errorf("expected role SERVICE, got %s", caller.Role)
	}
	if !caller.HasScope("hazards:read:full") {
		t.Error("service token should have hazards:read:full scope")
	}
	if !caller.HasScope("hazards:read:summary") {
		t.Error("service token should have hazards:read:summary scope")
	}
}

func TestVerifyToken_ServiceToken_Rejected(t *testing.T) {
	srv := newTestJWKSServer(t)
	defer srv.close()

	v := newTestVerifier(t, srv, "correct-token")

	// Wrong token should not match.
	_, err := v.VerifyToken("wrong-token-value")
	if err == nil {
		t.Fatal("wrong service token should be rejected")
	}
	t.Logf("correctly rejected wrong service token: %v", err)
}

func TestNewJWTVerifier_RejectsPlaceholderServiceToken(t *testing.T) {
	srv := newTestJWKSServer(t)
	defer srv.close()

	_, err := NewJWTVerifier(context.Background(), JWTVerifierConfig{
		AuthAuthority: srv.url(),
		ServiceToken:  "REPLACE_AGGREGATOR_SERVICE_TOKEN",
	})
	if err == nil {
		t.Fatal("expected error for placeholder service token")
	}
	t.Logf("correctly rejected placeholder: %v", err)
}

func TestNewJWTVerifier_InsecureSkipVerify(t *testing.T) {
	// Should succeed without any authority URL when insecure mode is on.
	v, err := NewJWTVerifier(context.Background(), JWTVerifierConfig{
		InsecureSkipVerify: true,
		ServiceToken:       "dev-token",
	})
	if err != nil {
		t.Fatalf("insecure verifier creation failed: %v", err)
	}
	defer v.Close()

	// Sign a token with HS256 (any key) — insecure mode should accept it.
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":  "dev-user",
		"role": "MEDIA",
		"exp":  time.Now().Add(time.Hour).Unix(),
	})
	tokenStr, err := token.SignedString([]byte("any-dev-secret"))
	if err != nil {
		t.Fatalf("failed signing dev token: %v", err)
	}

	caller, err := v.VerifyToken(tokenStr)
	if err != nil {
		t.Fatalf("insecure mode should accept any token: %v", err)
	}
	if caller.Role != "MEDIA" {
		t.Errorf("expected role MEDIA, got %s", caller.Role)
	}
}

func TestVerifyToken_InsecureMode_RejectsExpired(t *testing.T) {
	v, err := NewJWTVerifier(context.Background(), JWTVerifierConfig{
		InsecureSkipVerify: true,
		ServiceToken:       "dev-token",
	})
	if err != nil {
		t.Fatalf("verifier creation failed: %v", err)
	}
	defer v.Close()

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "dev-user",
		"exp": time.Now().Add(-2 * time.Hour).Unix(), // expired well beyond leeway
	})
	tokenStr, err := token.SignedString([]byte("any-dev-secret"))
	if err != nil {
		t.Fatalf("failed signing token: %v", err)
	}

	_, err = v.VerifyToken(tokenStr)
	if err == nil {
		t.Fatal("insecure mode should still reject expired tokens")
	}
	t.Logf("correctly rejected expired token in insecure mode: %v", err)
}

func TestExtractCallerContext_RoleFallback(t *testing.T) {
	tests := []struct {
		role           string
		expectedScopes []string
	}{
		{"MEDIA", []string{"hazards:read:summary", "notifications:read", "webhooks:read"}},
		{"FIELD_TEAM", []string{"hazards:read:summary", "hazards:read:full", "notifications:read", "webhooks:read", "webhooks:write"}},
		{"BNPB_INTERNAL", []string{"hazards:read:summary", "hazards:read:full", "notifications:read", "webhooks:read", "webhooks:write"}},
		{"UNKNOWN", nil},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("role=%s", tt.role), func(t *testing.T) {
			claims := jwt.MapClaims{
				"sub":  "user-role-test",
				"role": tt.role,
			}
			ctx := extractCallerContext(claims)
			if len(tt.expectedScopes) == 0 {
				if len(ctx.Scopes) != 0 {
					t.Errorf("expected no scopes for role %s, got %v", tt.role, ctx.Scopes)
				}
				return
			}
			for _, s := range tt.expectedScopes {
				if !ctx.HasScope(s) {
					t.Errorf("role %s: expected scope %s", tt.role, s)
				}
			}
		})
	}
}

func TestNewJWTVerifier_RequiresAuthority(t *testing.T) {
	_, err := NewJWTVerifier(context.Background(), JWTVerifierConfig{
		// No authority, no insecure skip.
	})
	if err == nil {
		t.Fatal("expected error when AUTH_AUTHORITY is empty and insecure skip is false")
	}
	t.Logf("correctly required authority: %v", err)
}
