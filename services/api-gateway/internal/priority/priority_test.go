package priority

import (
	"crypto/rand"
	"crypto/rsa"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestReservedCapacityRequiresVerifiedRole(t *testing.T) {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	v := &Verifier{issuer: "https://issuer.test", audience: "bnpb"}
	keys := map[string]*rsa.PublicKey{"test": &k.PublicKey}
	v.keys.Store(&keys)
	for _, tc := range []struct {
		role    string
		expired bool
		want    bool
	}{{"FIELD_TEAM", false, true}, {"BNPB_INTERNAL", false, true}, {"MEDIA", false, false}, {"FIELD_TEAM", true, false}} {
		exp := time.Now().Add(time.Minute)
		if tc.expired {
			exp = time.Now().Add(-time.Minute)
		}
		token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"iss": v.issuer, "aud": "bnpb", "exp": exp.Unix(), "role": tc.role})
		token.Header["kid"] = "test"
		raw, err := token.SignedString(k)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer "+raw)
		if got := v.Allowed(r); got != tc.want {
			t.Fatalf("%+v: %v", tc, got)
		}
		r.Header.Set("Authorization", "Bearer "+raw+"tampered")
		if v.Allowed(r) {
			t.Fatal("accepted forged token")
		}
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-Role", "FIELD_TEAM")
	if v.Allowed(r) {
		t.Fatal("trusted spoofed header")
	}
}
