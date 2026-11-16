// Package priority verifies role claims locally before granting reserved capacity.
// Caller-supplied role headers and unsigned JWT payloads never grant priority.
package priority

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Verifier struct {
	issuer, audience string
	keys             atomic.Pointer[map[string]*rsa.PublicKey]
}

func New(ctx context.Context, issuer, audience string) *Verifier {
	v := &Verifier{issuer: issuer, audience: audience}
	if issuer == "" {
		return v
	}
	go func() {
		for ctx.Err() == nil {
			delay := time.Minute
			if v.refresh(ctx) != nil {
				delay = 5 * time.Second
			}
			t := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
	}()
	return v
}
func (v *Verifier) Allowed(r *http.Request) bool {
	keys := v.keys.Load()
	if keys == nil || v.issuer == "" {
		return false
	}
	raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || len(raw) > 16384 {
		return false
	}
	opts := []jwt.ParserOption{jwt.WithValidMethods([]string{"RS256"}), jwt.WithIssuer(v.issuer), jwt.WithExpirationRequired()}
	if v.audience != "" {
		opts = append(opts, jwt.WithAudience(v.audience))
	}
	t, err := jwt.Parse(raw, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		k := (*keys)[kid]
		if k == nil {
			return nil, errors.New("unknown signing key")
		}
		return k, nil
	}, opts...)
	if err != nil || !t.Valid {
		return false
	}
	claims, ok := t.Claims.(jwt.MapClaims)
	if !ok {
		return false
	}
	role, _ := claims["role"].(string)
	return role == "FIELD_TEAM" || role == "BNPB_INTERNAL"
}
func (v *Verifier) refresh(ctx context.Context) error {
	c := &http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	read := func(uri string, dst any) error {
		q, err := http.NewRequestWithContext(ctx, "GET", uri, nil)
		if err != nil {
			return err
		}
		resp, err := c.Do(q)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return errors.New("issuer unavailable")
		}
		b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if err != nil {
			return err
		}
		return json.Unmarshal(b, dst)
	}
	var discovery struct {
		Issuer string `json:"issuer"`
		JWKS   string `json:"jwks_uri"`
	}
	if err := read(strings.TrimRight(v.issuer, "/")+"/.well-known/openid-configuration", &discovery); err != nil {
		return err
	}
	if discovery.Issuer != v.issuer {
		return errors.New("issuer mismatch")
	}
	var doc struct {
		Keys []struct{ Kid, Kty, N, E, Use, Alg string } `json:"keys"`
	}
	if err := read(discovery.JWKS, &doc); err != nil {
		return err
	}
	keys := map[string]*rsa.PublicKey{}
	for _, k := range doc.Keys {
		if k.Kty != "RSA" || (k.Use != "" && k.Use != "sig") || (k.Alg != "" && k.Alg != "RS256") {
			continue
		}
		n, err := base64.RawURLEncoding.DecodeString(k.N)
		if err != nil {
			continue
		}
		e, err := base64.RawURLEncoding.DecodeString(k.E)
		if err != nil || len(e) > 4 {
			continue
		}
		exponent := new(big.Int).SetBytes(e).Int64()
		if exponent < 3 || len(n) < 256 {
			continue
		}
		keys[k.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(exponent)}
	}
	if len(keys) == 0 {
		return errors.New("no RS256 keys")
	}
	v.keys.Store(&keys)
	return nil
}
