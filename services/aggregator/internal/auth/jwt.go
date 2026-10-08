package auth

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type contextKey string

const (
	CallerContextKey contextKey = "caller_context"
)

type CallerContextData struct {
	ClientID string
	Role     string
	Scopes   []string
}

func (c *CallerContextData) HasScope(scope string) bool {
	for _, s := range c.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

// JWTVerifierConfig holds the configuration for creating a JWTVerifier.
type JWTVerifierConfig struct {
	// AuthAuthority is the OIDC issuer URL (e.g. http://auth.localhost:8080).
	// The JWKS URI is resolved from <authority>/.well-known/openid-configuration.
	AuthAuthority string

	// Audience is the expected aud claim. Empty means audience is not enforced.
	Audience string

	// ServiceToken is a static token for inter-service calls.
	// Empty or placeholder values are rejected at construction time.
	ServiceToken string

	// InsecureSkipVerify, when true, skips signature verification entirely.
	// This is a development-only flag; a warning is logged at startup.
	InsecureSkipVerify bool
}

type JWTVerifier struct {
	jwks               keyfunc.Keyfunc
	cancelJWKS         context.CancelFunc
	issuer             string
	audience           string
	serviceToken       string
	insecureSkipVerify bool
}

// NewJWTVerifier creates a verifier that validates tokens against
// the Zitadel JWKS endpoint resolved from the given authority.
func NewJWTVerifier(ctx context.Context, cfg JWTVerifierConfig) (*JWTVerifier, error) {
	v := &JWTVerifier{
		issuer:             cfg.AuthAuthority,
		audience:           cfg.Audience,
		insecureSkipVerify: cfg.InsecureSkipVerify,
	}

	// Validate the service token: reject empty or well-known placeholder values.
	if cfg.ServiceToken != "" {
		placeholders := []string{
			"REPLACE_AGGREGATOR_SERVICE_TOKEN",
			"REPLACE_SERVICE_TOKEN",
		}
		for _, ph := range placeholders {
			if cfg.ServiceToken == ph {
				return nil, fmt.Errorf("service token is a placeholder (%s); set a real secret", ph)
			}
		}
		v.serviceToken = cfg.ServiceToken
	}

	if cfg.InsecureSkipVerify {
		log.Println("[Auth] ⚠️  WARNING: AUTH_INSECURE_SKIP_VERIFY=true — JWT signatures are NOT verified. Do NOT use in production!")
		return v, nil
	}

	if cfg.AuthAuthority == "" {
		return nil, errors.New("AUTH_AUTHORITY is required when AUTH_INSECURE_SKIP_VERIFY is not true")
	}

	// Resolve the JWKS URI from the OIDC discovery endpoint.
	jwksURI, err := resolveJWKSURI(ctx, cfg.AuthAuthority)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve JWKS URI from %s: %w", cfg.AuthAuthority, err)
	}

	// Create a cancellable context for the background JWKS refresh goroutine.
	jwksCtx, cancel := context.WithCancel(context.Background())
	k, err := keyfunc.NewDefaultCtx(jwksCtx, []string{jwksURI})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create JWKS keyfunc from %s: %w", jwksURI, err)
	}
	v.jwks = k
	v.cancelJWKS = cancel

	return v, nil
}

// Close shuts down the background JWKS refresh goroutine.
func (v *JWTVerifier) Close() {
	if v.cancelJWKS != nil {
		v.cancelJWKS()
	}
}

// resolveJWKSURI fetches the OIDC discovery document and extracts the jwks_uri.
func resolveJWKSURI(ctx context.Context, authority string) (string, error) {
	discURL := strings.TrimRight(authority, "/") + "/.well-known/openid-configuration"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discURL, nil)
	if err != nil {
		return "", fmt.Errorf("creating request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetching %s: %w", discURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("discovery endpoint returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading response: %w", err)
	}

	var doc struct {
		JWKSURI string `json:"jwks_uri"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return "", fmt.Errorf("parsing discovery document: %w", err)
	}

	if doc.JWKSURI == "" {
		return "", errors.New("jwks_uri not found in discovery document")
	}

	return doc.JWKSURI, nil
}

// VerifyToken validates the given token string and returns the extracted caller context.
func (v *JWTVerifier) VerifyToken(tokenStr string) (*CallerContextData, error) {
	// Service-token shortcut: constant-time comparison prevents timing attacks.
	if v.serviceToken != "" && len(tokenStr) == len(v.serviceToken) {
		if subtle.ConstantTimeCompare([]byte(tokenStr), []byte(v.serviceToken)) == 1 {
			return &CallerContextData{
				ClientID: "service-aggregator",
				Role:     "SERVICE",
				Scopes:   []string{"hazards:read:summary", "hazards:read:full"},
			}, nil
		}
	}

	if v.insecureSkipVerify {
		return v.parseUnverified(tokenStr)
	}

	return v.parseVerified(tokenStr)
}

// parseUnverified parses the token without signature verification (dev-only).
func (v *JWTVerifier) parseUnverified(tokenStr string) (*CallerContextData, error) {
	parser := jwt.NewParser(
		jwt.WithExpirationRequired(),
	)
	token, _, err := parser.ParseUnverified(tokenStr, jwt.MapClaims{})
	if err != nil {
		return nil, fmt.Errorf("invalid token format: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.New("invalid token claims")
	}

	// Even in insecure mode, validate exp manually since ParseUnverified
	// doesn't actually enforce time-based claims.
	if exp, err := claims.GetExpirationTime(); err == nil && exp != nil {
		if time.Now().After(exp.Time.Add(1 * time.Minute)) {
			return nil, errors.New("token has expired")
		}
	}

	return extractCallerContext(claims), nil
}

// parseVerified parses and validates the token against the JWKS.
func (v *JWTVerifier) parseVerified(tokenStr string) (*CallerContextData, error) {
	parserOpts := []jwt.ParserOption{
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(30 * time.Second),
		jwt.WithValidMethods([]string{
			"RS256", "RS384", "RS512",
			"PS256", "PS384", "PS512",
			"ES256", "ES384", "ES512",
		}),
	}

	if v.issuer != "" {
		parserOpts = append(parserOpts, jwt.WithIssuer(v.issuer))
	}
	if v.audience != "" {
		parserOpts = append(parserOpts, jwt.WithAudience(v.audience))
	}

	token, err := jwt.Parse(tokenStr, v.jwks.KeyfuncCtx(context.Background()), parserOpts...)
	if err != nil {
		return nil, fmt.Errorf("token validation failed: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid or expired token")
	}

	return extractCallerContext(claims), nil
}

// extractCallerContext reads identity and authorization claims from the token.
//
// Scope precedence (fixes the bug where OIDC `scope: "openid profile"` shadowed
// the Zitadel Actions v2 custom `scopes` array):
//
//  1. `scopes` claim (array of strings or space-separated string) — set by
//     zitadel-claims action, always preferred when present.
//  2. `scope` claim (space-separated string or array) — standard OIDC claim,
//     used only when `scopes` is absent.
//  3. Role-based default scopes — fallback when neither claim is present.
func extractCallerContext(claims jwt.MapClaims) *CallerContextData {
	c := &CallerContextData{}

	if clientID, ok := claims["client_id"].(string); ok {
		c.ClientID = clientID
	} else if sub, ok := claims["sub"].(string); ok {
		c.ClientID = sub
	}

	if role, ok := claims["role"].(string); ok {
		c.Role = role
	}

	// 1. Prefer the custom `scopes` claim (array or space-separated string).
	if scopesSlice, ok := claims["scopes"].([]any); ok {
		for _, s := range scopesSlice {
			if sStr, ok := s.(string); ok {
				c.Scopes = append(c.Scopes, sStr)
			}
		}
	} else if scopesStr, ok := claims["scopes"].(string); ok {
		c.Scopes = strings.Fields(scopesStr)
	}

	// 2. Fall back to `scope` only if `scopes` was absent.
	if len(c.Scopes) == 0 {
		if scopeSlice, ok := claims["scope"].([]any); ok {
			for _, s := range scopeSlice {
				if sStr, ok := s.(string); ok {
					c.Scopes = append(c.Scopes, sStr)
				}
			}
		} else if scopeStr, ok := claims["scope"].(string); ok {
			c.Scopes = strings.Fields(scopeStr)
		}
	}

	// 3. Infer scopes from role if neither scope claim is present (per M1 matrix).
	if len(c.Scopes) == 0 {
		switch c.Role {
		case "MEDIA":
			c.Scopes = []string{"hazards:read:summary", "notifications:read", "webhooks:read"}
		case "FIELD_TEAM", "BNPB_INTERNAL":
			c.Scopes = []string{"hazards:read:summary", "hazards:read:full", "notifications:read", "webhooks:read", "webhooks:write"}
		}
	}

	return c
}

// UnaryServerInterceptor enforces authentication on incoming gRPC calls
func (v *JWTVerifier) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		// Allow health check calls without authentication
		if strings.HasPrefix(info.FullMethod, "/grpc.health.v1.Health") {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "missing metadata")
		}

		// Check x-service-token header first (inter-service calls).
		if v.serviceToken != "" {
			srvTokens := md.Get("x-service-token")
			if len(srvTokens) > 0 {
				tok := srvTokens[0]
				if len(tok) == len(v.serviceToken) &&
					subtle.ConstantTimeCompare([]byte(tok), []byte(v.serviceToken)) == 1 {
					svcCtx := &CallerContextData{
						ClientID: "service-aggregator",
						Role:     "SERVICE",
						Scopes:   []string{"hazards:read:summary", "hazards:read:full"},
					}
					newCtx := context.WithValue(ctx, CallerContextKey, svcCtx)
					return handler(newCtx, req)
				}
			}
		}

		// Extract authorization bearer token
		authHeaders := md.Get("authorization")
		if len(authHeaders) == 0 {
			return nil, status.Errorf(codes.Unauthenticated, "missing authorization metadata")
		}

		authHeader := authHeaders[0]
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
			return nil, status.Errorf(codes.Unauthenticated, "invalid authorization format, expected Bearer <token>")
		}

		callerCtx, err := v.VerifyToken(parts[1])
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "unauthorized token: %v", err)
		}

		newCtx := context.WithValue(ctx, CallerContextKey, callerCtx)
		return handler(newCtx, req)
	}
}

// GetCallerContext extracts CallerContextData from context
func GetCallerContext(ctx context.Context) *CallerContextData {
	if val, ok := ctx.Value(CallerContextKey).(*CallerContextData); ok {
		return val
	}
	return nil
}
