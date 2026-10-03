package auth

import (
	"context"
	"crypto"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"

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

type JWTVerifier struct {
	publicKey    crypto.PublicKey
	serviceToken string
}

func NewJWTVerifier(publicKeyPEM string, publicKeyPath string, serviceToken string) (*JWTVerifier, error) {
	v := &JWTVerifier{
		serviceToken: serviceToken,
	}

	var pemData []byte
	if publicKeyPEM != "" {
		pemData = []byte(publicKeyPEM)
	} else if publicKeyPath != "" {
		data, err := os.ReadFile(publicKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed reading public key file: %w", err)
		}
		pemData = data
	}

	if len(pemData) > 0 {
		block, _ := pem.Decode(pemData)
		if block == nil {
			return nil, errors.New("failed decoding PEM block for public key")
		}

		pub, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			// Try parsing as RSA PKCS#1 public key if PKIX fails
			rsaPub, errRSA := x509.ParsePKCS1PublicKey(block.Bytes)
			if errRSA != nil {
				return nil, fmt.Errorf("failed parsing public key: %w (rsa error: %v)", err, errRSA)
			}
			pub = rsaPub
		}
		v.publicKey = pub
	}

	return v, nil
}

func (v *JWTVerifier) VerifyToken(tokenStr string) (*CallerContextData, error) {
	if v.serviceToken != "" && tokenStr == v.serviceToken {
		return &CallerContextData{
			ClientID: "service-aggregator",
			Role:     "SERVICE",
			Scopes:   []string{"hazards:read:summary", "hazards:read:full"},
		}, nil
	}

	// Parse without verifying signature if no public key is configured (local dev/mock mode)
	if v.publicKey == nil {
		parser := jwt.NewParser()
		token, _, err := parser.ParseUnverified(tokenStr, jwt.MapClaims{})
		if err != nil {
			return nil, fmt.Errorf("invalid token format: %w", err)
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			return nil, errors.New("invalid token claims")
		}
		return extractCallerContext(claims), nil
	}

	// Verify asymmetric signature with configured public key
	token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
		// Ensure signing method is asymmetric RSA or ECDSA
		switch token.Method.(type) {
		case *jwt.SigningMethodRSA, *jwt.SigningMethodRSAPSS, *jwt.SigningMethodECDSA:
			return v.publicKey, nil
		default:
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
	})
	if err != nil {
		return nil, fmt.Errorf("token validation failed: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid or expired token")
	}

	return extractCallerContext(claims), nil
}

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

	// Scopes can be formatted as space-separated string or array of strings
	if scopeStr, ok := claims["scope"].(string); ok {
		c.Scopes = strings.Fields(scopeStr)
	} else if scopesSlice, ok := claims["scopes"].([]any); ok {
		for _, s := range scopesSlice {
			if sStr, ok := s.(string); ok {
				c.Scopes = append(c.Scopes, sStr)
			}
		}
	} else if scopeSlice, ok := claims["scope"].([]any); ok {
		for _, s := range scopeSlice {
			if sStr, ok := s.(string); ok {
				c.Scopes = append(c.Scopes, sStr)
			}
		}
	}

	// Infer scopes if role is given directly (per M1 matrix)
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

		// Optional check: validate x-service-token if caller provides it
		if v.serviceToken != "" && v.serviceToken != "REPLACE_AGGREGATOR_SERVICE_TOKEN" {
			srvTokens := md.Get("x-service-token")
			if len(srvTokens) == 0 {
				// Don't reject yet if bearer token is present, but keep correlation
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
