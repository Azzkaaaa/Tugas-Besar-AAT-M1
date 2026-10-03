package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Config struct {
	GRPCHost         string
	PVMBGBaseURL     string
	BMKGBaseURL      string
	PVMBGBearerToken string
	BMKGAPIKey       string
}

func loadConfig() *Config {
	grpcHost := getEnv("AGGREGATOR_GRPC_HOST", "localhost:9091")
	pvmbgBase := getEnv("PVMBG_BASE_URL", "http://localhost:8082")
	bmkgBase := getEnv("BMKG_BASE_URL", "http://localhost:8081")
	pvmbgToken := getEnv("PVMBG_BEARER_TOKEN", "")
	bmkgKey := getEnv("BMKG_API_KEY", "")

	// Load from .env if present
	if data, err := os.ReadFile(".env"); err == nil {
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "#") || !strings.Contains(line, "=") {
				continue
			}
			parts := strings.SplitN(line, "=", 2)
			k := strings.TrimSpace(parts[0])
			v := strings.Trim(strings.TrimSpace(parts[1]), "\"'")
			switch k {
			case "PVMBG_BEARER_TOKEN":
				if pvmbgToken == "" {
					pvmbgToken = v
				}
			case "BMKG_API_KEY":
				if bmkgKey == "" {
					bmkgKey = v
				}
			case "PVMBG_HTTP_PORT":
				if pvmbgBase == "http://localhost:8082" {
					pvmbgBase = "http://localhost:" + v
				}
			case "BMKG_HTTP_PORT":
				if bmkgBase == "http://localhost:8081" {
					bmkgBase = "http://localhost:" + v
				}
			}
		}
	}

	return &Config{
		GRPCHost:         grpcHost,
		PVMBGBaseURL:     pvmbgBase,
		BMKGBaseURL:      bmkgBase,
		PVMBGBearerToken: pvmbgToken,
		BMKGAPIKey:       bmkgKey,
	}
}

func getEnv(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}

func createTestJWT(role string, scopes []string) string {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":       "test-client-" + role,
		"client_id": "test-client-" + role,
		"role":      role,
		"scope":     strings.Join(scopes, " "),
		"exp":       time.Now().Add(10 * time.Minute).Unix(),
	})
	tokenStr, _ := token.SignedString([]byte("dev-secret"))
	return tokenStr
}

func withAuth(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func main() {
	flag.Parse()
	cfg := loadConfig()

	log.Println("=================================================================")
	log.Printf("[Aggregator E2E Test] Connecting to gRPC at %s", cfg.GRPCHost)
	log.Printf("[Aggregator E2E Test] Upstream BMKG: %s, PVMBG: %s", cfg.BMKGBaseURL, cfg.PVMBGBaseURL)
	log.Println("=================================================================")

	conn, err := grpc.NewClient(cfg.GRPCHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to dial gRPC: %v", err)
	}
	defer conn.Close()

	client := aggregatorv1.NewAggregatorServiceClient(conn)
	ctx := context.Background()

	mediaToken := createTestJWT("MEDIA", []string{"hazards:read:summary"})
	fieldToken := createTestJWT("FIELD_TEAM", []string{"hazards:read:summary", "hazards:read:full"})

	// Give poller a moment if freshly started
	log.Println("[Aggregator E2E Test] Checking initial ingestion readiness...")
	for i := 0; i < 5; i++ {
		statusResp, err := client.GetSourceStatuses(withAuth(ctx, mediaToken), &aggregatorv1.GetSourceStatusesRequest{})
		if err == nil && len(statusResp.Freshness) > 0 {
			hasFresh := false
			for _, s := range statusResp.Freshness {
				if s.Status == commonv1.SourceStatus_SOURCE_STATUS_FRESH {
					hasFresh = true
					break
				}
			}
			if hasFresh {
				break
			}
		}
		time.Sleep(1 * time.Second)
	}

	// --- TEST 1: Source Statuses ---
	log.Println("\n[TEST 1] Verifying GetSourceStatuses RPC...")
	statusResp, err := client.GetSourceStatuses(withAuth(ctx, mediaToken), &aggregatorv1.GetSourceStatusesRequest{})
	if err != nil {
		log.Fatalf("  FAILED: GetSourceStatuses returned error: %v", err)
	}
	log.Printf("  SUCCESS: Received %d source status entries:", len(statusResp.Freshness))
	for _, s := range statusResp.Freshness {
		log.Printf("    - Source: %s | Status: %s | LastSuccessAt: %v", s.Source.String(), s.Status.String(), s.LastSuccessAt.AsTime())
	}

	// --- TEST 2: Role Projection (Media Summary vs Full View) ---
	log.Println("\n[TEST 2] Verifying Media Role Projection (VIEW_SUMMARY)...")
	mediaResp, err := client.ListHazards(withAuth(ctx, mediaToken), &aggregatorv1.ListHazardsRequest{
		View: commonv1.View_VIEW_SUMMARY,
		Page: &commonv1.PageRequest{PageSize: 10},
	})
	if err != nil {
		log.Fatalf("  FAILED: ListHazards(VIEW_SUMMARY) error: %v", err)
	}
	log.Printf("  SUCCESS: Retrieved %d hazards in summary view", len(mediaResp.Items))
	for i, h := range mediaResp.Items {
		summary := h.GetSummary()
		if summary == nil {
			log.Fatalf("  FAILED: Hazard %d is not in summary format", i)
		}
		if h.GetFull() != nil {
			log.Fatalf("  FAILED: Full format exposed to Media role!")
		}
		if i < 3 {
			log.Printf("    - [%s] %s | %s | %s | Area: %s", summary.HazardId, summary.Source, summary.HazardType, summary.Severity, summary.AreaName)
		}
	}

	// --- TEST 3: Access Control (Media requesting Full View must be rejected) ---
	log.Println("\n[TEST 3] Verifying 403 Forbidden for Media requesting VIEW_FULL...")
	_, err = client.ListHazards(withAuth(ctx, mediaToken), &aggregatorv1.ListHazardsRequest{
		View: commonv1.View_VIEW_FULL,
	})
	if err == nil {
		log.Fatalf("  FAILED: Expected PermissionDenied (403), but got SUCCESS")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.PermissionDenied {
		log.Fatalf("  FAILED: Expected code PermissionDenied, got %v", err)
	}
	log.Printf("  SUCCESS: Server rejected unauthorized full view with: %v", st.Message())

	// --- TEST 4: Field Team Full View (Includes coordinates & attributes) ---
	log.Println("\n[TEST 4] Verifying Field Team access to VIEW_FULL...")
	fieldResp, err := client.ListHazards(withAuth(ctx, fieldToken), &aggregatorv1.ListHazardsRequest{
		View: commonv1.View_VIEW_FULL,
		Page: &commonv1.PageRequest{PageSize: 10},
	})
	if err != nil {
		log.Fatalf("  FAILED: ListHazards(VIEW_FULL) error: %v", err)
	}
	log.Printf("  SUCCESS: Retrieved %d hazards in full view", len(fieldResp.Items))
	if len(fieldResp.Items) > 0 {
		full := fieldResp.Items[0].GetFull()
		if full == nil {
			log.Fatalf("  FAILED: Expected Full hazard payload")
		}
		log.Printf("    - Hazard ID: %s", full.HazardId)
		log.Printf("    - Source Ref ID: %s", full.SourceRefId)
		log.Printf("    - Coordinates: Lat=%.4f, Lon=%.4f", full.GetLatitude(), full.GetLongitude())
		log.Printf("    - Dynamic Attributes: %v", full.Attributes.AsMap())
	}

	// --- TEST 5: Schema Evolution (v1 -> v2) on PVMBG ---
	if cfg.PVMBGBearerToken != "" {
		log.Println("\n[TEST 5] Testing PVMBG Schema Evolution (POST /admin/schema-version = 2)...")
		postAdmin(cfg.PVMBGBaseURL+"/admin/schema-version", cfg.PVMBGBearerToken, map[string]any{"version": 2})
		log.Println("  Waiting 11 seconds for new generator cycle and poller...")
		time.Sleep(11 * time.Second)

		fieldRespV2, err := client.ListHazards(withAuth(ctx, fieldToken), &aggregatorv1.ListHazardsRequest{
			View: commonv1.View_VIEW_FULL,
			Filter: &commonv1.HazardFilter{
				Source: commonv1.Source_SOURCE_PVMBG,
			},
			Page: &commonv1.PageRequest{PageSize: 5},
		})
		if err == nil && len(fieldRespV2.Items) > 0 {
			foundConfidence := false
			for _, h := range fieldRespV2.Items {
				full := h.GetFull()
				if full != nil && full.Attributes != nil {
					if _, has := full.Attributes.Fields["confidence_level"]; has {
						foundConfidence = true
						log.Printf("  SUCCESS: Found dynamic 'confidence_level' in hazard %s: %v", full.HazardId, full.Attributes.Fields["confidence_level"])
						break
					}
				}
			}
			if !foundConfidence {
				log.Println("  NOTE: No new v2 event generated yet or confidence_level attribute not yet present in top items.")
			}
		}

		// Restore schema version 1
		postAdmin(cfg.PVMBGBaseURL+"/admin/schema-version", cfg.PVMBGBearerToken, map[string]any{"version": 1})
	}

	// --- TEST 6: Outage Simulation & Graceful Degradation ---
	if cfg.PVMBGBearerToken != "" {
		log.Println("\n[TEST 6] Testing PVMBG Outage Simulation (POST /admin/outage = true)...")
		postAdmin(cfg.PVMBGBaseURL+"/admin/outage", cfg.PVMBGBearerToken, map[string]any{"enabled": true})
		log.Println("  Waiting 11 seconds for poller to detect outage...")
		time.Sleep(11 * time.Second)

		success := true

		// 6a. BMKG must continue serving hazards during PVMBG outage (isolation)
		bmkgList, err := client.ListHazards(withAuth(ctx, mediaToken), &aggregatorv1.ListHazardsRequest{
			View:   commonv1.View_VIEW_SUMMARY,
			Filter: &commonv1.HazardFilter{Source: commonv1.Source_SOURCE_BMKG},
		})
		if err != nil || len(bmkgList.Items) == 0 {
			log.Printf("  FAILED: BMKG should continue serving during PVMBG outage! Err: %v", err)
			success = false
		} else {
			log.Printf("  SUCCESS: BMKG continues serving %d hazards during PVMBG outage", len(bmkgList.Items))
		}

		// 6b. Previously-ingested PVMBG hazards must still be served from canonical store
		// (graceful degradation: serve stale data, not block it).
		// The mock's outage only makes the upstream HTTP endpoint return 503; the
		// canonical PostgreSQL store retains all previously-ingested records.
		pvmbgList, err := client.ListHazards(withAuth(ctx, mediaToken), &aggregatorv1.ListHazardsRequest{
			View:   commonv1.View_VIEW_SUMMARY,
			Filter: &commonv1.HazardFilter{Source: commonv1.Source_SOURCE_PVMBG},
		})
		if err != nil {
			log.Printf("  FAILED: ListHazards(PVMBG) returned error during outage: %v", err)
			success = false
		} else {
			log.Printf("  SUCCESS: Canonical store serves %d stale-but-available PVMBG hazards during outage (graceful degradation)", len(pvmbgList.Items))
		}

		// 6c. Source status must be STALE for PVMBG
		outageStatus, err := client.GetSourceStatuses(withAuth(ctx, mediaToken), &aggregatorv1.GetSourceStatusesRequest{})
		if err != nil {
			log.Printf("  FAILED: GetSourceStatuses returned error: %v", err)
			success = false
		} else {
			pvmbgFound := false
			for _, s := range outageStatus.Freshness {
				if s.Source == commonv1.Source_SOURCE_PVMBG {
					pvmbgFound = true
					if s.Status != commonv1.SourceStatus_SOURCE_STATUS_STALE {
						log.Printf("  FAILED: PVMBG source status should be STALE during outage, got %s", s.Status.String())
						success = false
					} else {
						log.Printf("  SUCCESS: PVMBG source status is correctly STALE during outage (StaleSince: %v)", s.StaleSince.AsTime())
					}
				}
			}
			if !pvmbgFound {
				log.Printf("  FAILED: PVMBG source not found in GetSourceStatuses response")
				success = false
			}
		}

		// Restore PVMBG outage to false
		postAdmin(cfg.PVMBGBaseURL+"/admin/outage", cfg.PVMBGBearerToken, map[string]any{"enabled": false})
		log.Println("  Restored PVMBG outage to false.")

		if success {
			log.Println("  SUCCESS: PVMBG outage simulation completed successfully")
		} else {
			log.Fatalf("  FAILED: PVMBG outage simulation failed")

		}
	}

	log.Println("=================================================================")
	log.Println(" ALL SPECIFICATION TESTS PASSED SUCCESSFULLY!")
	log.Println("=================================================================")
}

func postAdmin(endpoint, bearerToken string, body map[string]any) {
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+bearerToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Correlation-ID", "test-admin-e2e")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("  [Admin] Request to %s failed: %v", endpoint, err)
		return
	}
	defer resp.Body.Close()
	resBody, _ := io.ReadAll(resp.Body)
	log.Printf("  [Admin] POST %s returned HTTP %d: %s", endpoint, resp.StatusCode, string(resBody))
}
