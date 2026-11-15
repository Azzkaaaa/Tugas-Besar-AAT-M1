package problem3

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/auth"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

type listedInbox struct{ storage.IngestInbox }

func (listedInbox) ListMessages(context.Context, string, storage.IngestCursor, int) (storage.IngestPage, error) {
	return storage.IngestPage{Items: []storage.IngestMessage{{ID: "msg-1", Source: "PVMBG", State: storage.IngestParked,
		Reason: "invalid_utf8", ReceivedAt: time.Now(), Raw: []byte("private"), CorrelationID: "private"}},
		UnavailableUnits: []string{"ingest-inbox-01"}}, nil
}

func TestAuditOnlyExposesMetadata(t *testing.T) {
	api := api{store: &storage.Backend{Inbox: listedInbox{}}, verifier: &auth.JWTVerifier{}, auditToken: "test-auditor"}
	h := api.routes()
	for _, test := range []struct {
		path, auth string
		code       int
	}{
		{"/internal/audit/unprocessed?source=PVMBG", "Bearer test-auditor", 200},
		{"/internal/audit/unprocessed?source=PVMBG", "", 401},
		{"/internal/audit/unprocessed?limit=1001", "Bearer test-auditor", 400},
		{"/internal/ops/ingest/msg-1/payload", "Bearer test-auditor", 401},
	} {
		request := httptest.NewRequest(http.MethodGet, test.path, nil)
		request.Header.Set("Authorization", test.auth)
		response := httptest.NewRecorder()
		h.ServeHTTP(response, request)
		if response.Code != test.code || strings.Contains(response.Body.String(), "private") {
			t.Fatalf("status or disclosure error for %s: %d %s", test.path, response.Code, response.Body)
		}
		if test.code == 200 && (!strings.Contains(response.Body.String(), `"partial":true`) ||
			!strings.Contains(response.Body.String(), `"retryable":false`) || !strings.Contains(response.Body.String(), `"next_cursor":null`)) {
			t.Fatalf("incorrect Audit contract: %s", response.Body)
		}
	}
}
