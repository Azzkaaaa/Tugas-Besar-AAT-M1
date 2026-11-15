package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"time"
	"unicode/utf8"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/problem"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (g *gateway) submitFieldReport(client aggregatorv1.FieldReportServiceClient, timeout time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Header.Get("Authorization") == "":
			g.write(w, r, nil, problem.New(http.StatusUnauthorized, "Authentication is required."))
			return
		case client == nil:
			g.write(w, r, nil, problem.New(http.StatusServiceUnavailable, "Report intake is unavailable."))
			return
		}
		select {
		case g.inFlight <- struct{}{}:
			defer func() { <-g.inFlight }()
		default:
			g.write(w, r, nil, problem.New(http.StatusTooManyRequests, "Report intake is busy."))
			return
		}
		mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if mediaType != "application/json" || len(r.Header.Values("Idempotency-Key")) != 1 {
			g.write(w, r, nil, problem.BadRequest("Provide application/json and one Idempotency-Key."))
			return
		}
		controller := http.NewResponseController(w)
		_ = controller.SetReadDeadline(time.Now().Add(timeout))
		data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<10))
		_ = controller.SetReadDeadline(time.Time{})
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			g.write(w, r, nil, problem.New(http.StatusRequestEntityTooLarge, "Report body exceeds 16 KiB."))
			return
		}
		var body struct {
			ObservedAt  string `json:"observed_at"`
			AreaName    string `json:"area_name"`
			Description string `json:"description"`
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err != nil || !utf8.Valid(data) || !json.Valid(data) || decoder.Decode(&body) != nil {
			g.write(w, r, nil, problem.BadRequest("Invalid field report JSON."))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		result, err := client.SubmitFieldReport(ctx, &aggregatorv1.SubmitFieldReportRequest{
			IdempotencyKey: r.Header.Get("Idempotency-Key"), ObservedAt: body.ObservedAt,
			AreaName: body.AreaName, Description: body.Description,
		})
		if status.Code(err) == codes.AlreadyExists {
			g.write(w, r, nil, problem.New(http.StatusConflict, "Idempotency-Key is already associated with a different report."))
			return
		}
		if err != nil {
			g.write(w, r, nil, problem.FromGRPC(err))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(result)
	}
}
