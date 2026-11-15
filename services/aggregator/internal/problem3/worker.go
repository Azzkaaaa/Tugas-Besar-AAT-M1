package problem3

import (
	"context"
	"log/slog"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

func RunWorker(ctx context.Context, reports storage.FieldReports) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		count, err := reports.ProcessReports(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			slog.WarnContext(ctx, "field report worker will retry", "error", err)
		case count != 0:
			slog.InfoContext(ctx, "field reports processed", "count", count)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
