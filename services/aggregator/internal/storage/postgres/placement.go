package postgres

import (
	"context"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

func (s audit) Locate(ctx context.Context, id string) (storage.PlacementResult, error) {
	out := storage.PlacementResult{}
	h, err := (hazards{s.db}).Get(ctx, id)
	if err != nil {
		out.UnavailableUnits = []string{"postgres-canonical"}
		return out, err
	}
	if h == nil {
		return out, nil
	}
	out.Exists = true
	var node *string
	err = s.db.Pool.QueryRow(ctx, `SELECT current_setting('bnpb.node_name', true)`).Scan(&node)
	if err != nil || node == nil || *node == "" {
		out.UnavailableUnits = []string{"postgres-node-identity"}
		return out, err
	}
	out.Locations = []storage.HazardLocation{{Node: *node, Unit: "postgres-canonical", Role: "primary"}}
	return out, nil
}
