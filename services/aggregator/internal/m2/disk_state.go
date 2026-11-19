package m2

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

const diskStateFormat = "bnpb-satellite-disk-v1"

type diskStateHeader struct {
	Format                           string `json:"format"`
	Version                          int    `json:"version"`
	Sequence                         int64  `json:"sequence"`
	Lateness, Retention              time.Duration
	MaxDetections                    int
	MaxTime, Compacted               time.Time
	Late, Low, Expired, OverCapacity int64
	Active, Fires, Singles           int
	KeepFinal                        bool
}

type diskStateRow struct {
	Table      string          `json:"table"`
	Key        string          `json:"key"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	Component  string          `json:"component,omitempty"`
	AtMS       int64           `json:"at_ms,omitempty"`
	Members    int             `json:"members,omitempty"`
	OccurredMS int64           `json:"occurred_ms,omitempty"`
	Withdrawn  bool            `json:"withdrawn,omitempty"`
}

func (d *diskFireProcessor) header() diskStateHeader {
	return diskStateHeader{Format: diskStateFormat, Version: fireStateVersion, Sequence: d.lastSequence,
		Lateness: d.meta.Lateness, Retention: d.meta.Retention, MaxDetections: d.meta.MaxDetections,
		MaxTime: d.meta.MaxTime, Compacted: d.meta.compacted, Late: d.meta.Late, Low: d.meta.Low,
		Expired: d.meta.Expired, OverCapacity: d.meta.OverCapacity, Active: d.active,
		Fires: d.fires, Singles: d.singles, KeepFinal: d.keepFinal}
}

// WriteState encodes one SQL row at a time. The encoding is logical, ordered
// and deterministic, independent of SQLite page layout or previous snapshots.
// It never reconstructs a graph-sized map or compressed []byte in memory.
func (d *diskFireProcessor) WriteState(ctx context.Context, out io.Writer, sequence int64) error {
	if sequence != d.lastSequence {
		return workingError(errors.New("snapshot does not match the processor sequence"))
	}
	tx, err := d.graph.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return workingError(err)
	}
	defer tx.Rollback()
	z := gzip.NewWriter(out)
	encoder := json.NewEncoder(z)
	if err := encoder.Encode(d.header()); err != nil {
		return err
	}
	for _, table := range []struct{ name, query string }{
		{"points", `SELECT key,detection FROM points ORDER BY key`},
		{"seen", `SELECT key,at_ms FROM seen ORDER BY key`},
		{"owners", `SELECT key,component_id FROM owners ORDER BY key`},
		{"components", `SELECT id,members FROM components ORDER BY id`},
		{"published", `SELECT id FROM published ORDER BY id`},
		{"latest", `SELECT id,payload,occurred_ms,withdrawn FROM latest ORDER BY id`},
		{"final", `SELECT id,payload FROM final ORDER BY id`},
	} {
		rows, err := tx.QueryContext(ctx, table.query)
		if err != nil {
			return workingError(err)
		}
		for rows.Next() {
			row := diskStateRow{Table: table.name}
			switch table.name {
			case "points", "final":
				err = rows.Scan(&row.Key, &row.Payload)
			case "seen":
				err = rows.Scan(&row.Key, &row.AtMS)
			case "owners":
				err = rows.Scan(&row.Key, &row.Component)
			case "components":
				err = rows.Scan(&row.Key, &row.Members)
			case "published":
				err = rows.Scan(&row.Key)
			case "latest":
				err = rows.Scan(&row.Key, &row.Payload, &row.OccurredMS, &row.Withdrawn)
			}
			if err == nil {
				err = ctx.Err()
			}
			if err == nil {
				err = encoder.Encode(row)
			}
			if err != nil {
				rows.Close()
				return err
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return workingError(err)
		}
	}
	if err := z.Close(); err != nil {
		return err
	}
	return workingError(tx.Commit())
}

func restoreDiskFireProcessor(ctx context.Context, root string, lateness, retention time.Duration, maxPoints int, sequence int64, input io.Reader) (_ *diskFireProcessor, err error) {
	z, err := gzip.NewReader(input)
	if err != nil {
		return nil, err
	}
	defer z.Close()
	decoder := json.NewDecoder(z)
	decoder.DisallowUnknownFields()
	var header diskStateHeader
	if err := decoder.Decode(&header); err != nil {
		return nil, err
	}
	if header.Format != diskStateFormat || header.Version != fireStateVersion || header.Sequence != sequence ||
		header.Lateness != lateness || header.Retention != max(retention, lateness+linkWindow) ||
		header.MaxDetections != maxPoints || header.Active < 0 || header.Fires < 0 || header.Singles < 0 {
		return nil, fmt.Errorf("%w: snapshot format, policy or sequence differs", ErrIncompatibleState)
	}
	disk, err := newDiskFireProcessor(ctx, root, lateness, retention, maxPoints)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = disk.Close()
		}
	}()
	tx, err := disk.graph.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, workingError(err)
	}
	defer tx.Rollback()
	ranks := map[string]int{"points": 0, "seen": 1, "owners": 2, "components": 3, "published": 4, "latest": 5, "final": 6}
	lastRank, lastKey := -1, ""
	for {
		var row diskStateRow
		if err := decoder.Decode(&row); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		rank, known := ranks[row.Table]
		if !known || rank < lastRank || (rank == lastRank && row.Key <= lastKey) {
			return nil, errors.New("snapshot rows are duplicated or out of order")
		}
		lastRank, lastKey = rank, row.Key
		if err := restoreGraphRow(ctx, tx, row); err != nil {
			return nil, err
		}
	}
	if err := validateRestoredGraph(ctx, tx, header); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, workingError(err)
	}
	disk.meta.MaxTime, disk.meta.compacted = header.MaxTime, header.Compacted
	disk.meta.Late, disk.meta.Low, disk.meta.Expired, disk.meta.OverCapacity = header.Late, header.Low, header.Expired, header.OverCapacity
	disk.active, disk.fires, disk.singles = header.Active, header.Fires, header.Singles
	disk.keepFinal, disk.lastSequence = header.KeepFinal, sequence
	return disk, nil
}

func restoreGraphRow(ctx context.Context, tx *sql.Tx, row diskStateRow) error {
	if row.Key == "" {
		return errors.New("snapshot has an empty row key")
	}
	switch row.Table {
	case "points":
		var point Detection
		if err := json.Unmarshal(row.Payload, &point); err != nil || point.Key != row.Key {
			return errors.New("invalid snapshot point")
		}
		return putGraphPoint(ctx, tx, point)
	case "seen":
		return execGraph(ctx, tx, `INSERT INTO seen VALUES (?,?)`, row.Key, row.AtMS)
	case "owners":
		if row.Component == "" {
			return errors.New("snapshot owner has no component")
		}
		return execGraph(ctx, tx, `INSERT INTO owners VALUES (?,?)`, row.Key, row.Component)
	case "components":
		if row.Members < 1 {
			return errors.New("snapshot has an empty component")
		}
		return execGraph(ctx, tx, `INSERT INTO components VALUES (?,?)`, row.Key, row.Members)
	case "published":
		return execGraph(ctx, tx, `INSERT INTO published VALUES (?)`, row.Key)
	case "latest", "final":
		var record domain.HazardRecord
		if err := json.Unmarshal(row.Payload, &record); err != nil || record.SourceRefID != row.Key || record.Source != domain.SourceSatellite {
			return errors.New("invalid snapshot hazard")
		}
		if row.Table == "final" {
			return execGraph(ctx, tx, `INSERT INTO final VALUES (?,?)`, row.Key, []byte(row.Payload))
		}
		if row.Withdrawn != domain.Withdrawn(&record) || row.OccurredMS != record.OccurredAt.UnixMilli() {
			return errors.New("snapshot hazard metadata differs from its payload")
		}
		return execGraph(ctx, tx, `INSERT INTO latest VALUES (?,?,?,?)`, row.Key, []byte(row.Payload), row.OccurredMS, row.Withdrawn)
	default:
		return errors.New("unknown snapshot table")
	}
}

func validateRestoredGraph(ctx context.Context, tx *sql.Tx, header diskStateHeader) error {
	for _, check := range []struct {
		query string
		want  int
	}{
		{`SELECT COUNT(*) FROM points`, header.Active},
		{`SELECT COUNT(*) FROM components WHERE members>=2`, header.Fires},
		{`SELECT COUNT(*) FROM components WHERE members=1`, header.Singles},
		{`SELECT COUNT(*) FROM points p LEFT JOIN owners o ON o.key=p.key WHERE (p.confidence<>'l' AND o.key IS NULL) OR (p.confidence='l' AND o.key IS NOT NULL)`, 0},
		{`SELECT COUNT(*) FROM owners o LEFT JOIN points p ON p.key=o.key LEFT JOIN components c ON c.id=o.component_id WHERE p.key IS NULL OR c.id IS NULL`, 0},
		{`SELECT COUNT(*) FROM components c WHERE c.members<>(SELECT COUNT(*) FROM owners o WHERE o.component_id=c.id)`, 0},
	} {
		var got int
		if err := tx.QueryRowContext(ctx, check.query).Scan(&got); err != nil {
			return workingError(err)
		}
		if got != check.want {
			return errors.New("snapshot graph counts or ownership are inconsistent")
		}
	}
	return nil
}
