package cassandra

import (
	"context"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// Leave headroom below Cassandra's default 50KiB cross-partition batch limit.
// Authoritative conditional/fencing batches MUST NOT use this helper. It is
// only for deterministic derived effects whose completion marker is written
// last, so an interrupted sequence is safely repaired from canonical state.
const recoverableBatchBytes = 32 << 10

func (db *DB) execRecoverableBatch(ctx context.Context, batch *gocql.Batch) error {
	size := 0
	for _, entry := range batch.Entries {
		size += len(entry.Stmt) + 32*len(entry.Args)
		for _, arg := range entry.Args {
			switch value := arg.(type) {
			case string:
				size += len(value)
			case []byte:
				size += len(value)
			}
		}
	}
	if size <= recoverableBatchBytes {
		return batch.Exec()
	}
	// Single-partition mutations may contain a large required detection-key
	// list, without duplicating that payload in a cross-partition logged batch.
	for _, entry := range batch.Entries {
		if err := db.session.Query(entry.Stmt, entry.Args...).WithContext(ctx).Exec(); err != nil {
			return err
		}
	}
	return nil
}
