package cassandra

import (
	"context"
	"strings"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/telemetry"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// observer turns each query and batch into a client span, only under a caller's span (like the
// Postgres adapter's otelpgx tracer). Spans are named by operation and table; values are never
// recorded.
type observer struct{ tracer trace.Tracer }

func newObserver() observer {
	return observer{tracer: telemetry.ParentOnlyTracerProvider().Tracer("aggregator/storage/cassandra")}
}

func (o observer) ObserveQuery(ctx context.Context, q gocql.ObservedQuery) {
	o.span(ctx, querySummary(q.Statement), q.Keyspace, q.Start, q.End, q.Err, attribute.Int("db.response.returned_rows", q.Rows))
}

func (o observer) ObserveBatch(ctx context.Context, b gocql.ObservedBatch) {
	o.span(ctx, "BATCH", b.Keyspace, b.Start, b.End, b.Err, attribute.Int("db.operation.batch.size", len(b.Statements)))
}

func (o observer) span(ctx context.Context, summary, keyspace string, start, end time.Time, err error, extra attribute.KeyValue) {
	_, span := o.tracer.Start(ctx, summary,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithTimestamp(start),
		trace.WithAttributes(
			attribute.String("db.system.name", "cassandra"),
			attribute.String("db.namespace", keyspace),
			attribute.String("db.query.summary", summary),
			extra,
		),
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End(trace.WithTimestamp(end))
}

// querySummary is the operation and table of a CQL statement, e.g. "SELECT hazards_by_id".
func querySummary(stmt string) string {
	fields := strings.Fields(stmt)
	if len(fields) == 0 {
		return "CQL"
	}
	op := strings.ToUpper(fields[0])
	var marker string
	switch op {
	case "SELECT", "DELETE":
		marker = "FROM"
	case "INSERT":
		marker = "INTO"
	case "UPDATE":
		return op + " " + strings.Trim(fieldAt(fields, 1), "\"")
	default:
		return op
	}
	for i, f := range fields {
		if strings.EqualFold(f, marker) {
			return op + " " + strings.Trim(fieldAt(fields, i+1), "\"(")
		}
	}
	return op
}

func fieldAt(fields []string, i int) string {
	if i < len(fields) {
		return fields[i]
	}
	return ""
}
