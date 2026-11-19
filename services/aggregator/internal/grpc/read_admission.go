package grpc

import (
	"context"
	"sync"
	"sync/atomic"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// BoundedReadOptions binds one FULL read to its encoded response's lifetime,
// including buffers queued behind a slow HTTP/2 peer. Install after the JWT
// interceptor. SUMMARY and other small operations do not consume this credit.
// All three options are required: handler-return and stats.End happen before
// the transport necessarily releases its final reference to the payload.
func BoundedReadOptions() []grpc.ServerOption {
	a := newReadAdmission()
	return a.options()
}

func (a *readAdmission) options() []grpc.ServerOption {
	return []grpc.ServerOption{grpc.StatsHandler(a), grpc.ForceServerCodecV2(a), grpc.ChainUnaryInterceptor(a.intercept)}
}

type readAdmission struct {
	encoding.CodecV2
	slot    chan struct{}
	pending sync.Map // response pointer -> *readCredit, until Marshal takes ownership
}

type readCredit struct{ release func() }
type readRPC struct {
	credit   *readCredit
	response any
}
type readRPCKey struct{}

func newReadAdmission() *readAdmission {
	return &readAdmission{CodecV2: encoding.GetCodecV2("proto"), slot: make(chan struct{}, 1)}
}

func fullHazardRequest(req any) bool {
	switch r := req.(type) {
	case *aggregatorv1.ListHazardsRequest:
		return r.GetView() == commonv1.View_VIEW_FULL
	case *aggregatorv1.GetHazardRequest:
		return r.GetView() == commonv1.View_VIEW_FULL
	default:
		return false
	}
}

func (a *readAdmission) intercept(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	if !fullHazardRequest(req) {
		return next(ctx, req)
	}
	caller := auth.GetCallerContext(ctx)
	if caller == nil {
		return nil, status.Error(codes.Unauthenticated, "missing caller context")
	}
	if !caller.HasScope("hazards:read:full") {
		return nil, status.Error(codes.PermissionDenied, "insufficient scope for full view")
	}
	if ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	select {
	case a.slot <- struct{}{}:
	default:
		return nil, status.Error(codes.ResourceExhausted, "full hazard response capacity is busy; retry later")
	}
	credit := &readCredit{release: sync.OnceFunc(func() { <-a.slot })}
	rpc, ok := ctx.Value(readRPCKey{}).(*readRPC)
	if !ok {
		credit.release()
		return nil, status.Error(codes.Internal, "response lifetime tracking is unavailable")
	}
	rpc.credit = credit
	registered := false
	defer func() {
		if !registered {
			credit.release()
		}
	}()
	// Compression would free the original buffer before the compressed payload
	// leaves the transport queue. Keep bounded full responses in identity form.
	if err := grpc.SetSendCompressor(ctx, encoding.Identity); err != nil {
		return nil, status.Errorf(codes.Internal, "set full response encoding: %v", err)
	}
	response, err := next(ctx, req)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, status.Error(codes.Internal, "full hazard response is missing")
	}
	rpc.response = response
	a.pending.Store(response, credit)
	registered = true
	return response, nil
}

func (a *readAdmission) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, readRPCKey{}, &readRPC{})
}
func (a *readAdmission) HandleRPC(ctx context.Context, event stats.RPCStats) {
	if _, end := event.(*stats.End); !end {
		return
	}
	rpc, _ := ctx.Value(readRPCKey{}).(*readRPC)
	if rpc == nil || rpc.credit == nil {
		return
	}
	// Cleanup when an error/cancellation bypasses Marshal. Once Marshal claims
	// the credit, only the final encoded-buffer release may return it.
	if rpc.response == nil || a.pending.CompareAndDelete(rpc.response, rpc.credit) {
		rpc.credit.release()
	}
	rpc.response, rpc.credit = nil, nil
}
func (a *readAdmission) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (a *readAdmission) HandleConn(context.Context, stats.ConnStats) {}

func (a *readAdmission) Marshal(v any) (mem.BufferSlice, error) {
	value, tracked := a.pending.LoadAndDelete(v)
	data, err := a.CodecV2.Marshal(v)
	if !tracked {
		return data, err
	}
	credit := value.(*readCredit)
	if err != nil || data.Len() == 0 {
		credit.release()
		return data, err
	}
	var remaining atomic.Int32
	remaining.Store(int32(len(data)))
	done := func() {
		if remaining.Add(-1) == 0 {
			credit.release()
		}
	}
	for i, original := range data {
		b := original.ReadOnlyData()
		if mem.IsBelowBufferPoolingThreshold(cap(b)) {
			// NewBuffer otherwise becomes a no-op SliceBuffer with no final-free
			// callback. Only tiny responses need this small allocation/copy.
			capacity := max(1, cap(b))
			for mem.IsBelowBufferPoolingThreshold(capacity) {
				capacity *= 2
			}
			copyOf := make([]byte, len(b), capacity)
			copy(copyOf, b)
			original.Free()
			original, b = nil, copyOf
		}
		data[i] = mem.NewBuffer(&b, &readBufferRelease{original: original, done: done})
	}
	return data, nil
}

// This is a final-free callback, not an allocator. The large protobuf buffer is
// shared without copying and returned to its original pool after every slice
// and transport reference is gone. No response object is retained by the credit.
type readBufferRelease struct {
	original mem.Buffer
	done     func()
}

func (*readBufferRelease) Get(int) *[]byte { panic("read buffer release is not an allocator") }
func (p *readBufferRelease) Put(*[]byte) {
	if p.original != nil {
		p.original.Free()
	}
	p.done()
}
