package leader

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

type fixedFence struct{ err error }

func (fixedFence) Fence(context.Context, string, string) (Authority, error) { return Authority{}, nil }
func (f fixedFence) ValidateAuthority(context.Context, Authority) error     { return f.err }

func TestCheckAuthorityLogsOnlyLostAuthority(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	defer slog.SetDefault(prev)
	token := Authority{Lease: "aggregator-bmkg-poller", Holder: "pod-1", Epoch: 7}

	if err := CheckAuthority(WithAuthority(context.Background(), token, fixedFence{ErrLostAuthority})); !errors.Is(err, ErrLostAuthority) {
		t.Fatalf("want ErrLostAuthority, got %v", err)
	}
	out := buf.String()
	for _, want := range []string{`"msg":"fenced operation refused"`, `"lease":"aggregator-bmkg-poller"`, `"holder":"pod-1"`, `"epoch":7`} {
		if !strings.Contains(out, want) {
			t.Fatalf("log %q lacks %s", out, want)
		}
	}
	buf.Reset()
	if err := CheckAuthority(WithAuthority(context.Background(), token, fixedFence{errors.New("timeout")})); err == nil {
		t.Fatal("transport error must propagate")
	}
	if err := CheckAuthority(WithAuthority(context.Background(), token, fixedFence{})); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatalf("only lost authority is logged, got %q", buf.String())
	}
}
