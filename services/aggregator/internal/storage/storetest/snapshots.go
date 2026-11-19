package storetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/google/uuid"
)

// StreamingSnapshotContract is also invoked by dedicated backend tests so
// large snapshots can be verified without running unrelated heavy contracts.
func StreamingSnapshotContract(t *testing.T, b *storage.Backend) {
	t.Helper()
	s, ok := b.Snapshots.(storage.StreamingSnapshots)
	if !ok {
		t.Fatal("streaming snapshots unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	stream := "streaming-" + uuid.NewString()
	dir := t.TempDir()
	if seq, r, err := s.OpenLatestSnapshot(ctx, stream, 100, dir); err != nil || seq != 0 || r != nil {
		t.Fatal("unexpected initial state", seq, err)
	}
	if err := s.SaveSnapshotFrom(ctx, stream, 10, bytes.NewReader([]byte("old"))); err != nil {
		t.Fatal(err)
	}
	const largeSize = 12<<20 + 123
	input := &patternSnapshot{size: largeSize}
	if err := s.SaveSnapshotFrom(ctx, stream, 20, input); err != nil {
		t.Fatal(err)
	}
	if input.largest > storage.SnapshotChunkBytes {
		t.Fatalf("unbounded source read %d", input.largest)
	}
	seq, r, err := s.OpenLatestSnapshot(ctx, stream, 20, dir)
	if err != nil || seq != 20 || r == nil {
		t.Fatal("open large snapshot", seq, err)
	}
	actual, expected := sha256.New(), sha256.New()
	n, err := io.CopyBuffer(actual, r, make([]byte, 64<<10))
	if err != nil || n != largeSize {
		t.Fatal(n, err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	_, _ = io.CopyBuffer(expected, &patternSnapshot{size: largeSize}, make([]byte, 64<<10))
	if !bytes.Equal(actual.Sum(nil), expected.Sum(nil)) {
		t.Fatal("large snapshot bytes differ")
	}
	if err := s.SaveSnapshotFrom(ctx, stream, 20, bytes.NewReader([]byte("other codec"))); err != nil {
		t.Fatal(err)
	}
	seq, r, err = s.OpenLatestSnapshot(ctx, stream, 20, dir)
	if err != nil || seq != 20 || r == nil {
		t.Fatal(seq, err)
	}
	n, err = io.Copy(io.Discard, r)
	_ = r.Close()
	if err != nil || n != largeSize {
		t.Fatal("same-sequence overwrite changed winner", n, err)
	}
	// Two competing writers use different bytes and must still publish one
	// complete immutable value, never a mixture of chunks.
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, body := range []string{"first", "second"} {
		wg.Add(1)
		go func(body string) {
			defer wg.Done()
			errs <- s.SaveSnapshotFrom(ctx, stream, 30, bytes.NewReader([]byte(body)))
		}(body)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	seq, r, err = s.OpenLatestSnapshot(ctx, stream, 100, dir)
	if err != nil || seq != 30 || r == nil {
		t.Fatal(seq, err)
	}
	body, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || (string(body) != "first" && string(body) != "second") {
		t.Fatal("competing publication corrupted", string(body), err)
	}
	if seq, r, err := s.OpenLatestSnapshot(ctx, stream, 15, dir); err != nil || seq != 0 || r != nil {
		t.Fatal("retired snapshot returned", seq, err)
	}
	failure := errors.New("snapshot source failed")
	if err := s.SaveSnapshotFrom(ctx, stream, 40, &failedSnapshot{err: failure}); !errors.Is(err, failure) {
		t.Fatal("failed source was published", err)
	}
	if err := s.SaveSnapshotFrom(ctx, stream, 40, &failingUploadSnapshot{patternSnapshot: patternSnapshot{size: 2 << 20}, err: failure}); !errors.Is(err, failure) {
		t.Fatal("partial upload was published", err)
	}
	seq, r, err = s.OpenLatestSnapshot(ctx, stream, 100, dir)
	if err != nil || seq != 30 || r == nil {
		t.Fatal("failed upload changed latest", seq, err)
	}
	_ = r.Close()
	files, _ := os.ReadDir(dir)
	if len(files) != 0 {
		t.Fatal("temporary download files leaked", files)
	}
}

type patternSnapshot struct {
	size, offset int64
	largest      int
}

func (p *patternSnapshot) Read(b []byte) (int, error) {
	p.largest = max(p.largest, len(b))
	if p.offset >= p.size {
		return 0, io.EOF
	}
	n := int(min(int64(len(b)), p.size-p.offset))
	for i := 0; i < n; i++ {
		b[i] = byte((p.offset + int64(i)) * 7)
	}
	p.offset += int64(n)
	return n, nil
}
func (p *patternSnapshot) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		p.offset = offset
	case io.SeekCurrent:
		p.offset += offset
	case io.SeekEnd:
		p.offset = p.size + offset
	default:
		return 0, fmt.Errorf("invalid seek")
	}
	return p.offset, nil
}

type failedSnapshot struct{ err error }

func (p *failedSnapshot) Read([]byte) (int, error)       { return 0, p.err }
func (p *failedSnapshot) Seek(int64, int) (int64, error) { return 0, nil }

type failingUploadSnapshot struct {
	patternSnapshot
	seeks int
	err   error
}

func (p *failingUploadSnapshot) Seek(offset int64, whence int) (int64, error) {
	p.seeks++
	return p.patternSnapshot.Seek(offset, whence)
}
func (p *failingUploadSnapshot) Read(b []byte) (int, error) {
	if p.seeks >= 2 && p.offset >= storage.SnapshotChunkBytes {
		return 0, p.err
	}
	return p.patternSnapshot.Read(b)
}
