package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
)

func TestSnapshotIOIntegrityAndCleanup(t *testing.T) {
	ctx := context.Background()
	data := bytes.Repeat([]byte("bounded-state"), 70000)
	size, digest, err := SnapshotDigest(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	sink, err := NewSnapshotSink(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	chunks, err := UploadSnapshotChunks(ctx, bytes.NewReader(data), size, digest, func(index int, b []byte) error { return sink.Append(ctx, index, b) })
	if err != nil {
		t.Fatal(err)
	}
	r, err := sink.Finish(chunks, size, digest)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal("restored bytes differ", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	files, _ := os.ReadDir(dir)
	if len(files) != 0 {
		t.Fatal("temporary download leaked")
	}
}

func TestSnapshotIORejectsCorruptionAndSourceChanges(t *testing.T) {
	ctx := context.Background()
	size, digest, _ := SnapshotDigest(ctx, bytes.NewReader([]byte("correct")))
	for _, tc := range []struct {
		name   string
		append func(*SnapshotSink) error
		chunks int
		size   int64
		digest string
	}{
		{"missing", func(s *SnapshotSink) error { return nil }, 1, size, digest},
		{"gap", func(s *SnapshotSink) error { return s.Append(ctx, 1, []byte("correct")) }, 1, size, digest},
		{"oversized", func(s *SnapshotSink) error { return s.Append(ctx, 0, make([]byte, SnapshotChunkBytes+1)) }, 1, size, digest},
		{"digest", func(s *SnapshotSink) error { return s.Append(ctx, 0, []byte("corrupt")) }, 1, size, digest},
		{"size", func(s *SnapshotSink) error { return s.Append(ctx, 0, []byte("correct")) }, 1, size + 1, digest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			sink, err := NewSnapshotSink(dir)
			if err != nil {
				t.Fatal(err)
			}
			err = tc.append(sink)
			if err == nil {
				_, err = sink.Finish(tc.chunks, tc.size, tc.digest)
			}
			if !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatal("accepted invalid chunks", err)
			}
			if err := sink.Close(); err != nil {
				t.Fatal(err)
			}
			files, _ := os.ReadDir(dir)
			if len(files) != 0 {
				t.Fatal("failed download leaked")
			}
		})
	}
	if _, err := UploadSnapshotChunks(ctx, bytes.NewReader([]byte("changed")), size, digest, func(int, []byte) error { return nil }); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := SnapshotDigest(canceled, bytes.NewReader([]byte("correct"))); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	failed := errors.New("write failed")
	if _, err := UploadSnapshotChunks(ctx, bytes.NewReader([]byte("correct")), size, digest, func(int, []byte) error { return failed }); !errors.Is(err, failed) {
		t.Fatal(err)
	}
}
