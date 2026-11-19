package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"os"
)

const SnapshotChunkBytes = 256 << 10

var ErrInvalidSnapshot = errors.New("snapshot chunks, size or digest differ")

// SnapshotDigest streams the stable source once and leaves it rewound.
func SnapshotDigest(ctx context.Context, source io.ReadSeeker) (int64, string, error) {
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return 0, "", err
	}
	h := sha256.New()
	n, err := io.CopyBuffer(h, snapshotContextReader{ctx, source}, make([]byte, 64<<10))
	if err != nil {
		return 0, "", err
	}
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return 0, "", err
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

// UploadSnapshotChunks checks the source again before a backend publishes its
// manifest. A truncated or changed spool cannot create a valid-looking index.
func UploadSnapshotChunks(ctx context.Context, source io.Reader, size int64, digest string, put func(int, []byte) error) (int, error) {
	buffer := make([]byte, SnapshotChunkBytes)
	h := sha256.New()
	var total int64
	chunks := 0
	for {
		n, err := io.ReadFull(snapshotContextReader{ctx, source}, buffer)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return 0, err
		}
		if n == 0 && chunks != 0 {
			break
		}
		if e := put(chunks, buffer[:n]); e != nil {
			return 0, e
		}
		_, _ = h.Write(buffer[:n])
		total += int64(n)
		chunks++
		if err != nil {
			break
		}
	}
	if total != size || hex.EncodeToString(h.Sum(nil)) != digest {
		return 0, ErrInvalidSnapshot
	}
	return chunks, nil
}

type snapshotContextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r snapshotContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// SnapshotSink verifies a download without accumulating it in Go memory.
type SnapshotSink struct {
	file   *os.File
	hash   hash.Hash
	chunks int
	size   int64
}

func NewSnapshotSink(dir string) (*SnapshotSink, error) {
	f, err := os.CreateTemp(dir, "satellite-snapshot-*")
	if err != nil {
		return nil, err
	}
	return &SnapshotSink{file: f, hash: sha256.New()}, nil
}

func (s *SnapshotSink) Append(ctx context.Context, chunk int, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if chunk != s.chunks || len(data) > SnapshotChunkBytes {
		return ErrInvalidSnapshot
	}
	n, err := s.file.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	_, _ = s.hash.Write(data)
	s.size += int64(n)
	s.chunks++
	return nil
}

func (s *SnapshotSink) Finish(chunks int, size int64, digest string) (io.ReadCloser, error) {
	if chunks < 1 || s.chunks != chunks || s.size != size || hex.EncodeToString(s.hash.Sum(nil)) != digest {
		return nil, ErrInvalidSnapshot
	}
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	f := s.file
	s.file = nil
	return &snapshotReader{File: f}, nil
}

func (s *SnapshotSink) Close() error {
	if s.file == nil {
		return nil
	}
	f := s.file
	s.file = nil
	err := f.Close()
	removeErr := os.Remove(f.Name())
	return errors.Join(err, removeErr)
}

type snapshotReader struct{ *os.File }

func (r *snapshotReader) Close() error {
	if r.File == nil {
		return nil
	}
	f := r.File
	r.File = nil
	return errors.Join(f.Close(), os.Remove(f.Name()))
}
