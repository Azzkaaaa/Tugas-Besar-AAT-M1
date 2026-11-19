package handlers

import (
	"context"
	"io"
	"sync"
)

// streamBody joins the pump and its receiver before admission is returned.
// Closing only io.PipeReader would leave a Recv goroutine or a large pending
// frame alive when a generated response visitor stops writing.
type streamBody struct {
	reader *io.PipeReader
	cancel context.CancelFunc
	done   <-chan struct{}
	parent context.Context
	once   sync.Once
}

func newStreamBody(parent context.Context, reader *io.PipeReader, cancel context.CancelFunc, done <-chan struct{}) *streamBody {
	body := &streamBody{reader: reader, cancel: cancel, done: done, parent: parent}
	go func() {
		select {
		case <-parent.Done():
			_ = body.Close()
		case <-done:
		}
	}()
	return body
}

func (b *streamBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	if err != nil && b.parent.Err() != nil {
		return n, io.EOF
	}
	return n, err
}

func (b *streamBody) Close() error {
	b.once.Do(func() {
		b.cancel()
		_ = b.reader.Close()
		<-b.done
	})
	return nil
}
