package m2

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
)

// ReadSSECaptured spills oversized frames to disk. The archive receives exact
// wire bytes, including CRLF and delimiters. Only complete frames may advance.
func ReadSSECaptured(reader io.Reader, max int, consume func(sseFrame) error, archive func(sseFrame, io.Reader, error) error) error {
	b := bufio.NewReaderSize(reader, 64<<10)
	var raw, line []byte
	var file *os.File
	defer func() {
		if file != nil {
			file.Close()
			os.Remove(file.Name())
		}
	}()
	f := sseFrame{}
	oversized, longLine, invalidID := false, false, false
	reset := func() {
		if file != nil {
			file.Close()
			os.Remove(file.Name())
			file = nil
		}
		raw = nil
		line = nil
		f = sseFrame{}
		oversized = false
		longLine = false
		invalidID = false
	}
	write := func(fragment []byte) error {
		if file == nil && len(raw)+len(fragment) > max {
			oversized = true
			if archive == nil {
				return errors.New("sse_frame_size_exceeded")
			}
			var err error
			file, err = os.CreateTemp("", "sse-capture-*")
			if err != nil {
				return err
			}
			if _, err = file.Write(raw); err != nil {
				return err
			}
			raw = nil
		}
		if file != nil {
			_, err := file.Write(fragment)
			return err
		}
		raw = append(raw, fragment...)
		return nil
	}
	park := func(readErr error) error {
		if archive == nil {
			return nil
		}
		var captured io.Reader = bytes.NewReader(raw)
		if file != nil {
			if _, err := file.Seek(0, io.SeekStart); err != nil {
				return err
			}
			captured = file
		}
		return archive(f, captured, readErr)
	}
	for {
		fragment, err := b.ReadSlice('\n')
		if len(fragment) > 0 {
			if e := write(fragment); e != nil {
				return e
			}
			if !longLine {
				if len(line)+len(fragment) > max {
					longLine = true
				} else {
					line = append(line, fragment...)
				}
			}
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			if len(raw) > 0 || file != nil {
				if e := park(io.ErrUnexpectedEOF); e != nil {
					return e
				}
			}
			return err
		}
		text := strings.TrimSuffix(strings.TrimSuffix(string(line), "\n"), "\r")
		blank := !longLine && text == ""
		if blank {
			if oversized || invalidID {
				if e := park(nil); e != nil {
					return e
				}
				if invalidID {
					return errors.New("invalid_sse_id")
				}
				if f.ID != "" {
					f.Kind = "quarantined"
					f.Data = []byte(`{"reason":"sse_frame_size_exceeded"}`)
					if e := consume(f); e != nil {
						return e
					}
				}
			} else if len(f.Data) > 0 {
				if f.Kind == "" {
					f.Kind = "message"
				}
				f.Data = f.Data[:len(f.Data)-1]
				if e := consume(f); e != nil {
					return e
				}
			}
			reset()
			continue
		}
		if !longLine && !strings.HasPrefix(text, ":") {
			name, value, _ := strings.Cut(text, ":")
			value = strings.TrimPrefix(value, " ")
			switch name {
			case "id":
				if strings.ContainsRune(value, 0) {
					invalidID = true
				} else {
					f.ID = value
				}
			case "event":
				f.Kind = value
			case "data":
				if !oversized {
					f.Data = append(f.Data, []byte(value+"\n")...)
				}
			}
		}
		line = nil
		longLine = false
	}
}
