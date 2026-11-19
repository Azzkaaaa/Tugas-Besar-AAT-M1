package domain

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// FullProjectionSize counts the same wire representation as ToProjection(FULL)
// without constructing a second structpb tree for a large detection-key list.
func FullProjectionSize(h *HazardRecord) int {
	header := *h
	header.Attributes = nil
	event, _ := header.ToProto()
	event.Attributes = nil
	attrs, ok := structWireSize(h.Attributes)
	if !ok {
		attrs = 0 // ToProto replaces an unsupported attribute object with {}.
	}
	eventBytes := proto.Size(event) + protowire.SizeTag(11) + protowire.SizeBytes(attrs)
	return protowire.SizeTag(2) + protowire.SizeBytes(eventBytes)
}

func structWireSize(fields map[string]any) (int, bool) {
	size := 0
	for key, value := range fields {
		if !utf8.ValidString(key) {
			return 0, false
		}
		v, ok := valueWireSize(value)
		if !ok {
			return 0, false
		}
		entry := protowire.SizeTag(1) + protowire.SizeBytes(len(key)) + protowire.SizeTag(2) + protowire.SizeBytes(v)
		size += protowire.SizeTag(1) + protowire.SizeBytes(entry)
	}
	return size, true
}

func valueWireSize(value any) (int, bool) {
	switch v := value.(type) {
	case nil, bool:
		return 2, true // oneof null enum / boolean, including zero values
	case float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return 9, true // oneof fixed64 double, including zero
	case json.Number:
		_, err := strconv.ParseFloat(string(v), 64)
		return 9, err == nil
	case string:
		return 1 + protowire.SizeBytes(len(v)), utf8.ValidString(v)
	case []byte:
		return 1 + protowire.SizeBytes(base64.StdEncoding.EncodedLen(len(v))), true
	case map[string]any:
		size, ok := structWireSize(v)
		return 1 + protowire.SizeBytes(size), ok
	case []any:
		size := 0
		for _, item := range v {
			n, ok := valueWireSize(item)
			if !ok {
				return 0, false
			}
			size += 1 + protowire.SizeBytes(n)
		}
		return 1 + protowire.SizeBytes(size), true
	default:
		return 0, false
	}
}
