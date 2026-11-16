package domain

import "strconv"

// StreamSequence is kept as a decimal string to avoid loss of int64 precision in JSON.
// Comparing inside the store's row lock/LWT prevents an old stream worker from
// overwriting a newer committed projection after failover.
func StreamSequence(h *HazardRecord) int64 {
	if h == nil {
		return 0
	}
	s, _ := h.Attributes["stream_sequence"].(string)
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func Withdrawn(h *HazardRecord) bool { v, _ := h.Attributes["withdrawn"].(bool); return v }
