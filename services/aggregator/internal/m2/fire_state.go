package m2

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

// fireStateVersion changes whenever the encoded layout or the engine's
// deterministic semantics change; older snapshots are then ignored and state
// is rebuilt from the journal.
const fireStateVersion = 2

// fireState is the serialisable part of FireEngine. The spatial grid is rebuilt
// on restore; pairwise edges are derived on demand and never retained.
type fireState struct {
	Version       int                             `json:"version"`
	Lateness      time.Duration                   `json:"lateness"`
	Retention     time.Duration                   `json:"retention"`
	MaxDetections int                             `json:"max_detections"`
	Points        []Detection                     `json:"points"`
	Seen          map[string]time.Time            `json:"seen"`
	Owner         map[string]string               `json:"owner"`
	Groups        map[string][]string             `json:"groups"`
	Published     []string                        `json:"published"`
	Latest        map[string]*domain.HazardRecord `json:"latest"`
	MaxTime       time.Time                       `json:"max_time"`
	Compacted     time.Time                       `json:"compacted"`
	Late          int64                           `json:"late"`
	Low           int64                           `json:"low"`
	Expired       int64                           `json:"expired"`
	OverCapacity  int64                           `json:"over_capacity"`
}

// EncodeState returns a gzip-compressed, deterministic encoding of the engine.
// Restoring it and applying the remaining journal yields the same engine as a
// replay from sequence zero.
func (e *FireEngine) EncodeState() ([]byte, error) {
	s := fireState{Version: fireStateVersion, Lateness: e.Lateness, Retention: e.Retention, MaxDetections: e.MaxDetections,
		Seen: e.seen, Owner: e.owner, Groups: map[string][]string{}, Latest: e.latest, MaxTime: e.MaxTime, Compacted: e.compacted,
		Late: e.Late, Low: e.Low, Expired: e.Expired, OverCapacity: e.OverCapacity}
	for _, k := range sortedKeys(e.points) {
		s.Points = append(s.Points, e.points[k])
	}
	for id, members := range e.groups {
		s.Groups[id] = sortedKeys(members)
	}
	s.Published = sortedKeys(e.published)
	var buf bytes.Buffer
	z := gzip.NewWriter(&buf)
	if err := json.NewEncoder(z).Encode(s); err != nil {
		return nil, err
	}
	if err := z.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ErrIncompatibleState means the snapshot was produced by another engine
// version or configuration; callers must rebuild from the journal instead.
var ErrIncompatibleState = errors.New("incompatible_satellite_state")

// RestoreFireEngine rebuilds an engine from EncodeState output. The engine's
// configuration must match the snapshot, because lateness, retention and the
// soft bound all influence which state a full replay would hold.
func RestoreFireEngine(b []byte, lateness, retention time.Duration, maxPoints int) (*FireEngine, error) {
	z, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer z.Close()
	var s fireState
	if err = json.NewDecoder(io.LimitReader(z, 1<<31)).Decode(&s); err != nil {
		return nil, err
	}
	e := NewFireEngineRetention(lateness, retention, maxPoints)
	if s.Version != fireStateVersion || s.Lateness != e.Lateness || s.Retention != e.Retention || s.MaxDetections != e.MaxDetections {
		return nil, fmt.Errorf("%w: version %d lateness %s retention %s max %d", ErrIncompatibleState, s.Version, s.Lateness, s.Retention, s.MaxDetections)
	}
	for _, d := range s.Points {
		e.indexPoint(d)
	}
	if s.Seen != nil {
		e.seen = s.Seen
	}
	if s.Owner != nil {
		e.owner = s.Owner
	}
	for id, members := range s.Groups {
		g := keyset{}
		for _, k := range members {
			g[k] = true
		}
		e.groups[id] = g
	}
	for _, id := range s.Published {
		e.published[id] = true
	}
	if s.Latest != nil {
		e.latest = s.Latest
	}
	e.MaxTime, e.compacted = s.MaxTime, s.Compacted
	e.Late, e.Low, e.Expired, e.OverCapacity = s.Late, s.Low, s.Expired, s.OverCapacity
	return e, nil
}
