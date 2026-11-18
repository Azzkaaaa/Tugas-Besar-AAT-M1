package m2

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

const earthKM = 6371.0088

type cell [4]int64
type keyset map[string]bool

// FireEngine keeps a dynamic graph, indexed in 1 km ECEF cubes and 6 h time
// cells. An update/retraction only traverses the affected connected components.
// Durable input replay recreates the same graph AND identifier lineage.
type FireEngine struct {
	points    map[string]Detection
	seen      map[string]time.Time
	grid      map[cell]keyset
	edges     map[string]keyset
	owner     map[string]string
	groups    map[string]keyset
	published map[string]bool
	latest    map[string]*domain.HazardRecord
	MaxTime   time.Time
	Lateness  time.Duration
	// Retention is the declared correction horizon: once a component's newest
	// detection is older than MaxTime-Retention it is final and leaves memory.
	// It is never shorter than Lateness+linkWindow, so no accepted detection can
	// still connect to an evicted point.
	Retention time.Duration
	// MaxDetections is a soft pressure threshold. It never changes the declared
	// correction horizon: capacity pressure cannot make otherwise valid input late.
	MaxDetections int
	// Late counts detections beyond Lateness; Expired counts corrections or
	// retractions of state already finalized; OverCapacity counts inputs that
	// were accepted while the soft bound could not be restored.
	Late, Low, Expired, OverCapacity int64
	// compacted is the last finalization cutoff (aligned to compactStep).
	compacted time.Time
	// KeepFinal retains finalized fire records for offline reference comparison.
	KeepFinal bool
	final     map[string]*domain.HazardRecord
}

// linkWindow is the maximum time distance of a graph edge.
const linkWindow = 6 * time.Hour

// compactStep aligns finalization cutoffs so replay from any snapshot or from
// sequence zero evicts exactly the same state at the same input.
const compactStep = time.Hour

// DefaultRetention is the declared correction horizon used when none is set.
const DefaultRetention = 48 * time.Hour

func NewFireEngine(lateness time.Duration, maxPoints int) *FireEngine {
	return NewFireEngineRetention(lateness, DefaultRetention, maxPoints)
}

// NewFireEngineRetention clamps retention to the smallest horizon that keeps
// finalization invisible to every detection the lateness policy still accepts.
func NewFireEngineRetention(lateness, retention time.Duration, maxPoints int) *FireEngine {
	if retention < lateness+linkWindow {
		retention = lateness + linkWindow
	}
	return &FireEngine{
		points: map[string]Detection{}, seen: map[string]time.Time{}, grid: map[cell]keyset{}, edges: map[string]keyset{}, owner: map[string]string{},
		groups: map[string]keyset{}, published: map[string]bool{}, latest: map[string]*domain.HazardRecord{}, Lateness: lateness, Retention: retention,
		MaxDetections: maxPoints, final: map[string]*domain.HazardRecord{},
	}
}
func stableID(prefix, key string) string {
	h := sha256.Sum256([]byte(key))
	return prefix + hex.EncodeToString(h[:16])
}
func sortedKeys[T any](m map[string]T) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
func position(d Detection) cell {
	lat, lon := d.Latitude*math.Pi/180, d.Longitude*math.Pi/180
	return cell{int64(math.Floor(earthKM * math.Cos(lat) * math.Cos(lon))), int64(math.Floor(earthKM * math.Cos(lat) * math.Sin(lon))), int64(math.Floor(earthKM * math.Sin(lat))), d.Time.Unix() / 21600}
}
func distance(lat1, lon1, lat2, lon2 float64) float64 {
	a, b := (lat2-lat1)*math.Pi/180, (lon2-lon1)*math.Pi/180
	h := math.Sin(a/2)*math.Sin(a/2) + math.Cos(lat1*math.Pi/180)*math.Cos(lat2*math.Pi/180)*math.Sin(b/2)*math.Sin(b/2)
	return 2 * earthKM * math.Asin(math.Sqrt(math.Min(1, h)))
}
func (e *FireEngine) touch(key string, affected keyset) {
	if id := e.owner[key]; id != "" {
		for k := range e.groups[id] {
			affected[k] = true
		}
	}
}
func (e *FireEngine) remove(key string) {
	d, ok := e.points[key]
	if !ok {
		return
	}
	if d.Confidence != "l" {
		c := position(d)
		delete(e.grid[c], key)
		if len(e.grid[c]) == 0 {
			delete(e.grid, c)
		}
		for k := range e.edges[key] {
			delete(e.edges[k], key)
		}
		delete(e.edges, key)
	}
	delete(e.points, key)
}
func (e *FireEngine) add(d Detection, affected keyset) {
	e.points[d.Key] = d
	if d.Confidence == "l" {
		return
	}
	c := position(d)
	e.edges[d.Key] = keyset{}
	for x := int64(-1); x <= 1; x++ {
		for y := int64(-1); y <= 1; y++ {
			for z := int64(-1); z <= 1; z++ {
				for t := int64(-1); t <= 1; t++ {
					for k := range e.grid[cell{c[0] + x, c[1] + y, c[2] + z, c[3] + t}] {
						other := e.points[k]
						if d.Time.Sub(other.Time).Abs() <= 6*time.Hour && distance(d.Latitude, d.Longitude, other.Latitude, other.Longitude) <= 1 {
							e.touch(k, affected)
							e.edges[d.Key][k] = true
							e.edges[k][d.Key] = true
						}
					}
				}
			}
		}
	}
	if e.grid[c] == nil {
		e.grid[c] = keyset{}
	}
	e.grid[c][d.Key] = true
	affected[d.Key] = true
}

// Apply accepts flat VIIRS rows or {detection: row}, updates with a full row or
// {key, changes}, and retractions with {key} / {detection_key}. Unknown shapes
// return an error and are durably parked by the runner, never guessed silently.
func (e *FireEngine) Apply(kind string, raw []byte, sequence int64, received time.Time) ([]*domain.HazardRecord, error) {
	o, err := decodeObject(raw)
	if err != nil {
		return nil, err
	}
	if kind == "heartbeat" {
		if t, err := timestamp(o, "virtual_now"); err == nil && t.After(e.MaxTime) {
			e.MaxTime = t
			e.maybeCompact()
		}
		return nil, nil
	}
	key, _ := textField(o, "key")
	if key == "" {
		key, _ = textField(o, "detection_key")
	}
	var d Detection
	switch kind {
	case "detection", "update":
		row := o
		if b := o["detection"]; len(b) > 0 {
			row, err = decodeObject(b)
			if err != nil {
				return nil, err
			}
		}
		if b := o["changes"]; kind == "update" && len(b) > 0 {
			prev, ok := e.points[key]
			if !ok {
				if _, seen := e.seen[key]; !seen && !e.compacted.IsZero() {
					e.Expired++
					return nil, errors.New("correction_beyond_state_retention")
				}
				return nil, errors.New("update_unknown_key")
			}
			changes, err := decodeObject(b)
			if err != nil {
				return nil, err
			}
			row = map[string]json.RawMessage{}
			for k, v := range prev.Raw {
				row[k] = v
			}
			for k, v := range changes {
				row[k] = v
			}
		}
		d, err = parseDetection(row)
		if err != nil {
			return nil, err
		}
		if key == "" {
			key = d.Key
		}
		if kind == "detection" && key != d.Key {
			return nil, errors.New("detection_key_mismatch")
		}
		_, known := e.points[key]
		if _, dup := e.seen[key]; kind == "detection" && dup {
			return nil, nil
		}
		if !known && d.Time.Before(e.MaxTime.Add(-e.Lateness)) {
			e.Late++
			return nil, errors.New("late_beyond_allowed_lateness")
		}
		if !known && e.MaxDetections > 0 && len(e.points) >= e.MaxDetections {
			e.OverCapacity++
		}
		if d.Time.After(e.seen[key]) {
			e.seen[key] = d.Time
		}
		if d.Time.After(e.MaxTime) {
			e.MaxTime = d.Time
			defer e.maybeCompact()
		}
		if old, ok := e.points[key]; ok && d.Key == key && reflect.DeepEqual(old, d) {
			return nil, nil
		}
	case "retract":
		if key == "" {
			return nil, errors.New("retract_missing_key")
		}
		if _, ok := e.points[key]; !ok {
			if _, seen := e.seen[key]; !seen && !e.compacted.IsZero() {
				e.Expired++
				return nil, errors.New("retract_beyond_state_retention")
			}
			return nil, nil
		}
	default:
		return nil, errors.New("unknown_stream_event")
	}
	oldOwners := map[string]string{}
	affected := keyset{}
	e.touch(key, affected)
	// A coordinate correction may give a different detection key. Include both components.
	if kind != "retract" && d.Key != key {
		e.touch(d.Key, affected)
	}
	for k := range affected {
		oldOwners[k] = e.owner[k]
	}
	if old, ok := e.points[key]; ok && old.Confidence == "l" {
		e.Low--
	}
	e.remove(key)
	if kind != "retract" {
		if d.Key != key {
			if old, ok := e.points[d.Key]; ok && old.Confidence == "l" {
				e.Low--
			}
			e.remove(d.Key)
		}
		e.add(d, affected)
		if d.Confidence == "l" {
			e.Low++
		}
	}
	for k := range affected {
		if oldOwners[k] == "" {
			oldOwners[k] = e.owner[k]
		}
	}
	if d.Key != "" && oldOwners[d.Key] == "" {
		oldOwners[d.Key] = oldOwners[key]
	}
	oldIDs := keyset{}
	for _, id := range oldOwners {
		if id != "" {
			oldIDs[id] = true
			delete(e.groups, id)
		}
	}
	for k := range affected {
		delete(e.owner, k)
	}
	delete(e.owner, key)
	visited := keyset{}
	used := keyset{}
	newIDs := keyset{}
	for _, root := range sortedKeys(affected) {
		if visited[root] || e.edges[root] == nil {
			continue
		}
		members := keyset{root: true}
		todo := []string{root}
		visited[root] = true
		for len(todo) > 0 {
			k := todo[len(todo)-1]
			todo = todo[:len(todo)-1]
			for n := range e.edges[k] {
				if !visited[n] {
					visited[n] = true
					members[n] = true
					todo = append(todo, n)
				}
			}
		}
		candidates := keyset{}
		for k := range members {
			if id := oldOwners[k]; id != "" && !used[id] {
				candidates[id] = true
			}
		}
		ids := sortedKeys(candidates)
		sort.SliceStable(ids, func(i, j int) bool { return e.published[ids[i]] && !e.published[ids[j]] })
		id := ""
		if len(ids) > 0 {
			id = ids[0]
		} else {
			id = stableID("fire-", sortedKeys(members)[0])
		}
		// A split must not reuse an identifier already assigned to another component.
		if used[id] {
			id = stableID("fire-", sortedKeys(members)[0]+"|split|"+strconv.FormatInt(sequence, 10))
		}
		used[id] = true
		e.groups[id] = members
		newIDs[id] = true
		for k := range members {
			e.owner[k] = id
		}
	}
	changed := map[string]*domain.HazardRecord{}
	for id := range newIDs {
		members := e.groups[id]
		if len(members) < 2 {
			continue
		}
		h := e.record(id, members, received)
		previous := e.latest[id]
		if previous != nil {
			h.IngestedAt = previous.IngestedAt
		}
		if previous == nil || !sameFire(previous, h) {
			h.Attributes["stream_sequence"] = strconv.FormatInt(sequence, 10)
			changed[id] = h
			e.latest[id] = h
		}
		e.published[id] = true
	}
	for id := range oldIDs {
		if len(e.groups[id]) >= 2 {
			continue
		}
		previous := e.latest[id]
		if previous == nil || domain.Withdrawn(previous) {
			continue
		}
		h := *previous
		h.Attributes = map[string]any{"withdrawn": true, "detection_count": len(e.groups[id]), "stream_sequence": strconv.FormatInt(sequence, 10)}
		h.Severity = domain.SeverityNormal
		e.latest[id] = &h
		changed[id] = &h
	}
	out := []*domain.HazardRecord{}
	for _, id := range sortedKeys(changed) {
		out = append(out, changed[id])
	}
	return out, nil
}

// sameFire compares canonical JSON, so a record restored from a snapshot
// (whose attribute numbers/slices decode to generic types) equals the record a
// full replay would hold and never triggers a spurious revision.
func sameFire(a, b *domain.HazardRecord) bool {
	return reflect.DeepEqual(fireFingerprint(a), fireFingerprint(b))
}
func fireFingerprint(h *domain.HazardRecord) []byte {
	x := *h
	x.Attributes = map[string]any{}
	for k, v := range h.Attributes {
		if k != "stream_sequence" {
			x.Attributes[k] = v
		}
	}
	b, err := json.Marshal(x)
	if err != nil {
		return []byte(err.Error())
	}
	return b
}
func (e *FireEngine) record(id string, members keyset, received time.Time) *domain.HazardRecord {
	keys := sortedKeys(members)
	satellites := keyset{}
	var x, y, z, frp float64
	earliest := e.points[keys[0]].Time
	confidence := "n"
	for _, k := range keys {
		d := e.points[k]
		lat, lon := d.Latitude*math.Pi/180, d.Longitude*math.Pi/180
		x += math.Cos(lat) * math.Cos(lon)
		y += math.Cos(lat) * math.Sin(lon)
		z += math.Sin(lat)
		frp += d.FRP
		satellites[d.Satellite] = true
		if d.Time.Before(earliest) {
			earliest = d.Time
		}
		if d.Confidence == "h" {
			confidence = "h"
		}
	}
	lat, lon := math.Atan2(z, math.Hypot(x, y))*180/math.Pi, math.Atan2(y, x)*180/math.Pi
	severity := domain.SeverityWaspada
	if len(keys) >= 20 || frp >= 100 {
		severity = domain.SeverityAwas
	} else if len(keys) >= 6 || frp >= 30 {
		severity = domain.SeveritySiaga
	}
	return &domain.HazardRecord{Source: domain.SourceSatellite, SourceRefID: id, HazardType: domain.HazardTypeWildfire, Severity: severity, AreaName: nearestProvince(lat, lon), Latitude: lat, Longitude: lon, OccurredAt: earliest, IngestedAt: received, Attributes: map[string]any{"detection_count": len(keys), "satellites": sortedKeys(satellites), "max_confidence": confidence, "total_frp_mw": frp, "detection_keys": keys, "withdrawn": false}}
}
func (e *FireEngine) Counts() (fires, singletons int) {
	for _, g := range e.groups {
		if len(g) >= 2 {
			fires++
		} else {
			singletons++
		}
	}
	return
}

// Snapshot returns active fires in a stable order for offline comparison,
// including finalized fires when KeepFinal is set.
func (e *FireEngine) Snapshot() []*domain.HazardRecord {
	all := map[string]*domain.HazardRecord{}
	for id, h := range e.final {
		all[id] = h
	}
	for id, h := range e.latest {
		all[id] = h
	}
	out := []*domain.HazardRecord{}
	for _, id := range sortedKeys(all) {
		if h := all[id]; !domain.Withdrawn(h) {
			out = append(out, h)
		}
	}
	return out
}

// Active is the number of detections currently held in memory.
func (e *FireEngine) Active() int { return len(e.points) }

func (e *FireEngine) maybeCompact() {
	cutoff := e.MaxTime.Add(-e.Retention).Truncate(compactStep)
	if cutoff.After(e.compacted) {
		e.compactTo(cutoff)
	}
}

// compactTo finalizes every component whose newest detection is before cutoff.
// cutoff never exceeds MaxTime-Lateness-linkWindow, so later accepted
// detections cannot connect to anything evicted here.
func (e *FireEngine) compactTo(cutoff time.Time) {
	if limit := e.MaxTime.Add(-e.Lateness - linkWindow); cutoff.After(limit) {
		cutoff = limit
	}
	if !cutoff.After(e.compacted) {
		return
	}
	e.compacted = cutoff
	for _, id := range sortedKeys(e.groups) {
		members := e.groups[id]
		newest := time.Time{}
		for k := range members {
			if t := e.points[k].Time; t.After(newest) {
				newest = t
			}
		}
		if !newest.Before(cutoff) {
			continue
		}
		for k := range members {
			e.remove(k)
			delete(e.owner, k)
		}
		delete(e.groups, id)
		if h := e.latest[id]; h != nil && e.KeepFinal {
			e.final[id] = h
		}
		delete(e.latest, id)
		delete(e.published, id)
	}
	for k, d := range e.points {
		if d.Confidence == "l" && d.Time.Before(cutoff) {
			e.remove(k)
		}
	}
	for id, h := range e.latest {
		if _, live := e.groups[id]; !live && domain.Withdrawn(h) && h.OccurredAt.Before(cutoff) {
			delete(e.latest, id)
			delete(e.published, id)
		}
	}
	for k, t := range e.seen {
		if t.Before(cutoff) {
			delete(e.seen, k)
		}
	}
}
