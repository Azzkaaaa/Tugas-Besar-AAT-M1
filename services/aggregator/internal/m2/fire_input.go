package m2

import (
	"encoding/json"
	"errors"
	"reflect"
	"time"
)

type pointLookup func(string) (Detection, bool, error)
type seenLookup func(string) (time.Time, bool, error)

type fireInput struct {
	key                  string
	point                Detection
	skip, advance        bool
	insertOnly, remember bool
}

// Policy is shared by the in-memory reference and the disk processor. Lookup
// failures are operational errors supplied by the caller, never poison input.
func prepareFireInput(e *FireEngine, kind string, raw []byte, active int, lookup pointLookup, seen seenLookup) (in fireInput, err error) {
	o, err := decodeObject(raw)
	if err != nil {
		return in, err
	}
	if kind == "heartbeat" {
		if at, err := timestamp(o, "virtual_now"); err == nil && at.After(e.MaxTime) {
			e.MaxTime, in.advance = at, true
		}
		in.skip = true
		return in, nil
	}
	in.key, _ = textField(o, "key")
	if in.key == "" {
		in.key, _ = textField(o, "detection_key")
	}
	switch kind {
	case "detection", "update":
		row := o
		if b := o["detection"]; len(b) > 0 {
			row, err = decodeObject(b)
			if err != nil {
				return in, err
			}
		}
		if b := o["changes"]; kind == "update" && len(b) > 0 {
			previous, known, err := lookup(in.key)
			if err != nil {
				return in, err
			}
			if !known {
				_, wasSeen, err := seen(in.key)
				if err != nil {
					return in, err
				}
				if !wasSeen && !e.compacted.IsZero() {
					e.Expired++
					return in, errors.New("correction_beyond_state_retention")
				}
				return in, errors.New("update_unknown_key")
			}
			changes, err := decodeObject(b)
			if err != nil {
				return in, err
			}
			row = map[string]json.RawMessage{}
			for key, value := range previous.Raw {
				row[key] = value
			}
			for key, value := range changes {
				row[key] = value
			}
		}
		in.point, err = parseDetection(row)
		if err != nil {
			return in, err
		}
		if in.key == "" {
			in.key = in.point.Key
		}
		if kind == "detection" && in.key != in.point.Key {
			return in, errors.New("detection_key_mismatch")
		}
		previous, known, err := lookup(in.key)
		if err != nil {
			return in, err
		}
		lastSeen, wasSeen, err := seen(in.key)
		if err != nil {
			return in, err
		}
		if kind == "detection" && wasSeen {
			in.skip = true
			return in, nil
		}
		if !known && in.point.Time.Before(e.MaxTime.Add(-e.Lateness)) {
			e.Late++
			return in, errors.New("late_beyond_allowed_lateness")
		}
		if !known && e.MaxDetections > 0 && active >= e.MaxDetections {
			e.OverCapacity++
		}
		in.remember = in.point.Time.After(lastSeen)
		if in.point.Time.After(e.MaxTime) {
			e.MaxTime, in.advance = in.point.Time, true
		}
		in.skip = known && in.point.Key == in.key && reflect.DeepEqual(previous, in.point)
		in.insertOnly = kind == "detection" && !known && in.point.Confidence != "l"
	case "retract":
		if in.key == "" {
			return in, errors.New("retract_missing_key")
		}
		_, known, err := lookup(in.key)
		if err != nil {
			return in, err
		}
		if !known {
			_, wasSeen, err := seen(in.key)
			if err != nil {
				return in, err
			}
			if !wasSeen && !e.compacted.IsZero() {
				e.Expired++
				return in, errors.New("retract_beyond_state_retention")
			}
			in.skip = true
		}
	default:
		return in, errors.New("unknown_stream_event")
	}
	return in, nil
}
