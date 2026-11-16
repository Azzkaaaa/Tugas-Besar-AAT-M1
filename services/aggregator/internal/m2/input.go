package m2

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

func decodeObject(b []byte) (map[string]json.RawMessage, error) {
	if !utf8.Valid(b) {
		return nil, errors.New("invalid_utf8")
	}
	// Bound nesting before decoding; braces inside JSON strings do not count.
	depth := 0
	quoted, escape := false, false
	for _, c := range b {
		if quoted {
			if escape {
				escape = false
			} else if c == '\\' {
				escape = true
			} else if c == '"' {
				quoted = false
			}
			continue
		}
		if c == '"' {
			quoted = true
		}
		if c == '{' || c == '[' {
			depth++
			if depth > 32 {
				return nil, errors.New("json_depth_exceeded")
			}
		}
		if c == '}' || c == ']' {
			depth--
		}
	}
	var obj map[string]json.RawMessage
	d := json.NewDecoder(bytes.NewReader(b))
	if err := d.Decode(&obj); err != nil {
		return nil, fmt.Errorf("invalid_json: %w", err)
	}
	if obj == nil {
		return nil, errors.New("expected_object")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, errors.New("trailing_json")
	}
	return obj, nil
}
func textField(o map[string]json.RawMessage, k string) (string, error) {
	var s string
	if err := json.Unmarshal(o[k], &s); err != nil || s == "" || len(s) > 256 {
		return "", fmt.Errorf("invalid_string: %s", k)
	}
	return s, nil
}

// Numeric lexical forms are preserved for detection_keys, including trailing zeroes.
func lexical(o map[string]json.RawMessage, k string) (string, float64, error) {
	b, ok := o[k]
	if !ok {
		return "", 0, fmt.Errorf("missing_field: %s", k)
	}
	s := strings.TrimSpace(string(b))
	if strings.HasPrefix(s, "\"") {
		if err := json.Unmarshal(b, &s); err != nil {
			return "", 0, err
		}
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return "", 0, fmt.Errorf("invalid_number: %s", k)
	}
	return s, v, nil
}
func number(o map[string]json.RawMessage, k string) (float64, error) {
	if len(o[k]) == 0 || o[k][0] == '"' || bytes.Equal(o[k], []byte("null")) {
		return 0, fmt.Errorf("expected_number: %s", k)
	}
	_, v, e := lexical(o, k)
	return v, e
}
func timestamp(o map[string]json.RawMessage, k string) (time.Time, error) {
	s, err := textField(o, k)
	if err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return t, fmt.Errorf("invalid_time: %s", k)
	}
	return t.UTC(), nil
}
func boolean(o map[string]json.RawMessage, k string) (bool, error) {
	b := o[k]
	if !bytes.Equal(b, []byte("true")) && !bytes.Equal(b, []byte("false")) {
		return false, fmt.Errorf("expected_boolean: %s", k)
	}
	return bytes.Equal(b, []byte("true")), nil
}

type Detection struct {
	Key, Satellite, Confidence string
	Latitude, Longitude, FRP   float64
	Time                       time.Time
	Raw                        map[string]json.RawMessage
}

func parseDetection(o map[string]json.RawMessage) (Detection, error) {
	d := Detection{Raw: o}
	var err error
	if d.Satellite, err = textField(o, "satellite"); err != nil {
		return d, err
	}
	date, err := textField(o, "acq_date")
	if err != nil {
		return d, err
	}
	clock, n, err := lexical(o, "acq_time")
	if err != nil || n != math.Trunc(n) || n < 0 || n > 2359 || int(n)%100 >= 60 {
		return d, errors.New("invalid_acq_time")
	}
	d.Time, err = time.Parse("2006-01-02 1504", fmt.Sprintf("%s %04d", date, int(n)))
	if err != nil {
		return d, errors.New("invalid_acq_date")
	}
	lat, _, err := lexical(o, "latitude")
	if err != nil {
		return d, err
	}
	_, d.Latitude, _ = lexical(o, "latitude")
	lon, _, err := lexical(o, "longitude")
	if err != nil {
		return d, err
	}
	_, d.Longitude, _ = lexical(o, "longitude")
	if math.Abs(d.Latitude) > 90 || math.Abs(d.Longitude) > 180 {
		return d, errors.New("coordinates_out_of_range")
	}
	d.Key = strings.Join([]string{d.Satellite, date, clock, lat, lon}, "|")
	d.Confidence, err = textField(o, "confidence")
	if err != nil {
		return d, err
	}
	switch strings.ToLower(d.Confidence) {
	case "l", "low":
		d.Confidence = "l"
	case "n", "nominal":
		d.Confidence = "n"
	case "h", "high":
		d.Confidence = "h"
	default:
		return d, errors.New("invalid_confidence")
	}
	_, d.FRP, err = lexical(o, "frp")
	if err != nil || d.FRP < 0 {
		return d, errors.New("invalid_frp")
	}
	return d, nil
}
