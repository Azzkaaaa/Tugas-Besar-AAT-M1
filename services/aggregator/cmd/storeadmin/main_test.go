package main

import (
	"testing"
	"time"
)

func TestPrepareSettings(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	d, o, err := prepareSettings(env(nil))
	if err != nil || d != 2*time.Hour || o.Workers != 4 {
		t.Fatalf("defaults: %v %v %v", d, o.Workers, err)
	}
	d, o, err = prepareSettings(env(map[string]string{"STOREADMIN_PREPARE_TIMEOUT": "90m", "STOREADMIN_PREPARE_WORKERS": "16"}))
	if err != nil || d != 90*time.Minute || o.Workers != 16 {
		t.Fatalf("override: %v %v %v", d, o.Workers, err)
	}
	for _, bad := range []map[string]string{
		{"STOREADMIN_PREPARE_TIMEOUT": "soon"}, {"STOREADMIN_PREPARE_TIMEOUT": "-1m"},
		{"STOREADMIN_PREPARE_WORKERS": "0"}, {"STOREADMIN_PREPARE_WORKERS": "33"}, {"STOREADMIN_PREPARE_WORKERS": "x"},
	} {
		if _, _, err := prepareSettings(env(bad)); err == nil {
			t.Fatalf("accepted %v", bad)
		}
	}
}
