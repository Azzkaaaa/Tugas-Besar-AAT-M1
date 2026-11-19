package m2

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// Independent all-pairs connectivity checks the indexed graph after insertions,
// coordinate/confidence updates and retractions, including component splits.
func TestIndexedGraphMatchesPairwiseConnectivity(t *testing.T) {
	e := NewFireEngine(24*time.Hour, 1000)
	random := rand.New(rand.NewSource(19))
	for sequence := 1; sequence <= 200; sequence++ {
		kind := "detection"
		payload := detection(fmt.Sprintf("N%d", sequence), fmt.Sprint(600+sequence%50),
			fmt.Sprintf("%.4f", float64(random.Intn(12))*.006),
			fmt.Sprintf("%.4f", 100+float64(random.Intn(12))*.006), "n", 3)
		if sequence > 80 {
			keys := sortedKeys(e.points)
			key := keys[random.Intn(len(keys))]
			kind = "update"
			changes := map[string]any{"latitude": float64(random.Intn(12)) * .006,
				"longitude": 100 + float64(random.Intn(12))*.006, "confidence": "n"}
			if sequence%4 == 0 {
				changes["confidence"] = "l"
			}
			var err error
			if sequence%3 == 0 {
				kind = "retract"
				payload, err = json.Marshal(map[string]any{"key": key})
			} else {
				payload, err = json.Marshal(map[string]any{"key": key, "changes": changes})
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if _, err := e.Apply(kind, payload, int64(sequence), testNow); err != nil {
			t.Fatal(sequence, err)
		}
		visited := keyset{}
		var want []string
		for _, root := range sortedKeys(e.points) {
			if visited[root] || e.points[root].Confidence == "l" {
				continue
			}
			component := []string{root}
			visited[root] = true
			for head := 0; head < len(component); head++ {
				a := e.points[component[head]]
				for key, b := range e.points {
					if !visited[key] && b.Confidence != "l" && a.Time.Sub(b.Time).Abs() <= 6*time.Hour &&
						distance(a.Latitude, a.Longitude, b.Latitude, b.Longitude) <= 1 {
						visited[key] = true
						component = append(component, key)
					}
				}
			}
			sort.Strings(component)
			want = append(want, strings.Join(component, "\x00"))
		}
		var got []string
		for id, members := range e.groups {
			for key := range members {
				if e.owner[key] != id {
					t.Fatalf("input %d: inconsistent owner for %s", sequence, key)
				}
			}
			got = append(got, strings.Join(sortedKeys(members), "\x00"))
		}
		sort.Strings(want)
		sort.Strings(got)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("input %d (%s %s): component membership differs from all-pairs connectivity\ngot %q\nwant %q", sequence, kind, payload, got, want)
		}
	}
}
