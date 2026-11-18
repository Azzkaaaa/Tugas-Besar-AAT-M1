package cassandra

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

type ringHost struct {
	id, node, dc, rack string
	tokens             []int64
	up                 bool
}
type ringEntry struct {
	token int64
	host  ringHost
}

// Select the natural replica set including unavailable hosts, rather than using
// the driver's coordinator selection (which intentionally skips down replicas).
func naturalReplicas(token int64, hosts []ringHost, options map[string]string) ([]ringHost, int, error) {
	ring := []ringEntry{}
	racks := map[string]map[string]bool{}
	for _, h := range hosts {
		for _, value := range h.tokens {
			ring = append(ring, ringEntry{value, h})
		}
		if racks[h.dc] == nil {
			racks[h.dc] = map[string]bool{}
		}
		racks[h.dc][h.rack] = true
	}
	if len(ring) == 0 {
		return nil, 0, fmt.Errorf("empty token ring")
	}
	sort.Slice(ring, func(i, j int) bool { return ring[i].token < ring[j].token })
	start := sort.Search(len(ring), func(i int) bool { return ring[i].token >= token })
	if start == len(ring) {
		start = 0
	}
	simple := strings.HasSuffix(options["class"], SimpleStrategy)
	if !simple && !strings.HasSuffix(options["class"], NetworkTopologyStrategy) {
		return nil, 0, fmt.Errorf("unsupported placement strategy")
	}
	factors := map[string]int{}
	expected := 0
	for key, value := range options {
		if key == "class" {
			continue
		}
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 {
			return nil, 0, fmt.Errorf("invalid replication factor")
		}
		factors[key] = n
		expected += n
	}
	out := []ringHost{}
	seenHost := map[string]bool{}
	count := map[string]int{}
	seenRack := map[string]map[string]bool{}
	skipped := map[string][]ringHost{}
	add := func(h ringHost) { out = append(out, h); count[h.dc]++ }
	for step := 0; step < len(ring) && len(out) < expected; step++ {
		h := ring[(start+step)%len(ring)].host
		if seenHost[h.id] {
			continue
		}
		seenHost[h.id] = true
		if simple {
			out = append(out, h)
			continue
		}
		rf := factors[h.dc]
		if count[h.dc] >= rf {
			continue
		}
		if seenRack[h.dc] == nil {
			seenRack[h.dc] = map[string]bool{}
		}
		if seenRack[h.dc][h.rack] && len(seenRack[h.dc]) < len(racks[h.dc]) {
			skipped[h.dc] = append(skipped[h.dc], h)
			continue
		}
		seenRack[h.dc][h.rack] = true
		add(h)
		if len(seenRack[h.dc]) == len(racks[h.dc]) {
			for _, prior := range skipped[h.dc] {
				if count[h.dc] < rf {
					add(prior)
				}
			}
			skipped[h.dc] = nil
		}
	}
	return out, expected, nil
}

func (s audit) Locate(ctx context.Context, id string) (storage.PlacementResult, error) {
	out := storage.PlacementResult{}
	h, err := (hazards{s.db}).Get(ctx, id)
	if err != nil {
		out.UnavailableUnits = []string{fmt.Sprintf("canonical-shard-%02d", auditShard(id))}
		return out, err
	}
	if h == nil || domain.Withdrawn(h) {
		return out, nil
	}
	out.Exists = true
	var token int64
	if err = s.db.session.Query(`SELECT token(hazard_id) FROM hazards_by_id WHERE hazard_id=?`, id).WithContext(ctx).Scan(&token); err != nil {
		out.UnavailableUnits = []string{"placement-token"}
		return out, err
	}
	options, err := keyspaceReplication(ctx, s.db.session, s.db.keyspace)
	if err != nil {
		out.UnavailableUnits = []string{"placement-replication"}
		return out, err
	}
	hosts := []ringHost{}
	for _, info := range s.db.session.GetHosts() {
		h := ringHost{id: info.HostID(), dc: info.DataCenter(), rack: info.Rack(), up: info.IsUp()}
		if s.db.nodeFromRack {
			h.node = h.rack
		}
		for _, text := range info.Tokens() {
			value, err := strconv.ParseInt(text, 10, 64)
			if err != nil {
				out.UnavailableUnits = append(out.UnavailableUnits, "placement-ring")
				continue
			}
			h.tokens = append(h.tokens, value)
		}
		hosts = append(hosts, h)
	}
	replicas, expected, err := naturalReplicas(token, hosts, options)
	if err != nil {
		out.UnavailableUnits = append(out.UnavailableUnits, "placement-ring")
		return out, err
	}
	if len(replicas) < expected {
		out.UnavailableUnits = append(out.UnavailableUnits, "placement-replica-count")
	}
	for i, h := range replicas {
		unit := "cassandra-" + h.id
		if !h.up {
			out.UnavailableUnits = append(out.UnavailableUnits, unit)
		}
		if h.node == "" {
			out.UnavailableUnits = append(out.UnavailableUnits, unit+"-node-identity")
			continue
		}
		role := "replica"
		if i == 0 {
			role = "primary"
		}
		out.Locations = append(out.Locations, storage.HazardLocation{Node: h.node, Unit: unit, Role: role})
	}
	out.UnavailableUnits = uniqueUnits(out.UnavailableUnits)
	return out, nil
}
