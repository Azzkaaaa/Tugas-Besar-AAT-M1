// Package m2 implements the satellite and coastal ingest jobs. They share no HTTP
// clients, retry loops or leader leases with BMKG/PVMBG.
package m2

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	SatelliteURL, SatelliteToken, SatelliteStream       string
	CoastalURL, CoastalToken                            string
	AuditToken, HTTPAddress                             string
	Lateness, CoastalPoll, Timeout, StaleAfter, OpenFor time.Duration
	FailureThreshold, MaxBodyBytes, MaxDetections       int
	// Retention is the satellite correction horizon; SnapshotEvery is how many
	// committed inputs separate durable engine snapshots (0 disables them).
	Retention     time.Duration
	SnapshotEvery int
}

func LoadConfig() (Config, error) {
	c := Config{SatelliteURL: os.Getenv("SATELLITE_BASE_URL"), SatelliteToken: os.Getenv("SATELLITE_TOKEN"),
		SatelliteStream: "satellite-v1", CoastalURL: os.Getenv("PANTAI_BASE_URL"), CoastalToken: os.Getenv("PANTAI_AUTH_TOKEN"),
		AuditToken: os.Getenv("AUDIT_TOKEN"), HTTPAddress: os.Getenv("AGGREGATOR_M2_HTTP_ADDRESS"),
		Lateness: 24 * time.Hour, CoastalPoll: 500 * time.Millisecond, Timeout: 1500 * time.Millisecond,
		StaleAfter: 2 * time.Minute, OpenFor: 10 * time.Second, FailureThreshold: 3, MaxBodyBytes: 2 << 20, MaxDetections: 200000,
		Retention: DefaultRetention, SnapshotEvery: 2000}
	// P3's canonical name takes precedence over the earlier P4/P5 alias.
	if token := os.Getenv("AUDIT_API_TOKEN"); token != "" {
		c.AuditToken = token
	}
	if port := os.Getenv("AGGREGATOR_INTERNAL_HTTP_PORT"); port != "" {
		address := ":" + port
		if c.HTTPAddress != "" && c.HTTPAddress != address {
			return c, fmt.Errorf("configure one internal HTTP listener: AGGREGATOR_INTERNAL_HTTP_PORT and AGGREGATOR_M2_HTTP_ADDRESS disagree")
		}
		c.HTTPAddress = address
	}
	if v := os.Getenv("SATELLITE_STREAM_ID"); v != "" {
		c.SatelliteStream = v
	}
	for name, dst := range map[string]*time.Duration{"SATELLITE_ALLOWED_LATENESS": &c.Lateness, "PANTAI_POLL_INTERVAL": &c.CoastalPoll, "PANTAI_REQUEST_TIMEOUT": &c.Timeout, "PANTAI_STALE_AFTER": &c.StaleAfter, "PANTAI_BREAKER_OPEN_FOR": &c.OpenFor, "SATELLITE_STATE_RETENTION": &c.Retention} {
		if v := os.Getenv(name); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil || d <= 0 {
				return c, fmt.Errorf("%s must be a positive duration", name)
			}
			*dst = d
		}
	}
	for name, dst := range map[string]*int{"PANTAI_BREAKER_FAILURES": &c.FailureThreshold, "M2_MAX_BODY_BYTES": &c.MaxBodyBytes, "SATELLITE_MAX_DETECTIONS": &c.MaxDetections} {
		if v := os.Getenv(name); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 {
				return c, fmt.Errorf("%s must be positive", name)
			}
			*dst = n
		}
	}
	if v := os.Getenv("SATELLITE_SNAPSHOT_EVERY"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return c, fmt.Errorf("SATELLITE_SNAPSHOT_EVERY must be zero or positive")
		}
		c.SnapshotEvery = n
	}
	for _, p := range []struct{ label, base, token string }{{"satellite", c.SatelliteURL, c.SatelliteToken}, {"coastal", c.CoastalURL, c.CoastalToken}} {
		if p.base == "" {
			continue
		}
		u, err := url.Parse(p.base)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
			return c, fmt.Errorf("invalid %s base URL", p.label)
		}
		if strings.TrimSpace(p.token) == "" || strings.HasPrefix(p.token, "REPLACE_") {
			return c, fmt.Errorf("%s token is required when its URL is configured", p.label)
		}
	}
	if c.HTTPAddress != "" && len(c.AuditToken) < 16 {
		return c, fmt.Errorf("AUDIT_API_TOKEN (or AUDIT_TOKEN) must contain at least 16 characters when M2 HTTP is enabled")
	}
	return c, nil
}
