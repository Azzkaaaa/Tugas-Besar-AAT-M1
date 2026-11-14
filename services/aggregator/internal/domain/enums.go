package domain

import (
	"strings"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
)

// Canonical string values stored for hazards and source state. These are the only place the
// strings are spelled out; every other package uses the constants.
const (
	SourceBMKG  = "BMKG"
	SourcePVMBG = "PVMBG"

	HazardTypeSeismic  = "SEISMIC"
	HazardTypeVolcanic = "VOLCANIC"

	SeverityNormal  = "NORMAL"
	SeverityWaspada = "WASPADA"
	SeveritySiaga   = "SIAGA"
	SeverityAwas    = "AWAS"

	StatusFresh       = "FRESH"
	StatusStale       = "STALE"
	StatusUnavailable = "UNAVAILABLE"
)

// SourceKey identifies one ingest stream (its watermark and status).
type SourceKey string

const (
	SourceKeyBMKGSeismic   SourceKey = "bmkg_seismic"
	SourceKeyPVMBGVolcanic SourceKey = "pvmbg_volcanic"
)

// AllSources lists every source with the key of its ingest stream, in display order.
var AllSources = []struct {
	Source string
	Key    SourceKey
}{
	{SourceBMKG, SourceKeyBMKGSeismic},
	{SourcePVMBG, SourceKeyPVMBGVolcanic},
}

// SourceKeyFor returns the ingest stream of a source ("" when unknown).
func SourceKeyFor(source string) SourceKey {
	for _, s := range AllSources {
		if s.Source == source {
			return s.Key
		}
	}
	return ""
}

// SourceOf returns the source a key belongs to ("BMKG" for "bmkg_seismic").
func SourceOf(key SourceKey) string {
	head, _, _ := strings.Cut(string(key), "_")
	return strings.ToUpper(head)
}

func SourceToProto(s string) commonv1.Source {
	switch s {
	case SourceBMKG:
		return commonv1.Source_SOURCE_BMKG
	case SourcePVMBG:
		return commonv1.Source_SOURCE_PVMBG
	}
	return commonv1.Source_SOURCE_UNSPECIFIED
}

// SourceFromProto returns "" for UNSPECIFIED (no filter).
func SourceFromProto(s commonv1.Source) string {
	switch s {
	case commonv1.Source_SOURCE_BMKG:
		return SourceBMKG
	case commonv1.Source_SOURCE_PVMBG:
		return SourcePVMBG
	}
	return ""
}

func HazardTypeToProto(t string) commonv1.HazardType {
	switch t {
	case HazardTypeSeismic:
		return commonv1.HazardType_HAZARD_TYPE_SEISMIC
	case HazardTypeVolcanic:
		return commonv1.HazardType_HAZARD_TYPE_VOLCANIC
	}
	return commonv1.HazardType_HAZARD_TYPE_UNSPECIFIED
}

func HazardTypeFromProto(t commonv1.HazardType) string {
	switch t {
	case commonv1.HazardType_HAZARD_TYPE_SEISMIC:
		return HazardTypeSeismic
	case commonv1.HazardType_HAZARD_TYPE_VOLCANIC:
		return HazardTypeVolcanic
	}
	return ""
}

func SeverityToProto(s string) commonv1.Severity {
	switch s {
	case SeverityNormal:
		return commonv1.Severity_SEVERITY_NORMAL
	case SeverityWaspada:
		return commonv1.Severity_SEVERITY_WASPADA
	case SeveritySiaga:
		return commonv1.Severity_SEVERITY_SIAGA
	case SeverityAwas:
		return commonv1.Severity_SEVERITY_AWAS
	}
	return commonv1.Severity_SEVERITY_UNSPECIFIED
}

func SeverityFromProto(s commonv1.Severity) string {
	switch s {
	case commonv1.Severity_SEVERITY_NORMAL:
		return SeverityNormal
	case commonv1.Severity_SEVERITY_WASPADA:
		return SeverityWaspada
	case commonv1.Severity_SEVERITY_SIAGA:
		return SeveritySiaga
	case commonv1.Severity_SEVERITY_AWAS:
		return SeverityAwas
	}
	return ""
}

func StatusToProto(s string) commonv1.SourceStatus {
	switch s {
	case StatusFresh:
		return commonv1.SourceStatus_SOURCE_STATUS_FRESH
	case StatusStale:
		return commonv1.SourceStatus_SOURCE_STATUS_STALE
	case StatusUnavailable:
		return commonv1.SourceStatus_SOURCE_STATUS_UNAVAILABLE
	}
	return commonv1.SourceStatus_SOURCE_STATUS_UNSPECIFIED
}
