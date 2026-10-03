package domain

import (
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SourceStatusInfo struct {
	Source        string
	Status        string // "FRESH", "STALE", "UNAVAILABLE"
	LastSuccessAt time.Time
	LastAttemptAt time.Time
	StaleSince    *time.Time
	ErrorMessage  string
}

func (s *SourceStatusInfo) CalculateFreshness(threshold time.Duration, hasCachedData bool) {
	now := time.Now().UTC()
	if s.LastSuccessAt.IsZero() {
		if hasCachedData {
			s.Status = "STALE"
			t := s.LastAttemptAt
			s.StaleSince = &t
		} else {
			s.Status = "UNAVAILABLE"
			s.StaleSince = nil
		}
		return
	}

	age := now.Sub(s.LastSuccessAt)
	if age > threshold || s.ErrorMessage != "" {
		if hasCachedData {
			s.Status = "STALE"
			if s.StaleSince == nil {
				st := s.LastSuccessAt.Add(threshold)
				s.StaleSince = &st
			}
		} else {
			s.Status = "UNAVAILABLE"
			s.StaleSince = nil
		}
	} else {
		s.Status = "FRESH"
		s.StaleSince = nil
	}
}

func (s *SourceStatusInfo) ToProto() *commonv1.SourceFreshness {
	sourceEnum := commonv1.Source_SOURCE_UNSPECIFIED
	switch s.Source {
	case "BMKG":
		sourceEnum = commonv1.Source_SOURCE_BMKG
	case "PVMBG":
		sourceEnum = commonv1.Source_SOURCE_PVMBG
	}

	statusEnum := commonv1.SourceStatus_SOURCE_STATUS_UNSPECIFIED
	switch s.Status {
	case "FRESH":
		statusEnum = commonv1.SourceStatus_SOURCE_STATUS_FRESH
	case "STALE":
		statusEnum = commonv1.SourceStatus_SOURCE_STATUS_STALE
	case "UNAVAILABLE":
		statusEnum = commonv1.SourceStatus_SOURCE_STATUS_UNAVAILABLE
	}

	res := &commonv1.SourceFreshness{
		Source: sourceEnum,
		Status: statusEnum,
	}

	if !s.LastSuccessAt.IsZero() {
		res.LastSuccessAt = timestamppb.New(s.LastSuccessAt)
	}
	if s.StaleSince != nil && !s.StaleSince.IsZero() {
		res.StaleSince = timestamppb.New(*s.StaleSince)
	}

	return res
}
