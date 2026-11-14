package domain

import (
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SourceStatusInfo struct {
	Source        string
	Status        string // StatusFresh, StatusStale or StatusUnavailable
	LastSuccessAt time.Time
	LastAttemptAt time.Time
	StaleSince    *time.Time
	ErrorMessage  string
}

func (s *SourceStatusInfo) CalculateFreshness(threshold time.Duration, hasCachedData bool) {
	now := time.Now().UTC()
	if s.LastSuccessAt.IsZero() {
		if hasCachedData {
			s.Status = StatusStale
			t := s.LastAttemptAt
			if t.IsZero() {
				t = now
			}
			s.StaleSince = &t
		} else {
			s.Status = StatusUnavailable
			s.StaleSince = nil
		}
		return
	}

	age := now.Sub(s.LastSuccessAt)
	if age > threshold || s.ErrorMessage != "" {
		if hasCachedData {
			s.Status = StatusStale
			if s.StaleSince == nil {
				st := s.LastSuccessAt.Add(threshold)
				s.StaleSince = &st
			}
		} else {
			s.Status = StatusUnavailable
			s.StaleSince = nil
		}
	} else {
		s.Status = StatusFresh
		s.StaleSince = nil
	}
}

func (s *SourceStatusInfo) ToProto() *commonv1.SourceFreshness {
	res := &commonv1.SourceFreshness{
		Source: SourceToProto(s.Source),
		Status: StatusToProto(s.Status),
	}

	if !s.LastSuccessAt.IsZero() {
		res.LastSuccessAt = timestamppb.New(s.LastSuccessAt)
	}
	if s.StaleSince != nil && !s.StaleSince.IsZero() {
		res.StaleSince = timestamppb.New(*s.StaleSince)
	}

	return res
}
