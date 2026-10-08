use bnpb_common::bnpb::common::v1::hazard_projection::Data;
use bnpb_common::bnpb::common::v1::{HazardEvent, HazardFilter, HazardProjection, HazardSummary, HazardType, Severity, Source, View};
use prost_types::Timestamp;
use tonic::Status;

pub const SCOPE_SUMMARY: &str = "hazards:read:summary";
pub const SCOPE_FULL: &str = "hazards:read:full";

/// UNSPECIFIED resolves to SUMMARY before authorization; FULL is never silently downgraded.
pub fn resolve_view(raw: i32) -> Result<View, Status> {
    match View::try_from(raw) {
        Ok(View::Unspecified | View::Summary) => Ok(View::Summary),
        Ok(View::Full) => Ok(View::Full),
        Err(_) => Err(Status::invalid_argument("unknown view")),
    }
}

pub fn authorize(scopes: &[String], view: View) -> Result<(), Status> {
    let has = |scope: &str| scopes.iter().any(|granted| granted == scope);
    if !has(SCOPE_SUMMARY) {
        return Err(Status::permission_denied(format!("{SCOPE_SUMMARY} is required")));
    }
    if view == View::Full && !has(SCOPE_FULL) {
        return Err(Status::permission_denied(format!("view FULL requires {SCOPE_FULL}")));
    }
    Ok(())
}

/// Validated subscription filter. `None` fields match everything.
#[derive(Debug, Default)]
pub struct Filter {
    source: Option<Source>,
    hazard_type: Option<HazardType>,
    severity: Option<Severity>,
    since: Option<(i64, i32)>,
    until: Option<(i64, i32)>,
}

impl Filter {
    pub fn parse(filter: Option<HazardFilter>) -> Result<Self, Status> {
        let Some(filter) = filter else { return Ok(Self::default()) };
        let source = match Source::try_from(filter.source) {
            Ok(Source::Unspecified) => None,
            Ok(source) => Some(source),
            Err(_) => return Err(Status::invalid_argument("unknown filter.source")),
        };
        let hazard_type = match HazardType::try_from(filter.hazard_type) {
            Ok(HazardType::Unspecified) => None,
            Ok(hazard_type) => Some(hazard_type),
            Err(_) => return Err(Status::invalid_argument("unknown filter.hazard_type")),
        };
        let severity = match Severity::try_from(filter.severity) {
            Ok(Severity::Unspecified) => None,
            Ok(severity) => Some(severity),
            Err(_) => return Err(Status::invalid_argument("unknown filter.severity")),
        };
        if matches!(
            (source, hazard_type),
            (Some(Source::Bmkg), Some(HazardType::Volcanic)) | (Some(Source::Pvmbg), Some(HazardType::Seismic))
        ) {
            return Err(Status::invalid_argument("filter.source and filter.hazard_type cannot both match"));
        }
        let since = filter.since.as_ref().map(instant).transpose()?;
        let until = filter.until.as_ref().map(instant).transpose()?;
        if let (Some(since), Some(until)) = (since, until)
            && since >= until
        {
            return Err(Status::invalid_argument("filter.since must precede filter.until"));
        }
        Ok(Self { source, hazard_type, severity, since, until })
    }

    pub fn matches(&self, hazard: &HazardEvent) -> bool {
        let occurred = hazard.occurred_at.as_ref().map(|at| (at.seconds, at.nanos));
        self.source.is_none_or(|source| hazard.source == source as i32)
            && self.hazard_type.is_none_or(|hazard_type| hazard.hazard_type == hazard_type as i32)
            && self.severity.is_none_or(|severity| hazard.severity == severity as i32)
            && self.since.is_none_or(|since| occurred.is_some_and(|at| at >= since))
            && self.until.is_none_or(|until| occurred.is_some_and(|at| at < until))
    }

    /// Canonical form for cursor binding.
    pub fn binding_key(&self) -> String {
        let name = |value: Option<&'static str>| value.unwrap_or("*");
        let time = |value: Option<(i64, i32)>| value.map_or("*".into(), |(seconds, nanos)| format!("{seconds}.{nanos}"));
        format!(
            "{}|{}|{}|{}|{}",
            name(self.source.map(|v| v.as_str_name())),
            name(self.hazard_type.map(|v| v.as_str_name())),
            name(self.severity.map(|v| v.as_str_name())),
            time(self.since),
            time(self.until),
        )
    }
}

fn instant(timestamp: &Timestamp) -> Result<(i64, i32), Status> {
    if !(0..1_000_000_000).contains(&timestamp.nanos) {
        return Err(Status::invalid_argument("invalid filter timestamp"));
    }
    Ok((timestamp.seconds, timestamp.nanos))
}

/// Summary is an explicit allowlist; coordinates, source_ref_id and attributes never leave in it.
pub fn project(hazard: &HazardEvent, view: View) -> HazardProjection {
    let data = match view {
        View::Full => Data::Full(hazard.clone()),
        _ => Data::Summary(HazardSummary {
            hazard_id: hazard.hazard_id.clone(),
            source: hazard.source,
            hazard_type: hazard.hazard_type,
            severity: hazard.severity,
            area_name: hazard.area_name.clone(),
            occurred_at: hazard.occurred_at,
            ingested_at: hazard.ingested_at,
        }),
    };
    HazardProjection { data: Some(data) }
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::*;

    fn at(seconds: i64) -> Option<Timestamp> {
        Some(Timestamp { seconds, nanos: 0 })
    }

    fn hazard() -> HazardEvent {
        HazardEvent {
            hazard_id: "h1".into(),
            source: Source::Bmkg as i32,
            source_ref_id: "ref".into(),
            hazard_type: HazardType::Seismic as i32,
            severity: Severity::Siaga as i32,
            area_name: "Area".into(),
            latitude: Some(-6.2),
            longitude: Some(106.8),
            occurred_at: at(100),
            ingested_at: at(101),
            attributes: Some(Default::default()),
        }
    }

    fn scopes(list: &[&str]) -> Vec<String> {
        list.iter().map(|scope| scope.to_string()).collect()
    }

    #[test]
    fn views_and_scopes() {
        assert_eq!(resolve_view(0).unwrap(), View::Summary);
        assert_eq!(resolve_view(2).unwrap(), View::Full);
        assert_eq!(resolve_view(9).unwrap_err().code(), Code::InvalidArgument);
        let media = scopes(&[SCOPE_SUMMARY]);
        assert!(authorize(&media, View::Summary).is_ok());
        assert_eq!(authorize(&media, View::Full).unwrap_err().code(), Code::PermissionDenied);
        assert_eq!(authorize(&[], View::Summary).unwrap_err().code(), Code::PermissionDenied);
        assert!(authorize(&scopes(&[SCOPE_SUMMARY, SCOPE_FULL]), View::Full).is_ok());
    }

    #[test]
    fn filter_validation() {
        let parse = |filter: HazardFilter| Filter::parse(Some(filter)).map_err(|status| status.code());
        assert_eq!(parse(HazardFilter { source: 9, ..Default::default() }).unwrap_err(), Code::InvalidArgument);
        assert_eq!(parse(HazardFilter { severity: 9, ..Default::default() }).unwrap_err(), Code::InvalidArgument);
        let mismatched = HazardFilter { source: Source::Pvmbg as i32, hazard_type: HazardType::Seismic as i32, ..Default::default() };
        assert_eq!(parse(mismatched).unwrap_err(), Code::InvalidArgument);
        assert_eq!(parse(HazardFilter { since: at(5), until: at(5), ..Default::default() }).unwrap_err(), Code::InvalidArgument);
        let bad_nanos = Some(Timestamp { seconds: 1, nanos: -1 });
        assert_eq!(parse(HazardFilter { since: bad_nanos, ..Default::default() }).unwrap_err(), Code::InvalidArgument);
    }

    #[test]
    fn filter_matching() {
        let matches = |filter: HazardFilter| Filter::parse(Some(filter)).unwrap().matches(&hazard());
        assert!(Filter::parse(None).unwrap().matches(&hazard()));
        assert!(matches(HazardFilter { source: Source::Bmkg as i32, severity: Severity::Siaga as i32, ..Default::default() }));
        assert!(!matches(HazardFilter { hazard_type: HazardType::Volcanic as i32, ..Default::default() }));
        assert!(!matches(HazardFilter { severity: Severity::Awas as i32, ..Default::default() }));
        // since inclusive, until exclusive.
        assert!(matches(HazardFilter { since: at(100), until: at(101), ..Default::default() }));
        assert!(!matches(HazardFilter { until: at(100), ..Default::default() }));
    }

    #[test]
    fn summary_projection_is_an_allowlist() {
        let Some(Data::Summary(summary)) = project(&hazard(), View::Summary).data else { panic!("expected summary") };
        assert_eq!(
            summary,
            HazardSummary {
                hazard_id: "h1".into(),
                source: Source::Bmkg as i32,
                hazard_type: HazardType::Seismic as i32,
                severity: Severity::Siaga as i32,
                area_name: "Area".into(),
                occurred_at: at(100),
                ingested_at: at(101),
            }
        );
        assert_eq!(project(&hazard(), View::Full).data, Some(Data::Full(hazard())));
    }
}
