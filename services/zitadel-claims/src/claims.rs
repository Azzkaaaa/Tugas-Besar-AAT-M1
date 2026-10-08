use serde_json::{Value, json};

use crate::config::Config;

/// Single source of the M1 permission matrix, in precedence order. Zitadel appends these
/// claims to userinfo, introspection and JWT access tokens through the Actions v2 target.
const SCOPES_BY_ROLE: [(&str, &[&str]); 3] = [
    ("BNPB_INTERNAL", &["hazards:read:summary", "hazards:read:full", "notifications:read", "webhooks:read", "webhooks:write"]),
    ("FIELD_TEAM", &["hazards:read:summary", "hazards:read:full", "notifications:read", "webhooks:read", "webhooks:write"]),
    ("MEDIA", &["hazards:read:summary", "notifications:read", "webhooks:read"]),
];
const ACTIVE_GRANT: i64 = 1; // domain.UserGrantStateActive

/// Builds the Actions v2 response for a preuserinfo/preaccesstoken function call.
/// Unexpected payload shapes yield `{}` (no claims) rather than an error, because the
/// target interrupts token issuance on error.
pub fn claims_for(payload: &Value, config: &Config) -> Value {
    let client = payload.pointer("/application/client_id").and_then(Value::as_str);
    // Executions are instance-wide; leave tokens of unrelated applications untouched.
    if !client.is_some_and(|client| config.client_ids.iter().any(|id| id == client)) {
        return json!({});
    }
    let grants = payload.get("user_grants").and_then(Value::as_array).map(Vec::as_slice).unwrap_or_default();
    let granted: Vec<&str> = grants
        .iter()
        .filter(|grant| {
            grant.get("project_id").and_then(Value::as_str) == Some(&config.project_id)
                && grant.get("resource_owner").and_then(Value::as_str) == Some(&config.organization_id)
                && grant.get("state").is_none_or(|state| state.as_i64() == Some(ACTIVE_GRANT))
        })
        .filter_map(|grant| grant.get("roles").and_then(Value::as_array))
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    match SCOPES_BY_ROLE.iter().find(|(role, _)| granted.contains(role)) {
        Some((role, scopes)) => json!({ "append_claims": [{ "key": "role", "value": role }, { "key": "scopes", "value": scopes }] }),
        None => json!({}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        Config {
            signing_key: "signing-test".into(),
            client_ids: vec!["bridge-test".into(), "api-test".into(), "gateway-test".into()],
            project_id: "project-test".into(),
            organization_id: "org-test".into(),
            port: 0,
        }
    }

    fn grant(roles: &[&str]) -> Value {
        json!({ "project_id": "project-test", "resource_owner": "org-test", "state": 1, "roles": roles })
    }

    fn with(mut grant: Value, key: &str, value: Value) -> Value {
        grant[key] = value;
        grant
    }

    fn call(client: &str, grants: Vec<Value>) -> Value {
        claims_for(&json!({ "application": { "client_id": client }, "user_grants": grants }), &config())
    }

    #[test]
    fn scoped_to_project_applications_configured_org_and_active_grants() {
        assert_eq!(call("console", vec![grant(&["BNPB_INTERNAL"])]), json!({}));
        assert_eq!(call("api-test", vec![with(grant(&["MEDIA"]), "project_id", json!("other"))]), json!({}));
        assert_eq!(call("api-test", vec![with(grant(&["MEDIA"]), "resource_owner", json!("other"))]), json!({}));
        assert_eq!(call("api-test", vec![with(grant(&["MEDIA"]), "state", json!(2))]), json!({}));
        assert_eq!(claims_for(&json!({ "application": { "client_id": "api-test" } }), &config()), json!({}));
        assert_eq!(claims_for(&json!({}), &config()), json!({}));
        assert_eq!(call("api-test", vec![with(grant(&[]), "roles", json!("MEDIA"))]), json!({}));
        assert_eq!(call("api-test", vec![grant(&["UNKNOWN"])]), json!({}));
    }

    #[test]
    fn highest_role_wins_and_absent_state_counts_as_active() {
        let both = call("api-test", vec![grant(&["MEDIA"]), grant(&["FIELD_TEAM", "BNPB_INTERNAL"])]);
        assert_eq!(both["append_claims"][0]["value"], "BNPB_INTERNAL");
        let stateless = grant(&["MEDIA"]).as_object().unwrap().clone().into_iter().filter(|(key, _)| key != "state").collect();
        assert_eq!(
            call("gateway-test", vec![Value::Object(stateless)]),
            json!({ "append_claims": [
                { "key": "role", "value": "MEDIA" },
                { "key": "scopes", "value": ["hazards:read:summary", "notifications:read", "webhooks:read"] },
            ] })
        );
    }
}
