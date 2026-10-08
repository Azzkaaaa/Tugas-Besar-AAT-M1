use hmac::{Hmac, Mac};
use sha2::Sha256;

/// Accepted clock skew between Zitadel and this service, in seconds.
const TOLERANCE: u64 = 5 * 60;

/// Verifies `ZITADEL-Signature: t=<unix>,v1=<hex>` where v1 = HMAC-SHA256(key, "<t>." + body).
/// Several `v1` entries are allowed (key rotation); any one matching is enough.
pub fn verify(body: &[u8], header: Option<&str>, key: &str, now: u64) -> bool {
    let Some(header) = header else { return false };
    if key.is_empty() {
        return false;
    }
    let mut timestamp = None;
    let mut signatures = Vec::new();
    for part in header.split(',') {
        let mut pair = part.trim().splitn(2, '=');
        match (pair.next(), pair.next()) {
            (Some("t"), Some(value)) => timestamp = Some(value),
            (Some("v1"), Some(value)) => {
                if let Ok(bytes) = hex::decode(value) {
                    signatures.push(bytes);
                }
            }
            _ => {}
        }
    }
    let Some(timestamp) = timestamp else { return false };
    if timestamp.is_empty() || !timestamp.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    let Ok(seconds) = timestamp.parse::<u64>() else { return false };
    if seconds.abs_diff(now) > TOLERANCE {
        return false;
    }
    let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes()).expect("HMAC accepts any key length");
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(body);
    // verify_slice compares in constant time and rejects length mismatches.
    signatures.iter().any(|signature| mac.clone().verify_slice(signature).is_ok())
}

#[cfg(test)]
pub(crate) fn sign(body: &[u8], key: &str, timestamp: u64) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes()).unwrap();
    mac.update(format!("{timestamp}.").as_bytes());
    mac.update(body);
    format!("t={timestamp},v1={}", hex::encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_800_000_000;

    #[test]
    fn rejects_tampered_bodies_and_malformed_headers() {
        let body = br#"{"a":1}"#;
        let header = sign(body, "signing-test", NOW);
        assert!(verify(body, Some(&header), "signing-test", NOW));
        assert!(!verify(br#"{"a":2}"#, Some(&header), "signing-test", NOW));
        assert!(!verify(body, Some("v1=00"), "signing-test", NOW));
        assert!(!verify(body, None, "signing-test", NOW));
        assert!(!verify(body, Some(&header), "", NOW));
        assert!(!verify(body, Some(&sign(body, "wrong-key", NOW)), "signing-test", NOW));
    }

    #[test]
    fn enforces_timestamp_tolerance() {
        let body = b"{}";
        assert!(verify(body, Some(&sign(body, "k", NOW - 299)), "k", NOW));
        assert!(!verify(body, Some(&sign(body, "k", NOW - 600)), "k", NOW));
        assert!(!verify(body, Some(&sign(body, "k", NOW + 600)), "k", NOW));
        let header = sign(body, "k", NOW).replacen("t=", "t=+", 1);
        assert!(!verify(body, Some(&header), "k", NOW));
    }

    #[test]
    fn accepts_any_matching_v1_entry() {
        let body = b"{}";
        let valid = sign(body, "k", NOW);
        let v1 = valid.split_once(",v1=").unwrap().1;
        let header = format!("t={NOW},v1=zz,v1={},v1={v1}", "00".repeat(32));
        assert!(verify(body, Some(&header), "k", NOW));
    }
}
