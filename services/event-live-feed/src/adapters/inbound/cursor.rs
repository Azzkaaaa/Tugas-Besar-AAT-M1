use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bnpb_common::bnpb::common::v1::View;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::adapters::inbound::hazard::Filter;
use crate::core::ports::StreamMeta;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorError {
    /// Malformed, forged, or issued for another client, view or filter: INVALID_ARGUMENT.
    Invalid,
    /// From an earlier stream epoch: OUT_OF_RANGE.
    Expired,
}

/// What a cursor is bound to besides its position.
#[derive(Clone, Copy)]
pub struct Binding<'a> {
    pub subject: &'a str,
    pub view: View,
    pub filter: &'a Filter,
}

/// `<epoch hex>.<sequence>.<HMAC>`, signed with the stream's cursor key. The key belongs to the
/// epoch (shared through the replay store, so any replica verifies it), so a reset invalidates
/// every cursor; the epoch is compared first to report that as OUT_OF_RANGE.
pub fn encode(meta: &StreamMeta, sequence: u64, binding: &Binding) -> String {
    format!("{:016x}.{sequence}.{}", meta.epoch, URL_SAFE_NO_PAD.encode(mac(meta, meta.epoch, sequence, binding).finalize().into_bytes()))
}

pub fn decode(meta: &StreamMeta, cursor: &str, binding: &Binding) -> Result<u64, CursorError> {
    let mut parts = cursor.split('.');
    let (Some(epoch), Some(sequence), Some(signature), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
        return Err(CursorError::Invalid);
    };
    let epoch = u64::from_str_radix(epoch, 16).map_err(|_| CursorError::Invalid)?;
    let sequence: u64 = sequence.parse().map_err(|_| CursorError::Invalid)?;
    let signature = URL_SAFE_NO_PAD.decode(signature).map_err(|_| CursorError::Invalid)?;
    if epoch != meta.epoch {
        return Err(CursorError::Expired);
    }
    mac(meta, epoch, sequence, binding).verify_slice(&signature).map_err(|_| CursorError::Invalid)?;
    Ok(sequence)
}

fn mac(meta: &StreamMeta, epoch: u64, sequence: u64, binding: &Binding) -> HmacSha256 {
    let mut mac = HmacSha256::new_from_slice(&meta.cursor_key).expect("HMAC accepts any key length");
    let message = format!("v1\0{epoch}\0{sequence}\0{}\0{}\0{}", binding.subject, binding.view.as_str_name(), binding.filter.binding_key());
    mac.update(message.as_bytes());
    mac
}

#[cfg(test)]
mod tests {
    use bnpb_common::bnpb::common::v1::{HazardFilter, HazardType};

    use super::*;

    fn filter(hazard_type: HazardType) -> Filter {
        Filter::parse(Some(HazardFilter { hazard_type: hazard_type as i32, ..Default::default() })).unwrap()
    }

    #[test]
    fn round_trip_and_bindings() {
        let meta = StreamMeta { epoch: 42, cursor_key: [1; 32] };
        let seismic = filter(HazardType::Seismic);
        let binding = Binding { subject: "user-1", view: View::Summary, filter: &seismic };
        let cursor = encode(&meta, 9, &binding);
        assert_eq!(decode(&meta, &cursor, &binding), Ok(9));

        let other_user = Binding { subject: "user-2", ..binding };
        assert_eq!(decode(&meta, &cursor, &other_user), Err(CursorError::Invalid));
        let full = Binding { view: View::Full, ..binding };
        assert_eq!(decode(&meta, &cursor, &full), Err(CursorError::Invalid));
        let volcanic = filter(HazardType::Volcanic);
        assert_eq!(decode(&meta, &cursor, &Binding { filter: &volcanic, ..binding }), Err(CursorError::Invalid));

        let tampered = cursor.replacen(".9.", ".8.", 1);
        assert_eq!(decode(&meta, &tampered, &binding), Err(CursorError::Invalid));
        for garbage in ["", "abc", "1.2", "zz.1.AAAA", "1.x.AAAA", "1.2.3.4"] {
            assert_eq!(decode(&meta, garbage, &binding), Err(CursorError::Invalid), "{garbage}");
        }
    }

    #[test]
    fn earlier_epoch_is_expired() {
        let seismic = filter(HazardType::Seismic);
        let binding = Binding { subject: "user-1", view: View::Summary, filter: &seismic };
        let cursor = encode(&StreamMeta { epoch: 1, cursor_key: [1; 32] }, 3, &binding);
        assert_eq!(decode(&StreamMeta { epoch: 2, cursor_key: [2; 32] }, &cursor, &binding), Err(CursorError::Expired));
    }

    #[test]
    fn same_epoch_with_another_key_is_invalid() {
        let seismic = filter(HazardType::Seismic);
        let binding = Binding { subject: "user-1", view: View::Summary, filter: &seismic };
        let cursor = encode(&StreamMeta { epoch: 1, cursor_key: [1; 32] }, 3, &binding);
        assert_eq!(decode(&StreamMeta { epoch: 1, cursor_key: [2; 32] }, &cursor, &binding), Err(CursorError::Invalid));
    }
}
