use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bnpb_common::bnpb::common::v1::View;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::adapters::inbound::hazard::Filter;

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

/// `<epoch hex>.<sequence>.<HMAC>`. The MAC key is random per process, like the epoch, so a
/// restart invalidates every cursor; the epoch is compared first to report that as OUT_OF_RANGE.
pub struct CursorCodec {
    epoch: u64,
    key: [u8; 32],
}

impl CursorCodec {
    pub fn new(epoch: u64) -> Self {
        Self::with_key(epoch, rand::random())
    }

    pub fn with_key(epoch: u64, key: [u8; 32]) -> Self {
        Self { epoch, key }
    }

    pub fn encode(&self, sequence: u64, binding: &Binding) -> String {
        format!(
            "{:016x}.{sequence}.{}",
            self.epoch,
            URL_SAFE_NO_PAD.encode(self.mac(self.epoch, sequence, binding).finalize().into_bytes())
        )
    }

    pub fn decode(&self, cursor: &str, binding: &Binding) -> Result<u64, CursorError> {
        let mut parts = cursor.split('.');
        let (Some(epoch), Some(sequence), Some(mac), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
            return Err(CursorError::Invalid);
        };
        let epoch = u64::from_str_radix(epoch, 16).map_err(|_| CursorError::Invalid)?;
        let sequence: u64 = sequence.parse().map_err(|_| CursorError::Invalid)?;
        let mac = URL_SAFE_NO_PAD.decode(mac).map_err(|_| CursorError::Invalid)?;
        if epoch != self.epoch {
            return Err(CursorError::Expired);
        }
        self.mac(epoch, sequence, binding).verify_slice(&mac).map_err(|_| CursorError::Invalid)?;
        Ok(sequence)
    }

    fn mac(&self, epoch: u64, sequence: u64, binding: &Binding) -> HmacSha256 {
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC accepts any key length");
        let message =
            format!("v1\0{epoch}\0{sequence}\0{}\0{}\0{}", binding.subject, binding.view.as_str_name(), binding.filter.binding_key());
        mac.update(message.as_bytes());
        mac
    }
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
        let codec = CursorCodec::with_key(42, [1; 32]);
        let seismic = filter(HazardType::Seismic);
        let binding = Binding { subject: "user-1", view: View::Summary, filter: &seismic };
        let cursor = codec.encode(9, &binding);
        assert_eq!(codec.decode(&cursor, &binding), Ok(9));

        let other_user = Binding { subject: "user-2", ..binding };
        assert_eq!(codec.decode(&cursor, &other_user), Err(CursorError::Invalid));
        let full = Binding { view: View::Full, ..binding };
        assert_eq!(codec.decode(&cursor, &full), Err(CursorError::Invalid));
        let volcanic = filter(HazardType::Volcanic);
        assert_eq!(codec.decode(&cursor, &Binding { filter: &volcanic, ..binding }), Err(CursorError::Invalid));

        let tampered = cursor.replacen(".9.", ".8.", 1);
        assert_eq!(codec.decode(&tampered, &binding), Err(CursorError::Invalid));
        for garbage in ["", "abc", "1.2", "zz.1.AAAA", "1.x.AAAA", "1.2.3.4"] {
            assert_eq!(codec.decode(garbage, &binding), Err(CursorError::Invalid), "{garbage}");
        }
    }

    #[test]
    fn earlier_epoch_is_expired() {
        let seismic = filter(HazardType::Seismic);
        let binding = Binding { subject: "user-1", view: View::Summary, filter: &seismic };
        let cursor = CursorCodec::with_key(1, [1; 32]).encode(3, &binding);
        assert_eq!(CursorCodec::with_key(2, [2; 32]).decode(&cursor, &binding), Err(CursorError::Expired));
    }
}
