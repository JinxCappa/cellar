//! Storage domain identifiers.

use uuid::Uuid;

/// Default storage domain ID as a 128-bit constant.
/// This is used when no cache-specific domain is configured.
pub const DEFAULT_STORAGE_DOMAIN_ID_U128: u128 = 0x00000000_0000_0000_0000_000000000001;

/// Get the default storage domain ID.
pub fn default_storage_domain_id() -> Uuid {
    Uuid::from_u128(DEFAULT_STORAGE_DOMAIN_ID_U128)
}
