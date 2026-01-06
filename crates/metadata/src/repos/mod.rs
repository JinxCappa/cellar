//! Repository traits for metadata operations.

pub mod bootstrap;
pub mod caches;
pub mod chunks;
pub mod domains;
pub mod gc;
pub mod manifests;
pub mod store_paths;
pub mod tokens;
pub mod tombstones;
pub mod trusted_keys;
pub mod uploads;

pub use bootstrap::BootstrapRepo;
pub use caches::CacheRepo;
pub use chunks::ChunkRepo;
pub use domains::{DomainRepo, DomainUsage};
pub use gc::{GcRepo, ReachabilityRepo};
pub use manifests::ManifestRepo;
pub use store_paths::StorePathRepo;
pub use tokens::TokenRepo;
pub use tombstones::TombstoneRepo;
pub use trusted_keys::TrustedKeyRepo;
pub use uploads::UploadRepo;
