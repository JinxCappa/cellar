//! Core domain types and shared logic for Nix binary cache vNext.
//!
//! This crate defines the canonical data model used across all other crates:
//! - Store path identifiers and hashes
//! - Chunk metadata and hashing
//! - Manifest structure and hashing
//! - Upload session lifecycle
//! - Narinfo content and signatures
//! - Token scopes and authorization

pub mod chunk;
pub mod config;
pub mod error;
pub mod hash;
pub mod manifest;
pub mod narinfo;
pub mod storage_domain;
pub mod store_path;
pub mod token;
pub mod upload;

pub use chunk::{Chunk, ChunkHash, ChunkInfo};
pub use error::{Error, Result};
pub use hash::{ContentHash, NarHash, NarHasher};
pub use manifest::{Manifest, ManifestHash};
pub use narinfo::{NarInfo, Signature};
pub use storage_domain::{DEFAULT_STORAGE_DOMAIN_ID_U128, default_storage_domain_id};
pub use store_path::{StorePath, StorePathHash};
pub use token::{Token, TokenId, TokenScope};
pub use upload::{UploadId, UploadSession, UploadState};

/// Default chunk size: 16 MiB
pub const DEFAULT_CHUNK_SIZE: u64 = 16 * 1024 * 1024;

/// Maximum chunk size: 32 MiB
pub const MAX_CHUNK_SIZE: u64 = 32 * 1024 * 1024;

/// Minimum chunk size: 1 MiB
pub const MIN_CHUNK_SIZE: u64 = 1024 * 1024;
