//! Narinfo generation and signing for Nix binary cache vNext.
//!
//! This crate provides:
//! - Ed25519 key generation and management
//! - Narinfo fingerprint signing
//! - Signature verification

pub mod error;
pub mod key;
pub mod signer;

pub use error::{SignerError, SignerResult};
pub use key::{KeyPair, PublicKey, SecretKey};
pub use signer::{NarInfoSigner, Signer};
