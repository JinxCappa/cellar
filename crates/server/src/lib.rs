//! HTTP API server for Nix binary cache vNext.
//!
//! This crate provides the HTTP control plane:
//! - Upload session management
//! - Chunk upload endpoints
//! - Upload commit with verification
//! - NAR download streaming
//! - Narinfo retrieval
//! - Admin endpoints (GC, tokens, keys)

pub mod auth;
pub mod bootstrap;
pub mod compression;
pub mod error;
pub mod handlers;
pub mod metrics;
pub mod ratelimit;
pub mod routes;
pub mod state;

pub use auth::TraceId;
pub use error::ApiError;
pub use ratelimit::{RateLimitState, TokenIdExtension};
pub use routes::create_router;
pub use state::AppState;
