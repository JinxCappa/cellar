//! Metadata store abstraction and implementations for Cellar.
//!
//! This crate provides the control-plane data model:
//! - Upload sessions and chunk tracking
//! - Manifests and chunk reference counts
//! - Store path visibility and narinfo metadata
//! - Tokens, scopes, and revocation
//! - Signing keys and trusted builder keys
//! - Garbage collection state

pub mod error;
pub mod models;
pub mod postgres;
pub mod repos;
pub mod store;

pub use error::{MetadataError, MetadataResult};
pub use postgres::PostgresStore;
pub use store::{MetadataStore, SqliteStore};

use cellar_core::config::MetadataConfig;
use std::sync::Arc;

/// Create a metadata store from configuration.
pub async fn from_config(config: &MetadataConfig) -> MetadataResult<Arc<dyn MetadataStore>> {
    match config {
        MetadataConfig::Sqlite { path } => {
            let store = SqliteStore::new(path).await?;
            Ok(Arc::new(store))
        }
        MetadataConfig::Postgres { url, max_connections } => {
            let store = PostgresStore::new(url, *max_connections).await?;
            Ok(Arc::new(store))
        }
    }
}
