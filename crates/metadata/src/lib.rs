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
        MetadataConfig::Sqlite {
            path,
            query_timeout_secs,
        } => {
            let store = SqliteStore::new(path, *query_timeout_secs).await?;
            Ok(Arc::new(store) as Arc<dyn MetadataStore>)
        }
        MetadataConfig::Postgres {
            url,
            host,
            port,
            username,
            password,
            database,
            ssl_mode,
            max_connections,
            statement_timeout_ms,
        } => {
            let store = if let Some(url) = url {
                // URL takes precedence for backward compatibility
                tracing::info!("Connecting to PostgreSQL using connection URL");
                PostgresStore::from_url(url, *max_connections, *statement_timeout_ms).await?
            } else if let (Some(host), Some(database)) = (host.as_ref(), database.as_ref()) {
                // Use individual parameters
                PostgresStore::from_params(
                    host,
                    port.unwrap_or(5432),
                    username.as_deref(),
                    password.as_deref(),
                    database,
                    *ssl_mode,
                    *max_connections,
                    *statement_timeout_ms,
                )
                .await?
            } else {
                return Err(MetadataError::Config(
                    "postgres config requires either 'url' or 'host' + 'database'".to_string(),
                ));
            };
            Ok(Arc::new(store) as Arc<dyn MetadataStore>)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellar_core::config::MetadataConfig;

    #[tokio::test]
    async fn test_from_config_sqlite() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("metadata.db");
        let config = MetadataConfig::Sqlite {
            path: db_path.clone(),
            query_timeout_secs: None,
        };

        let store = from_config(&config).await.unwrap();
        store.health_check().await.unwrap();
        assert!(db_path.exists());
    }
}
