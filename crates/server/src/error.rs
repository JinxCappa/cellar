//! API error types.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

/// API error response.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// Error code for programmatic handling.
    pub code: String,
    /// Human-readable error message.
    pub message: String,
}

/// API error type.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("upload expired")]
    UploadExpired,

    #[error("hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },

    #[error("incomplete upload: missing {missing} chunks")]
    IncompleteUpload { missing: usize },

    #[error("internal error: {0}")]
    Internal(String),

    #[error("storage error: {0}")]
    Storage(#[from] cellar_storage::StorageError),

    #[error("metadata error: {0}")]
    Metadata(#[from] cellar_metadata::MetadataError),

    #[error("core error: {0}")]
    Core(#[from] cellar_core::Error),

    #[error("signer error: {0}")]
    Signer(#[from] cellar_signer::SignerError),
}

impl ApiError {
    /// Get the error code for this error.
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotFound(_) => "not_found",
            Self::BadRequest(_) => "bad_request",
            Self::Unauthorized(_) => "unauthorized",
            Self::Forbidden(_) => "forbidden",
            Self::Conflict(_) => "conflict",
            Self::UploadExpired => "upload_expired",
            Self::HashMismatch { .. } => "hash_mismatch",
            Self::IncompleteUpload { .. } => "incomplete_upload",
            Self::Internal(_) => "internal_error",
            Self::Storage(_) => "storage_error",
            Self::Metadata(_) => "metadata_error",
            Self::Core(_) => "core_error",
            Self::Signer(_) => "signer_error",
        }
    }

    /// Get the HTTP status code for this error.
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::UploadExpired => StatusCode::GONE,
            Self::HashMismatch { .. } => StatusCode::BAD_REQUEST,
            Self::IncompleteUpload { .. } => StatusCode::BAD_REQUEST,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Storage(e) => match e {
                cellar_storage::StorageError::NotFound(_) => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Metadata(e) => match e {
                cellar_metadata::MetadataError::NotFound(_) => StatusCode::NOT_FOUND,
                cellar_metadata::MetadataError::AlreadyExists(_) => StatusCode::CONFLICT,
                cellar_metadata::MetadataError::Constraint(_) => StatusCode::CONFLICT,
                cellar_metadata::MetadataError::MissingChunks { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Core(_) => StatusCode::BAD_REQUEST,
            Self::Signer(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let body = ErrorResponse {
            code: self.code().to_string(),
            message: self.to_string(),
        };
        (status, Json(body)).into_response()
    }
}

/// Result type for API handlers.
pub type ApiResult<T> = std::result::Result<T, ApiError>;
