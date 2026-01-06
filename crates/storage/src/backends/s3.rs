//! S3-compatible storage backend using AWS SDK.

use crate::error::{StorageError, StorageResult};
use crate::traits::{ByteStream, KeyStream, ObjectMeta, ObjectStore, StreamingUpload};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::ProvideCredentials;
use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::provider::future::ProvideCredentials as ProvideCredentialsFuture;
use aws_sdk_s3::Client;
use aws_smithy_http_client::Builder as SmithyHttpClientBuilder;
use bytes::Bytes;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use std::time::Duration;
use tokio::sync::OnceCell;
use tokio_util::io::ReaderStream;
use tracing::instrument;

/// Maximum range size for get_range operations (128 MiB).
/// This prevents large memory allocations from user-controlled range requests.
const MAX_RANGE_SIZE: u64 = 128 * 1024 * 1024;

/// Minimum part size for S3 multipart uploads (5 MiB).
/// S3 requires all parts except the last to be at least 5 MB.
const MIN_PART_SIZE: usize = 5 * 1024 * 1024;

/// Maximum buffer size before spilling to temp file (64 MiB).
/// This prevents unbounded memory growth if the caller sends many small chunks.
const MAX_BUFFER_SIZE: usize = 64 * 1024 * 1024;

/// Marker included in lazy-credentials initialization errors so we can map them
/// to actionable storage config errors instead of generic S3 transport failures.
const CREDENTIALS_INIT_ERROR_MARKER: &str = "cellar-s3-lazy-credentials-init";
const CREDENTIALS_RESOLVE_ERROR_MARKER: &str = "cellar-s3-lazy-credentials-resolve";

/// Lazily initializes the AWS default credentials chain on first signed request.
///
/// This avoids constructor-time side effects (notably TLS/native-root initialization)
/// in environments where no root certificates are available.
#[derive(Debug)]
struct LazyDefaultCredentialsProvider {
    region: String,
    chain: OnceCell<aws_config::default_provider::credentials::DefaultCredentialsChain>,
}

impl LazyDefaultCredentialsProvider {
    fn new(region: String) -> Self {
        Self {
            region,
            chain: OnceCell::new(),
        }
    }

    async fn build_chain(
        &self,
    ) -> Result<aws_config::default_provider::credentials::DefaultCredentialsChain, CredentialsError>
    {
        let region = aws_config::Region::new(self.region.clone());

        tokio::task::spawn(async move {
            aws_config::default_provider::credentials::DefaultCredentialsChain::builder()
                .region(region)
                .build()
                .await
        })
        .await
        .map_err(|join_err| {
            CredentialsError::provider_error(format!(
                "{CREDENTIALS_INIT_ERROR_MARKER}: failed to initialize AWS default credential chain: {join_err}"
            ))
        })
    }

    async fn chain(
        &self,
    ) -> Result<&aws_config::default_provider::credentials::DefaultCredentialsChain, CredentialsError>
    {
        self.chain
            .get_or_try_init(|| async { self.build_chain().await })
            .await
    }

    async fn credentials(&self) -> aws_credential_types::provider::Result {
        let chain = self.chain().await?;
        chain.provide_credentials().await.map_err(|err| {
            CredentialsError::provider_error(format!(
                "{CREDENTIALS_RESOLVE_ERROR_MARKER}: default AWS credentials resolution failed: {err}"
            ))
        })
    }
}

impl ProvideCredentials for LazyDefaultCredentialsProvider {
    fn provide_credentials<'a>(&'a self) -> ProvideCredentialsFuture<'a>
    where
        Self: 'a,
    {
        ProvideCredentialsFuture::new(self.credentials())
    }
}

fn map_s3_operation_error<E>(err: aws_sdk_s3::error::SdkError<E>) -> StorageError
where
    E: std::error::Error + Send + Sync + 'static,
{
    let err_text = err.to_string();
    if err_text.contains(CREDENTIALS_INIT_ERROR_MARKER)
        || err_text.contains(CREDENTIALS_RESOLVE_ERROR_MARKER)
    {
        return StorageError::Config(
            "S3 credential initialization failed. Configure AWS credentials explicitly or ensure ambient AWS credentials and trust roots are available."
                .to_string(),
        );
    }

    StorageError::S3(Box::new(err))
}

/// S3-compatible object store using AWS SDK.
pub struct S3Backend {
    client: Client,
    bucket: String,
    prefix: Option<String>,
    /// Stored endpoint for backend identity (normalized).
    endpoint: String,
    /// Stored region for backend identity.
    region: String,
}

impl std::fmt::Debug for S3Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Backend")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .finish_non_exhaustive()
    }
}

impl S3Backend {
    /// Create a new S3 backend.
    ///
    /// # Arguments
    /// * `force_path_style` - Use path-style URLs (`endpoint/bucket/key`) instead of
    ///   virtual-hosted style (`bucket.endpoint/key`). Required for MinIO and some
    ///   S3-compatible services. AWS S3 (including VPC/FIPS/dual-stack endpoints)
    ///   requires virtual-hosted style (false).
    pub async fn new(
        bucket: &str,
        endpoint: Option<String>,
        region: Option<String>,
        prefix: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        force_path_style: bool,
    ) -> StorageResult<Self> {
        let has_access_key_id = access_key_id.is_some();
        let has_secret_access_key = secret_access_key.is_some();
        if has_access_key_id ^ has_secret_access_key {
            return Err(StorageError::Config(
                "s3 config requires both access_key_id and secret_access_key when either is set"
                    .to_string(),
            ));
        }

        // Build S3 client config directly and defer ambient credentials chain
        // initialization until first signed request.
        let resolved_region = region.unwrap_or_else(|| "us-east-1".to_string());
        let mut s3_config_builder = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(aws_config::Region::new(resolved_region.clone()));

        // Apply credentials: explicit config or ambient AWS credential chain
        if let (Some(key_id), Some(secret)) = (access_key_id, secret_access_key) {
            let credentials = aws_sdk_s3::config::Credentials::new(
                key_id,
                secret,
                None, // session token
                None, // expiration
                "cellar-config",
            );
            s3_config_builder = s3_config_builder.credentials_provider(credentials);
        } else {
            // Use a lazy provider so chain construction happens on first signed
            // request instead of backend construction.
            s3_config_builder = s3_config_builder
                .credentials_provider(LazyDefaultCredentialsProvider::new(resolved_region.clone()));
        }

        let normalized_endpoint = endpoint.as_ref().map(|endpoint_url| {
            // Handle bare host:port endpoints (e.g., "minio:9000") by prepending http://
            let endpoint_lower = endpoint_url.to_lowercase();
            if endpoint_lower.starts_with("http://") || endpoint_lower.starts_with("https://") {
                endpoint_url.clone()
            } else {
                format!("http://{}", endpoint_url)
            }
        });

        if let Some(endpoint_url) = &normalized_endpoint {
            s3_config_builder = s3_config_builder.endpoint_url(endpoint_url);

            // For explicit HTTP endpoints (e.g. local MinIO), use an HTTP-only client
            // so SDK initialization doesn't depend on native trust roots.
            if endpoint_url.to_ascii_lowercase().starts_with("http://") {
                s3_config_builder =
                    s3_config_builder.http_client(SmithyHttpClientBuilder::new().build_http());
            }
        }

        if force_path_style {
            s3_config_builder = s3_config_builder.force_path_style(true);
        }

        let client = Client::from_conf(s3_config_builder.build());

        // Store endpoint for backend identity. Use a canonical form for identity matching:
        // - If explicit endpoint provided, use normalized form
        // - Otherwise, use "s3.{region}.amazonaws.com" as the canonical AWS S3 endpoint
        let stored_endpoint = match &normalized_endpoint {
            Some(url) => url.clone(),
            None => format!("s3.{}.amazonaws.com", resolved_region),
        };

        // Normalize prefix: strip trailing slashes to avoid double-slash keys like "prefix//key"
        let normalized_prefix = prefix.map(|p| p.trim_end_matches('/').to_string());

        Ok(Self {
            client,
            bucket: bucket.to_string(),
            prefix: normalized_prefix,
            endpoint: stored_endpoint,
            region: resolved_region,
        })
    }

    /// Get the backend identity for token envelope validation.
    pub fn backend_identity(&self) -> crate::token::BackendIdentity {
        crate::token::BackendIdentity::S3 {
            endpoint: self.endpoint.clone(),
            region: self.region.clone(),
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
        }
    }

    /// Get the full object key for a key (applies prefix if configured).
    fn full_key(&self, key: &str) -> String {
        match &self.prefix {
            Some(prefix) => format!("{}/{}", prefix, key),
            None => key.to_string(),
        }
    }

    /// Strip the configured prefix from a full object key.
    /// Returns the key relative to the prefix, or the full key if no prefix is configured.
    fn strip_prefix(&self, full_key: &str) -> String {
        match &self.prefix {
            Some(prefix) => {
                let prefix_with_slash = format!("{}/", prefix);
                full_key
                    .strip_prefix(&prefix_with_slash)
                    .unwrap_or(full_key)
                    .to_string()
            }
            None => full_key.to_string(),
        }
    }

    /// Convert an AWS SDK error to StorageError, mapping NotFound appropriately.
    fn map_sdk_error<E>(err: aws_sdk_s3::error::SdkError<E>, key: &str) -> StorageError
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        // Check for NoSuchKey / NotFound in service error
        if let aws_sdk_s3::error::SdkError::ServiceError(ref service_err) = err {
            let raw = service_err.raw();
            if raw.status().as_u16() == 404 {
                return StorageError::NotFound(key.to_string());
            }
        }
        map_s3_operation_error(err)
    }
}

#[async_trait]
impl ObjectStore for S3Backend {
    #[instrument(skip(self), fields(backend = "s3"))]
    async fn exists(&self, key: &str) -> StorageResult<bool> {
        let full_key = self.full_key(key);
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(err) => {
                // Check for 404
                if let aws_sdk_s3::error::SdkError::ServiceError(ref service_err) = err
                    && service_err.raw().status().as_u16() == 404
                {
                    return Ok(false);
                }
                Err(map_s3_operation_error(err))
            }
        }
    }

    #[instrument(skip(self), fields(backend = "s3"))]
    async fn head(&self, key: &str) -> StorageResult<ObjectMeta> {
        let full_key = self.full_key(key);
        let output = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(|e| Self::map_sdk_error(e, key))?;

        // Convert AWS DateTime to time::OffsetDateTime
        let last_modified = output.last_modified().and_then(|dt| {
            time::OffsetDateTime::from_unix_timestamp(dt.secs())
                .inspect_err(|e| {
                    tracing::warn!(
                        key = %key,
                        timestamp = dt.secs(),
                        error = %e,
                        "Failed to convert S3 timestamp, object will be protected from GC"
                    );
                })
                .ok()
        });

        Ok(ObjectMeta {
            size: output.content_length().unwrap_or(0) as u64,
            last_modified,
            content_type: output.content_type().map(|s| s.to_string()),
        })
    }

    #[instrument(skip(self), fields(backend = "s3"))]
    async fn get(&self, key: &str) -> StorageResult<Bytes> {
        let full_key = self.full_key(key);
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(|e| Self::map_sdk_error(e, key))?;

        let bytes = output
            .body
            .collect()
            .await
            .map_err(|e| StorageError::S3(Box::new(e)))?
            .into_bytes();

        Ok(bytes)
    }

    #[instrument(skip(self), fields(backend = "s3"))]
    async fn get_stream(&self, key: &str) -> StorageResult<ByteStream> {
        let full_key = self.full_key(key);
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(|e| Self::map_sdk_error(e, key))?;

        // Convert AWS ByteStream to AsyncRead, then wrap with ReaderStream for true streaming
        let async_read = output.body.into_async_read();
        let reader_stream = ReaderStream::new(async_read);

        // Map the tokio_util stream items to our ByteStream format
        use futures::StreamExt;
        let stream = reader_stream.map(|result| result.map_err(StorageError::Io));

        Ok(Box::pin(stream))
    }

    #[instrument(skip(self), fields(backend = "s3"))]
    async fn get_range(&self, key: &str, start: u64, end: u64) -> StorageResult<Bytes> {
        // Validate range parameters
        if end < start {
            return Err(StorageError::InvalidRange(format!(
                "end ({}) < start ({})",
                end, start
            )));
        }

        // Empty range: return empty bytes immediately (consistent with filesystem backend)
        if end == start {
            return Ok(Bytes::new());
        }

        let range_size = end - start;
        if range_size > MAX_RANGE_SIZE {
            return Err(StorageError::InvalidRange(format!(
                "range size {} exceeds maximum {} bytes",
                range_size, MAX_RANGE_SIZE
            )));
        }

        // S3 Range header uses inclusive end, so subtract 1
        // Format: bytes=start-end (both inclusive)
        let range_header = format!("bytes={}-{}", start, end - 1);

        let full_key = self.full_key(key);
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .range(range_header)
            .send()
            .await
            .map_err(|e| Self::map_sdk_error(e, key))?;

        let bytes = output
            .body
            .collect()
            .await
            .map_err(|e| StorageError::S3(Box::new(e)))?
            .into_bytes();

        Ok(bytes)
    }

    #[instrument(skip(self, data), fields(backend = "s3", size = data.len()))]
    async fn put(&self, key: &str, data: Bytes) -> StorageResult<()> {
        let full_key = self.full_key(key);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .body(data.into())
            .send()
            .await
            .map_err(map_s3_operation_error)?;
        Ok(())
    }

    #[instrument(skip(self, data), fields(backend = "s3", size = data.len()))]
    async fn put_if_not_exists(&self, key: &str, data: Bytes) -> StorageResult<bool> {
        // Check if exists first (race condition possible but acceptable)
        if self.exists(key).await? {
            return Ok(false);
        }

        self.put(key, data).await?;
        Ok(true)
    }

    #[instrument(skip(self), fields(backend = "s3"))]
    async fn put_stream(&self, key: &str) -> StorageResult<Box<dyn StreamingUpload>> {
        let full_key = self.full_key(key);

        // Start multipart upload
        let create_output = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(map_s3_operation_error)?;

        let upload_id = create_output
            .upload_id()
            .ok_or_else(|| StorageError::Config("S3 did not return upload_id".to_string()))?
            .to_string();

        Ok(Box::new(S3Upload {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            key: full_key,
            upload_id,
            parts: Vec::new(),
            part_number: 1,
            bytes_written: 0,
            buffer: Vec::with_capacity(MIN_PART_SIZE),
            spill_file: None,
            spill_bytes: 0,
            spill_read_pos: 0,
        }))
    }

    #[instrument(skip(self), fields(backend = "s3"))]
    async fn delete(&self, key: &str) -> StorageResult<()> {
        let full_key = self.full_key(key);

        // Note: S3 delete_object doesn't error on missing keys by default,
        // so we do a head check first to return NotFound if needed
        if !self.exists(key).await? {
            return Err(StorageError::NotFound(key.to_string()));
        }

        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(map_s3_operation_error)?;

        Ok(())
    }

    #[instrument(skip(self), fields(backend = "s3"))]
    async fn list(&self, prefix: &str) -> StorageResult<Vec<String>> {
        let full_prefix = self.full_key(prefix);
        let mut results = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full_prefix);

            if let Some(token) = continuation_token.take() {
                request = request.continuation_token(token);
            }

            let output = request.send().await.map_err(map_s3_operation_error)?;

            for obj in output.contents() {
                if let Some(obj_key) = obj.key() {
                    let stripped = self.strip_prefix(obj_key);
                    results.push(stripped);
                }
            }

            if output.is_truncated() == Some(true) {
                continuation_token = output.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        Ok(results)
    }

    #[instrument(skip(self), fields(backend = "s3"))]
    async fn list_stream(&self, prefix: &str) -> StorageResult<KeyStream> {
        // Use async_stream for true streaming without full materialization
        let full_prefix = self.full_key(prefix);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let prefix_to_strip = self.prefix.clone();

        let stream = async_stream::try_stream! {
            let mut continuation_token: Option<String> = None;

            loop {
                let mut request = client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix(&full_prefix);

                if let Some(token) = continuation_token.take() {
                    request = request.continuation_token(token);
                }

                let output = request
                    .send()
                    .await
                    .map_err(map_s3_operation_error)?;

                for obj in output.contents() {
                    if let Some(obj_key) = obj.key() {
                        // Strip prefix inline
                        let stripped = match &prefix_to_strip {
                            Some(pfx) => {
                                let prefix_with_slash = format!("{}/", pfx);
                                obj_key
                                    .strip_prefix(&prefix_with_slash)
                                    .unwrap_or(obj_key)
                                    .to_string()
                            }
                            None => obj_key.to_string(),
                        };
                        yield stripped;
                    }
                }

                if output.is_truncated() == Some(true) {
                    continuation_token = output.next_continuation_token().map(|s| s.to_string());
                } else {
                    break;
                }
            }
        };

        Ok(Box::pin(stream))
    }

    #[instrument(skip(self), fields(backend = "s3"))]
    async fn copy(&self, from: &str, to: &str) -> StorageResult<()> {
        let from_full = self.full_key(from);
        let to_full = self.full_key(to);

        // CopySource format: bucket/key
        // The key portion must be URL-encoded for special characters (spaces, unicode, etc.)
        // We encode the key but not the bucket name or the slash separator
        let encoded_key = utf8_percent_encode(&from_full, NON_ALPHANUMERIC).to_string();
        let copy_source = format!("{}/{}", self.bucket, encoded_key);

        self.client
            .copy_object()
            .bucket(&self.bucket)
            .key(&to_full)
            .copy_source(&copy_source)
            .send()
            .await
            .map_err(|e| Self::map_sdk_error(e, from))?;

        Ok(())
    }

    fn backend_name(&self) -> &'static str {
        "s3"
    }

    #[instrument(skip(self), fields(backend = "s3"))]
    async fn health_check(&self) -> StorageResult<()> {
        const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(10);

        let marker_key = match &self.prefix {
            Some(prefix) => format!("{}/.cellar-health-check", prefix),
            None => ".cellar-health-check".to_string(),
        };

        let health_check_future = async {
            // Write a small marker object
            let marker_data = Bytes::from_static(b"health-check");
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&marker_key)
                .body(marker_data.into())
                .send()
                .await
                .map_err(map_s3_operation_error)?;

            // Delete the marker (ignore NotFound from race conditions)
            match self
                .client
                .delete_object()
                .bucket(&self.bucket)
                .key(&marker_key)
                .send()
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    // S3 delete doesn't typically error on missing, but handle it
                    if let aws_sdk_s3::error::SdkError::ServiceError(ref se) = e
                        && se.raw().status().as_u16() != 404
                    {
                        return Err(map_s3_operation_error(e));
                    }
                }
            }

            Ok(())
        };

        tokio::time::timeout(HEALTH_CHECK_TIMEOUT, health_check_future)
            .await
            .map_err(|_| {
                StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "S3 health check timed out after 10 seconds",
                ))
            })?
    }

    fn listing_capabilities(&self) -> crate::traits::ListingCapabilities {
        crate::traits::ListingCapabilities { resumable: true }
    }

    fn list_pages<'a>(
        &'a self,
        prefix: &str,
        options: crate::traits::ListingOptions,
        resume: Option<crate::traits::ListingResume>,
    ) -> std::pin::Pin<
        Box<dyn futures::Stream<Item = StorageResult<crate::traits::ListingPage>> + Send + 'a>,
    > {
        use crate::token::TokenEnvelope;

        let full_prefix = self.full_key(prefix);
        let page_size = options.normalized_page_size();
        let backend_identity = self.backend_identity();
        let prefix_owned = prefix.to_string();
        let options_for_stream = options.clone();

        // Validate and extract S3 continuation token from resume envelope
        let initial_token: Option<String> = match resume {
            Some(resume_opts) => {
                // Parse the TokenEnvelope
                let envelope = match TokenEnvelope::from_token(&resume_opts.start_token) {
                    Ok(e) => e,
                    Err(e) => return Box::pin(futures::stream::once(async move { Err(e) })),
                };

                // Validate the envelope matches current listing parameters
                if let Err(e) = envelope.validate(&backend_identity, prefix, &options) {
                    return Box::pin(futures::stream::once(async move { Err(e) }));
                }

                // Extract the S3-specific continuation token
                match String::from_utf8(envelope.provider_token().to_vec()) {
                    Ok(s3_token) => Some(s3_token),
                    Err(e) => {
                        let err = StorageError::InvalidContinuationToken(format!(
                            "invalid S3 continuation token encoding: {}",
                            e
                        ));
                        return Box::pin(futures::stream::once(async move { Err(err) }));
                    }
                }
            }
            None => None,
        };

        let stream = async_stream::try_stream! {
            let mut continuation_token: Option<String> = initial_token;

            loop {
                let mut request = self
                    .client
                    .list_objects_v2()
                    .bucket(&self.bucket)
                    .prefix(&full_prefix)
                    .max_keys(page_size as i32);

                if let Some(token) = continuation_token.take() {
                    request = request.continuation_token(token);
                }

                let output = request
                    .send()
                    .await
                    .map_err(|e| {
                        // Check for InvalidContinuationToken-like errors from S3
                        if let aws_sdk_s3::error::SdkError::ServiceError(ref service_err) = e {
                            let raw = service_err.raw();
                            // S3 returns 400 for invalid continuation tokens
                            if raw.status().as_u16() == 400 {
                                let body = format!("{:?}", service_err.err());
                                if body.contains("continuation") || body.contains("token") {
                                    return StorageError::InvalidContinuationToken(
                                        "S3 rejected continuation token".to_string()
                                    );
                                }
                            }
                        }
                        map_s3_operation_error(e)
                    })?;

                let mut keys = Vec::new();
                for obj in output.contents() {
                    if let Some(obj_key) = obj.key() {
                        keys.push(self.strip_prefix(obj_key));
                    }
                }

                let has_more = output.is_truncated() == Some(true);
                let next_s3_token = if has_more {
                    output.next_continuation_token().map(|s| s.to_string())
                } else {
                    None
                };

                // Wrap the S3 continuation token in a TokenEnvelope
                let next_token = match &next_s3_token {
                    Some(s3_token) => {
                        match TokenEnvelope::new(
                            backend_identity.clone(),
                            prefix_owned.clone(),
                            &options_for_stream,
                            s3_token.as_bytes().to_vec(),
                        ) {
                            Ok(envelope) => {
                                match envelope.to_token() {
                                    Ok(token) => Some(token),
                                    Err(e) => {
                                        // Token serialization failed - log and continue without resumability
                                        tracing::warn!(
                                            error = %e,
                                            "Failed to serialize continuation token, listing will not be resumable"
                                        );
                                        None
                                    }
                                }
                            }
                            Err(e) => {
                                // Token envelope creation failed (size limit exceeded) - log and continue
                                tracing::warn!(
                                    error = %e,
                                    "Continuation token too large, listing will not be resumable for this page"
                                );
                                None
                            }
                        }
                    }
                    None => None,
                };

                if !keys.is_empty() {
                    yield crate::traits::ListingPage {
                        keys,
                        next_token,
                    };
                }

                if !has_more {
                    break;
                }

                continuation_token = next_s3_token;
            }
        };

        Box::pin(stream)
    }
}

/// Streaming upload for S3 backend using multipart upload.
///
/// Buffers incoming data to meet S3's 5 MB minimum part size requirement.
/// If the buffer exceeds 64 MiB, data spills to a temporary file to bound memory usage.
struct S3Upload {
    client: Client,
    bucket: String,
    key: String,
    upload_id: String,
    parts: Vec<aws_sdk_s3::types::CompletedPart>,
    part_number: i32,
    bytes_written: u64,
    /// In-memory buffer for accumulating data until we reach MIN_PART_SIZE.
    buffer: Vec<u8>,
    /// Temporary file for spillover when buffer exceeds MAX_BUFFER_SIZE.
    spill_file: Option<tokio::fs::File>,
    /// Bytes written to spill file (tracked separately from buffer).
    spill_bytes: usize,
    /// Position of next byte to read from spill file (avoids O(n²) rewrites).
    spill_read_pos: usize,
}

impl S3Upload {
    /// Upload a single part to S3 and track it.
    async fn upload_part(&mut self, data: Bytes) -> StorageResult<()> {
        let upload_output = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .part_number(self.part_number)
            .body(data.into())
            .send()
            .await
            .map_err(map_s3_operation_error)?;

        let completed_part = aws_sdk_s3::types::CompletedPart::builder()
            .e_tag(upload_output.e_tag().unwrap_or_default())
            .part_number(self.part_number)
            .build();

        self.parts.push(completed_part);
        self.part_number += 1;

        Ok(())
    }
}

#[async_trait]
impl StreamingUpload for S3Upload {
    async fn write(&mut self, data: Bytes) -> StorageResult<()> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

        self.bytes_written += data.len() as u64;

        // If we have a spill file, write directly to it
        if let Some(ref mut file) = self.spill_file {
            file.write_all(&data).await.map_err(StorageError::Io)?;
            self.spill_bytes += data.len();
        } else {
            // Append to in-memory buffer
            self.buffer.extend_from_slice(&data);

            // Check if we need to spill to file
            if self.buffer.len() > MAX_BUFFER_SIZE {
                // Create temp file and spill current buffer
                let mut file =
                    tokio::fs::File::from_std(tempfile::tempfile().map_err(StorageError::Io)?);
                file.write_all(&self.buffer)
                    .await
                    .map_err(StorageError::Io)?;
                self.spill_bytes = self.buffer.len();
                self.buffer.clear();
                self.buffer.shrink_to_fit();
                self.spill_file = Some(file);
                tracing::debug!(
                    key = %self.key,
                    spill_bytes = self.spill_bytes,
                    "S3 upload spilled to temp file due to buffer overflow"
                );
            }
        }

        // Upload parts when we have enough unread data in spill file.
        // Track read position instead of rewriting the file (O(n) vs O(n²)).
        while self.spill_bytes - self.spill_read_pos >= MIN_PART_SIZE {
            let file = self.spill_file.as_mut().unwrap();
            file.seek(std::io::SeekFrom::Start(self.spill_read_pos as u64))
                .await
                .map_err(StorageError::Io)?;

            let mut part_data = vec![0u8; MIN_PART_SIZE];
            file.read_exact(&mut part_data)
                .await
                .map_err(StorageError::Io)?;

            // Advance read position instead of rewriting file
            self.spill_read_pos += MIN_PART_SIZE;

            // Upload the part
            self.upload_part(Bytes::from(part_data)).await?;
        }

        // Upload parts from buffer if we have enough
        while self.buffer.len() >= MIN_PART_SIZE && self.spill_file.is_none() {
            let part_data: Vec<u8> = self.buffer.drain(..MIN_PART_SIZE).collect();
            self.upload_part(Bytes::from(part_data)).await?;
        }

        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> StorageResult<u64> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        // Upload any remaining data from spill file + buffer as the last part
        // (last part can be any size, including < 5 MB)
        let mut final_data = Vec::new();

        if let Some(mut file) = self.spill_file.take() {
            // Read remaining unread data from spill file (from spill_read_pos to end)
            let remaining = self.spill_bytes - self.spill_read_pos;
            if remaining > 0 {
                file.seek(std::io::SeekFrom::Start(self.spill_read_pos as u64))
                    .await
                    .map_err(StorageError::Io)?;
                final_data.reserve(remaining);
                file.take(remaining as u64)
                    .read_to_end(&mut final_data)
                    .await
                    .map_err(StorageError::Io)?;
            }
        }

        // Append any remaining buffer data
        final_data.extend_from_slice(&self.buffer);

        // Upload final part if there's any data
        if !final_data.is_empty() {
            self.upload_part(Bytes::from(final_data)).await?;
        }

        // Handle zero-byte uploads: S3 multipart requires parts to be at least 1 byte,
        // so we abort the multipart upload and use PutObject instead for empty files.
        if self.parts.is_empty() {
            // Abort the multipart upload we started (best-effort cleanup).
            // If abort fails (transient network error), log and continue - the abort is just
            // cleanup for orphaned parts, and shouldn't prevent creating the actual file.
            if let Err(e) = self
                .client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .upload_id(&self.upload_id)
                .send()
                .await
            {
                tracing::warn!(
                    key = %self.key,
                    upload_id = %self.upload_id,
                    error = %e,
                    "Failed to abort multipart upload for zero-byte file, orphaned parts may remain"
                );
            }

            // Use simple PutObject for empty file
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&self.key)
                .body(Bytes::new().into())
                .send()
                .await
                .map_err(map_s3_operation_error)?;

            return Ok(self.bytes_written);
        }

        let completed_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(self.parts.clone()))
            .build();

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await
            .map_err(map_s3_operation_error)?;

        Ok(self.bytes_written)
    }

    async fn abort(self: Box<Self>) -> StorageResult<()> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .send()
            .await
            .map_err(map_s3_operation_error)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::{BackendIdentity, TokenEnvelope};
    use crate::traits::{ListingOptions, ListingResume};
    use futures::StreamExt;

    async fn make_backend(prefix: Option<String>) -> S3Backend {
        S3Backend::new(
            "test-bucket",
            Some("s3.test".to_string()),
            Some("us-east-1".to_string()),
            prefix,
            Some("access".to_string()),
            Some("secret".to_string()),
            true,
        )
        .await
        .expect("backend should construct for unit tests")
    }

    #[tokio::test]
    async fn test_full_key_and_strip_prefix() {
        let backend = make_backend(Some("prefix".to_string())).await;
        assert_eq!(backend.full_key("path/file"), "prefix/path/file");
        assert_eq!(backend.strip_prefix("prefix/path/file"), "path/file");
        assert_eq!(backend.strip_prefix("other/path"), "other/path");

        let backend = make_backend(None).await;
        assert_eq!(backend.full_key("path/file"), "path/file");
        assert_eq!(backend.strip_prefix("path/file"), "path/file");
    }

    #[tokio::test]
    async fn test_backend_identity_fields() {
        let backend = make_backend(Some("prefix".to_string())).await;
        match backend.backend_identity() {
            BackendIdentity::S3 {
                endpoint,
                region,
                bucket,
                prefix,
            } => {
                assert_eq!(endpoint, "http://s3.test");
                assert_eq!(region, "us-east-1");
                assert_eq!(bucket, "test-bucket");
                assert_eq!(prefix, Some("prefix".to_string()));
            }
            _ => panic!("expected s3 identity"),
        }
    }

    #[tokio::test]
    async fn test_list_pages_rejects_mismatched_token() {
        let backend = make_backend(None).await;
        let options = ListingOptions::new(200);
        let mismatched_identity = BackendIdentity::S3 {
            endpoint: "http://other".to_string(),
            region: "us-west-2".to_string(),
            bucket: "other-bucket".to_string(),
            prefix: None,
        };
        let envelope = TokenEnvelope::new(
            mismatched_identity,
            "items/".to_string(),
            &options,
            b"token".to_vec(),
        )
        .unwrap();
        let token = envelope.to_token().unwrap();

        let mut stream = backend.list_pages("items/", options, Some(ListingResume::new(token)));
        let result = stream.next().await.unwrap();
        assert!(matches!(
            result,
            Err(StorageError::InvalidContinuationToken(_))
        ));
    }

    #[tokio::test]
    async fn test_s3_new_requires_complete_credentials() {
        let err = S3Backend::new(
            "bucket",
            None,
            Some("us-east-1".to_string()),
            None,
            Some("access".to_string()),
            None,
            false,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, StorageError::Config(_)));
    }

    #[tokio::test]
    async fn test_s3_new_normalizes_endpoint() {
        let backend = S3Backend::new(
            "bucket",
            Some("minio:9000".to_string()),
            Some("us-east-1".to_string()),
            Some("prefix".to_string()),
            None,
            None,
            true,
        )
        .await
        .unwrap();

        match backend.backend_identity() {
            BackendIdentity::S3 {
                endpoint,
                region,
                bucket,
                prefix,
            } => {
                assert_eq!(endpoint, "http://minio:9000");
                assert_eq!(region, "us-east-1");
                assert_eq!(bucket, "bucket");
                assert_eq!(prefix.as_deref(), Some("prefix"));
            }
            other => panic!("unexpected identity: {other:?}"),
        }
    }
}
