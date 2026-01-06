use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Credentials;
use bytes::Bytes;
use cellar_storage::backends::s3::S3Backend;
use cellar_storage::traits::{ListingOptions, ListingResume, ObjectStore};
use futures::StreamExt;
use std::collections::HashSet;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, runners::AsyncRunner};

const MINIO_IMAGE: &str = "minio/minio";
const MINIO_TAG: &str = "RELEASE.2024-02-12T21-36-45Z";

fn should_skip_s3_tests() -> bool {
    std::env::var("SKIP_S3_TESTS").is_ok()
}

struct MinioContext {
    _container: ContainerAsync<GenericImage>,
    endpoint: String,
    access_key: String,
    secret_key: String,
}

impl MinioContext {
    async fn new() -> Result<Self, String> {
        let access_key = "minio-access-key".to_string();
        let secret_key = "minio-secret-key".to_string();

        let container: ContainerAsync<GenericImage> = GenericImage::new(MINIO_IMAGE, MINIO_TAG)
            .with_exposed_port(9000.tcp())
            .with_wait_for(WaitFor::message_on_stdout("API:"))
            .with_env_var("MINIO_ROOT_USER", access_key.clone())
            .with_env_var("MINIO_ROOT_PASSWORD", secret_key.clone())
            .with_cmd(vec!["server", "/data"])
            .start()
            .await
            .map_err(|e| format!("failed to start MinIO container: {e}"))?;

        let host = container
            .get_host()
            .await
            .map_err(|e| format!("failed to get host: {e}"))?;
        let port = container
            .get_host_port_ipv4(9000.tcp())
            .await
            .map_err(|e| format!("failed to get port: {e}"))?;

        let endpoint = format!("http://{host}:{port}");

        Ok(Self {
            _container: container,
            endpoint,
            access_key,
            secret_key,
        })
    }

    async fn create_bucket(&self, bucket: &str) -> Result<(), String> {
        let credentials = Credentials::new(
            self.access_key.clone(),
            self.secret_key.clone(),
            None,
            None,
            "test",
        );
        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(aws_config::Region::new("us-east-1"))
            .credentials_provider(credentials)
            .http_client(aws_smithy_http_client::Builder::new().build_http())
            .endpoint_url(self.endpoint.clone())
            .force_path_style(true)
            .build();

        let client = Client::from_conf(config);
        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .map_err(|e| format!("failed to create bucket: {e}"))?;
        Ok(())
    }
}

struct S3TestHarness {
    _context: MinioContext,
    backend: S3Backend,
}

impl S3TestHarness {
    async fn new(prefix: Option<String>) -> Result<Self, String> {
        let context = MinioContext::new().await?;
        let bucket = "cellar-test";
        context.create_bucket(bucket).await?;

        let backend = S3Backend::new(
            bucket,
            Some(context.endpoint.clone()),
            Some("us-east-1".to_string()),
            prefix,
            Some(context.access_key.clone()),
            Some(context.secret_key.clone()),
            true,
        )
        .await
        .map_err(|e| format!("failed to create S3 backend: {e}"))?;

        Ok(Self {
            _context: context,
            backend,
        })
    }
}

#[tokio::test]
async fn test_s3_put_get_list_pages_resume() {
    if should_skip_s3_tests() {
        return;
    }

    let harness = match S3TestHarness::new(Some("prefix".to_string())).await {
        Ok(harness) => harness,
        Err(err) => {
            eprintln!("Skipping S3 test: {err}");
            return;
        }
    };
    let backend = &harness.backend;

    backend
        .put("items/a", Bytes::from_static(b"a"))
        .await
        .unwrap();
    backend
        .put("items/b", Bytes::from_static(b"b"))
        .await
        .unwrap();
    backend
        .put("items/c", Bytes::from_static(b"c"))
        .await
        .unwrap();

    let mut stream = backend.list_pages("items/", ListingOptions::new(2), None);
    let first_page = stream.next().await.unwrap().unwrap();
    assert_eq!(first_page.keys.len(), 2);
    let token = first_page
        .next_token
        .expect("expected continuation token for first page");

    let mut keys: HashSet<String> = first_page.keys.into_iter().collect();
    let mut resume_stream = backend.list_pages(
        "items/",
        ListingOptions::new(2),
        Some(ListingResume::new(token)),
    );
    while let Some(page) = resume_stream.next().await {
        let page = page.unwrap();
        keys.extend(page.keys);
    }

    let expected: HashSet<String> = ["items/a", "items/b", "items/c"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(keys, expected);
}

#[tokio::test]
async fn test_s3_put_stream_and_get_range() {
    if should_skip_s3_tests() {
        return;
    }

    let harness = match S3TestHarness::new(None).await {
        Ok(harness) => harness,
        Err(err) => {
            eprintln!("Skipping S3 test: {err}");
            return;
        }
    };
    let backend = &harness.backend;
    let key = "streamed.bin";

    let data = vec![7u8; 6 * 1024 * 1024];
    let split_at = 3 * 1024 * 1024;

    let mut upload = backend.put_stream(key).await.unwrap();
    upload
        .write(Bytes::from(data[..split_at].to_vec()))
        .await
        .unwrap();
    upload
        .write(Bytes::from(data[split_at..].to_vec()))
        .await
        .unwrap();
    let total = upload.finish().await.unwrap();
    assert_eq!(total as usize, data.len());

    let range = backend.get_range(key, 0, 1024).await.unwrap();
    assert_eq!(range.len(), 1024);
    assert!(range.iter().all(|b| *b == 7));
}
