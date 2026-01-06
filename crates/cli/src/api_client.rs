use anyhow::{Context, Result};
use reqwest::Url;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[derive(Clone)]
pub struct ApiClient {
    http: reqwest::Client,
    base_url: Url,
    token: String,
}

impl ApiClient {
    pub fn new(base_url: &str, token: &str) -> Result<Self> {
        let base_url = Url::parse(base_url).context("invalid server URL")?;
        Ok(Self {
            http: reqwest::Client::new(),
            base_url,
            token: token.to_string(),
        })
    }

    fn url(&self, path: &str) -> Result<Url> {
        self.base_url.join(path).context("failed to build API URL")
    }

    async fn send_json<T: DeserializeOwned>(&self, req: reqwest::RequestBuilder) -> Result<T> {
        let response = req.bearer_auth(&self.token).send().await?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("API error ({}): {}", status, body);
        }
        Ok(serde_json::from_str(&body)?)
    }

    async fn send_empty(&self, req: reqwest::RequestBuilder) -> Result<()> {
        let response = req.bearer_auth(&self.token).send().await?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("API error ({}): {}", status, body);
        }
        Ok(())
    }

    pub async fn create_cache(&self, req: CreateCacheRequest) -> Result<CacheResponse> {
        let url = self.url("/v1/admin/caches")?;
        self.send_json(self.http.post(url).json(&req)).await
    }

    pub async fn list_caches(&self) -> Result<Vec<CacheResponse>> {
        let url = self.url("/v1/admin/caches")?;
        let response: ListCachesResponse = self.send_json(self.http.get(url)).await?;
        Ok(response.caches)
    }

    pub async fn get_cache(&self, id: &str) -> Result<CacheResponse> {
        let url = self.url(&format!("/v1/admin/caches/{id}"))?;
        self.send_json(self.http.get(url)).await
    }

    pub async fn get_cache_by_name(&self, name: &str) -> Result<CacheResponse> {
        let caches = self.list_caches().await?;
        caches
            .into_iter()
            .find(|c| c.cache_name == name)
            .ok_or_else(|| anyhow::anyhow!("cache not found: {name}"))
    }

    pub async fn update_cache(&self, id: &str, req: UpdateCacheRequest) -> Result<CacheResponse> {
        let url = self.url(&format!("/v1/admin/caches/{id}"))?;
        self.send_json(self.http.put(url).json(&req)).await
    }

    pub async fn delete_cache(&self, id: &str) -> Result<()> {
        let url = self.url(&format!("/v1/admin/caches/{id}"))?;
        self.send_empty(self.http.delete(url)).await
    }

    pub async fn create_domain(&self, req: CreateDomainRequest) -> Result<DomainResponse> {
        let url = self.url("/v1/admin/domains")?;
        self.send_json(self.http.post(url).json(&req)).await
    }

    pub async fn list_domains(&self) -> Result<Vec<DomainResponse>> {
        let url = self.url("/v1/admin/domains")?;
        let response: ListDomainsResponse = self.send_json(self.http.get(url)).await?;
        Ok(response.domains)
    }

    pub async fn get_domain(&self, id: &str) -> Result<DomainResponse> {
        let url = self.url(&format!("/v1/admin/domains/{id}"))?;
        self.send_json(self.http.get(url)).await
    }

    pub async fn get_domain_by_name(&self, name: &str) -> Result<DomainResponse> {
        let url = self.url(&format!("/v1/admin/domains/by-name/{name}"))?;
        self.send_json(self.http.get(url)).await
    }

    pub async fn update_domain(
        &self,
        id: &str,
        req: UpdateDomainRequest,
    ) -> Result<DomainResponse> {
        let url = self.url(&format!("/v1/admin/domains/{id}"))?;
        self.send_json(self.http.put(url).json(&req)).await
    }

    pub async fn delete_domain(&self, id: &str) -> Result<()> {
        let url = self.url(&format!("/v1/admin/domains/{id}"))?;
        self.send_empty(self.http.delete(url)).await
    }

    pub async fn create_token(&self, req: CreateTokenRequest) -> Result<CreateTokenResponse> {
        let url = self.url("/v1/admin/tokens")?;
        self.send_json(self.http.post(url).json(&req)).await
    }

    pub async fn list_tokens(&self) -> Result<Vec<TokenInfo>> {
        let url = self.url("/v1/admin/tokens")?;
        self.send_json(self.http.get(url)).await
    }

    pub async fn revoke_token(&self, id: &str) -> Result<()> {
        let url = self.url(&format!("/v1/admin/tokens/{id}"))?;
        self.send_empty(self.http.delete(url)).await
    }

    pub async fn trigger_gc(&self, req: TriggerGcRequest) -> Result<TriggerGcResponse> {
        let url = self.url("/v1/admin/gc")?;
        self.send_json(self.http.post(url).json(&req)).await
    }

    pub async fn list_gc_jobs(
        &self,
        cache_id: Option<&str>,
        limit: Option<u32>,
    ) -> Result<Vec<GcJobResponse>> {
        let mut url = self.url("/v1/admin/gc")?;
        if let Some(cache_id) = cache_id {
            url.query_pairs_mut().append_pair("cache_id", cache_id);
        }
        if let Some(limit) = limit {
            url.query_pairs_mut()
                .append_pair("limit", &limit.to_string());
        }
        self.send_json(self.http.get(url)).await
    }

    pub async fn get_gc_job(&self, id: &str) -> Result<GcJobResponse> {
        let url = self.url(&format!("/v1/admin/gc/{id}"))?;
        self.send_json(self.http.get(url)).await
    }

    pub async fn get_stats(&self) -> Result<MetricsResponse> {
        let url = self.url("/v1/admin/metrics")?;
        self.send_json(self.http.get(url)).await
    }
}

// =============================================================================
// Request/response types (mirrored from server handlers)
// =============================================================================

#[derive(Debug, Serialize)]
pub struct CreateCacheRequest {
    pub cache_name: String,
    pub public_base_url: Option<String>,
    #[serde(default)]
    pub is_public: bool,
    #[serde(default)]
    pub is_default: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain_name: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct UpdateCacheRequest {
    pub cache_name: Option<String>,
    pub public_base_url: Option<String>,
    pub is_public: Option<bool>,
    pub is_default: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain_name: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CreateDomainRequest {
    pub domain_name: String,
}

#[derive(Debug, Serialize)]
pub struct UpdateDomainRequest {
    pub domain_name: String,
}

#[derive(Debug, Deserialize)]
pub struct DomainResponse {
    pub domain_id: String,
    pub domain_name: String,
    pub is_default: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize)]
pub struct ListDomainsResponse {
    pub domains: Vec<DomainResponse>,
}

#[derive(Debug, Deserialize)]
pub struct CacheResponse {
    pub cache_id: String,
    pub cache_name: String,
    #[serde(default)]
    pub domain_id: Option<String>,
    #[serde(default)]
    pub domain_name: Option<String>,
    pub public_base_url: Option<String>,
    pub is_public: bool,
    pub is_default: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize)]
pub struct ListCachesResponse {
    pub caches: Vec<CacheResponse>,
}

#[derive(Debug, Serialize)]
pub struct CreateTokenRequest {
    pub scopes: Vec<String>,
    pub cache_id: Option<String>,
    pub expires_in_secs: Option<u64>,
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateTokenResponse {
    pub token_id: String,
    pub token_secret: String,
    pub expires_at: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TokenInfo {
    pub token_id: String,
    pub cache_id: Option<String>,
    pub scopes: Vec<String>,
    pub expires_at: Option<String>,
    pub revoked_at: Option<String>,
    pub created_at: String,
    pub last_used_at: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TriggerGcRequest {
    pub job_type: String,
    pub cache_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TriggerGcResponse {
    pub job_id: String,
}

#[derive(Debug, Deserialize, Default)]
pub struct GcStats {
    pub items_processed: u64,
    pub items_deleted: u64,
    pub bytes_reclaimed: u64,
    pub errors: u64,
}

#[derive(Debug, Deserialize)]
pub struct GcJobResponse {
    pub job_id: String,
    pub cache_id: Option<String>,
    pub job_type: String,
    pub state: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub stats: Option<GcStats>,
}

#[derive(Debug, Deserialize)]
pub struct MetricsResponse {
    pub store_paths_count: u64,
    pub chunks_count: u64,
    pub chunks_total_size: u64,
    pub chunks_referenced: u64,
    pub chunks_unreferenced: u64,
    pub upload_sessions_created: u64,
    pub upload_sessions_committed: u64,
    pub upload_sessions_resumed: u64,
    pub upload_sessions_expired: u64,
    pub chunks_uploaded: u64,
    pub chunks_deduplicated: u64,
    pub bytes_uploaded: u64,
    pub bytes_deduplicated: u64,
    pub chunk_hash_mismatches: u64,
}
