//! Single-attempt HTTP transport; [`crate::Client`] owns retry policy.
use crate::{ClientBuildError, ClientError, IngestResult, SdkTransport};
use alayasiki_core::{error::ErrorCode, http::ApiResponse, ingest::IngestionRequest};
use async_trait::async_trait;
use query::{QueryRequest, QueryResponse};
use reqwest::{
    header::{HeaderValue, AUTHORIZATION},
    redirect::Policy,
    Url,
};
use serde::{de::DeserializeOwned, Serialize};

const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

pub struct RemoteTransport {
    client: reqwest::Client,
    base_url: Url,
    authorization: HeaderValue,
}
impl RemoteTransport {
    pub fn new(base_url: &str, bearer_token: &str) -> Result<Self, ClientBuildError> {
        let base_url =
            Url::parse(base_url).map_err(|_| ClientBuildError::InvalidRemoteConfiguration)?;
        if !matches!(base_url.scheme(), "http" | "https")
            || base_url.host_str().is_none()
            || !base_url.username().is_empty()
            || base_url.password().is_some()
            || base_url.query().is_some()
            || base_url.fragment().is_some()
            || !matches!(base_url.path(), "" | "/")
            || bearer_token.is_empty()
            || !bearer_token
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-._~+/=".contains(&byte))
        {
            return Err(ClientBuildError::InvalidRemoteConfiguration);
        }
        let mut authorization = HeaderValue::from_str(&format!("Bearer {bearer_token}"))
            .map_err(|_| ClientBuildError::InvalidRemoteConfiguration)?;
        authorization.set_sensitive(true);
        let client = reqwest::Client::builder()
            .redirect(Policy::none())
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|_| ClientBuildError::InvalidRemoteConfiguration)?;
        Ok(Self {
            client,
            base_url,
            authorization,
        })
    }
    async fn post<R: Serialize + Sync, T: DeserializeOwned>(
        &self,
        path: &str,
        request: &R,
    ) -> Result<T, ClientError> {
        let url = self
            .base_url
            .join(path)
            .map_err(|_| ClientError::Protocol("invalid endpoint".into()))?;
        let mut response = self
            .client
            .post(url)
            .header(AUTHORIZATION, self.authorization.clone())
            .json(request)
            .send()
            .await
            .map_err(|_| ClientError::Network)?;
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|size| size > MAX_RESPONSE_BYTES as u64)
        {
            return Err(ClientError::Protocol("response exceeds size limit".into()));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| ClientError::Network)? {
            if chunk.len() > MAX_RESPONSE_BYTES.saturating_sub(body.len()) {
                return Err(ClientError::Protocol("response exceeds size limit".into()));
            }
            body.extend_from_slice(&chunk);
        }
        let envelope: ApiResponse<T> = serde_json::from_slice(&body)
            .map_err(|_| ClientError::Protocol("invalid API response".into()))?;
        match (
            status.is_success(),
            envelope.success,
            envelope.data,
            envelope.error,
        ) {
            (true, true, Some(data), None) => Ok(data),
            (false, false, None, Some(error)) => {
                let transient = matches!(status.as_u16(), 429 | 502 | 503 | 504);
                let permanent_code = matches!(
                    error.code,
                    ErrorCode::InvalidArgument
                        | ErrorCode::NotFound
                        | ErrorCode::PermissionDenied
                        | ErrorCode::Unauthenticated
                );
                Err(ClientError::Remote {
                    code: error.code,
                    message: error.message,
                    retryable: transient && error.retryable && !permanent_code,
                })
            }
            _ => Err(ClientError::Protocol("inconsistent API response".into())),
        }
    }
}
#[async_trait]
impl SdkTransport for RemoteTransport {
    async fn ingest(&self, request: IngestionRequest) -> Result<IngestResult, ClientError> {
        self.post("v1/ingest", &request).await
    }
    async fn query(&self, request: QueryRequest) -> Result<QueryResponse, ClientError> {
        self.post("v1/query", &request).await
    }
}
