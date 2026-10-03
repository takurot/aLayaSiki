use std::sync::Arc;

use alayasiki_core::ingest::IngestionRequest;
use async_trait::async_trait;
use query::dsl::Traversal;
use query::{QueryMode, QueryRequest, QueryResponse, SearchMode};

use crate::integrations::{normalize_depth, normalize_top_k};
use crate::{Client, ClientError, IngestResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlamaVectorQuery {
    pub query: String,
    pub top_k: usize,
    pub mode: Option<QueryMode>,
    pub search_mode: Option<SearchMode>,
    pub model_id: Option<String>,
    pub snapshot_id: Option<String>,
}

impl LlamaVectorQuery {
    fn into_request(self) -> QueryRequest {
        QueryRequest {
            query: self.query,
            top_k: normalize_top_k(self.top_k),
            mode: self.mode.unwrap_or(QueryMode::Evidence),
            search_mode: self.search_mode.unwrap_or(SearchMode::Local),
            model_id: self.model_id,
            snapshot_id: self.snapshot_id,
            ..QueryRequest::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlamaGraphQuery {
    pub query: String,
    pub top_k: usize,
    pub depth: u8,
    pub mode: Option<QueryMode>,
    pub search_mode: Option<SearchMode>,
    pub relation_types: Option<Vec<String>>,
    pub model_id: Option<String>,
    pub snapshot_id: Option<String>,
}

impl LlamaGraphQuery {
    fn into_request(self) -> QueryRequest {
        QueryRequest {
            query: self.query,
            top_k: normalize_top_k(self.top_k),
            mode: self.mode.unwrap_or(QueryMode::Evidence),
            search_mode: self.search_mode.unwrap_or(SearchMode::Local),
            traversal: Traversal {
                depth: normalize_depth(self.depth),
                relation_types: self.relation_types.unwrap_or_default(),
            },
            model_id: self.model_id,
            snapshot_id: self.snapshot_id,
            ..QueryRequest::default()
        }
    }
}

#[allow(clippy::double_must_use)]
#[async_trait]
pub trait VectorStore {
    async fn add(&self, request: IngestionRequest) -> Result<IngestResult, ClientError>;
    async fn similarity_search(
        &self,
        query: LlamaVectorQuery,
    ) -> Result<QueryResponse, ClientError>;
}

#[allow(clippy::double_must_use)]
#[async_trait]
pub trait GraphStore {
    async fn query_subgraph(&self, query: LlamaGraphQuery) -> Result<QueryResponse, ClientError>;
}

pub struct LlamaIndexAdapter {
    client: Arc<Client>,
}

impl LlamaIndexAdapter {
    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl VectorStore for LlamaIndexAdapter {
    async fn add(&self, request: IngestionRequest) -> Result<IngestResult, ClientError> {
        self.client.ingest(request).await
    }

    async fn similarity_search(
        &self,
        query: LlamaVectorQuery,
    ) -> Result<QueryResponse, ClientError> {
        self.client.query(query.into_request()).await
    }
}

#[async_trait]
impl GraphStore for LlamaIndexAdapter {
    async fn query_subgraph(&self, query: LlamaGraphQuery) -> Result<QueryResponse, ClientError> {
        self.client.query(query.into_request()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_query_preserves_explicit_modes() {
        let request = LlamaVectorQuery {
            query: "summarize the dataset".to_string(),
            top_k: 12,
            mode: Some(QueryMode::Answer),
            search_mode: Some(SearchMode::Global),
            model_id: None,
            snapshot_id: None,
        }
        .into_request();

        assert_eq!(request.mode, QueryMode::Answer);
        assert_eq!(request.search_mode, SearchMode::Global);
    }

    #[test]
    fn graph_query_preserves_modes_and_relation_types() {
        let request = LlamaGraphQuery {
            query: "trace suppliers".to_string(),
            top_k: 7,
            depth: 3,
            mode: Some(QueryMode::Answer),
            search_mode: Some(SearchMode::Drift),
            relation_types: Some(vec!["supplies".to_string(), "owns".to_string()]),
            model_id: None,
            snapshot_id: None,
        }
        .into_request();

        assert_eq!(request.mode, QueryMode::Answer);
        assert_eq!(request.search_mode, SearchMode::Drift);
        assert_eq!(request.traversal.depth, 3);
        assert_eq!(request.traversal.relation_types, ["supplies", "owns"]);
    }

    #[test]
    fn omitted_options_retain_adapter_defaults() {
        let request = LlamaGraphQuery {
            query: "find evidence".to_string(),
            top_k: 0,
            depth: 0,
            mode: None,
            search_mode: None,
            relation_types: None,
            model_id: None,
            snapshot_id: None,
        }
        .into_request();

        assert_eq!(request.top_k, 1);
        assert_eq!(request.traversal.depth, 1);
        assert_eq!(request.mode, QueryMode::Evidence);
        assert_eq!(request.search_mode, SearchMode::Local);
        assert!(request.traversal.relation_types.is_empty());
    }
}
