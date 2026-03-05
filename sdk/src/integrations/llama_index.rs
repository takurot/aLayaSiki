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
    pub model_id: Option<String>,
    pub snapshot_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlamaGraphQuery {
    pub query: String,
    pub top_k: usize,
    pub depth: u8,
    pub model_id: Option<String>,
    pub snapshot_id: Option<String>,
}

#[async_trait]
pub trait VectorStore {
    async fn add(&self, request: IngestionRequest) -> Result<IngestResult, ClientError>;
    async fn similarity_search(
        &self,
        query: LlamaVectorQuery,
    ) -> Result<QueryResponse, ClientError>;
}

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

    pub fn client(&self) -> Arc<Client> {
        self.client.clone()
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
        let request = QueryRequest {
            query: query.query,
            top_k: normalize_top_k(query.top_k),
            mode: QueryMode::Evidence,
            search_mode: SearchMode::Local,
            model_id: query.model_id,
            snapshot_id: query.snapshot_id,
            ..QueryRequest::default()
        };

        self.client.query(request).await
    }
}

#[async_trait]
impl GraphStore for LlamaIndexAdapter {
    async fn query_subgraph(&self, query: LlamaGraphQuery) -> Result<QueryResponse, ClientError> {
        let request = QueryRequest {
            query: query.query,
            top_k: normalize_top_k(query.top_k),
            mode: QueryMode::Evidence,
            search_mode: SearchMode::Local,
            traversal: Traversal {
                depth: normalize_depth(query.depth),
                relation_types: Vec::new(),
            },
            model_id: query.model_id,
            snapshot_id: query.snapshot_id,
            ..QueryRequest::default()
        };

        self.client.query(request).await
    }
}
