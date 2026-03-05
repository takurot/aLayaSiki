use std::sync::Arc;

use alayasiki_core::ingest::IngestionRequest;
use async_trait::async_trait;
use query::dsl::Traversal;
use query::{QueryMode, QueryRequest, QueryResponse, SearchMode};

use crate::integrations::{normalize_depth, normalize_top_k};
use crate::{Client, ClientError, IngestResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LangChainSimilarityQuery {
    pub query: String,
    pub top_k: usize,
    pub model_id: Option<String>,
    pub snapshot_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LangChainGraphQuery {
    pub query: String,
    pub top_k: usize,
    pub depth: u8,
    pub model_id: Option<String>,
    pub snapshot_id: Option<String>,
}

#[async_trait]
pub trait GraphVectorStore {
    async fn add_documents(
        &self,
        requests: Vec<IngestionRequest>,
    ) -> Result<Vec<IngestResult>, ClientError>;

    async fn similarity_search(
        &self,
        query: LangChainSimilarityQuery,
    ) -> Result<QueryResponse, ClientError>;

    async fn graph_search(&self, query: LangChainGraphQuery) -> Result<QueryResponse, ClientError>;
}

pub struct LangChainAdapter {
    client: Arc<Client>,
}

impl LangChainAdapter {
    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
    }

    pub fn client(&self) -> Arc<Client> {
        self.client.clone()
    }
}

#[async_trait]
impl GraphVectorStore for LangChainAdapter {
    async fn add_documents(
        &self,
        requests: Vec<IngestionRequest>,
    ) -> Result<Vec<IngestResult>, ClientError> {
        let mut out = Vec::with_capacity(requests.len());
        for request in requests {
            out.push(self.client.ingest(request).await?);
        }
        Ok(out)
    }

    async fn similarity_search(
        &self,
        query: LangChainSimilarityQuery,
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

    async fn graph_search(&self, query: LangChainGraphQuery) -> Result<QueryResponse, ClientError> {
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
