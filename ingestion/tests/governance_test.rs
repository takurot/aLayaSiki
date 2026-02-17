use alayasiki_core::auth::{Authorizer, Principal, ResourceContext};
use alayasiki_core::governance::{
    EncryptionPolicy, GovernanceError, InMemoryGovernancePolicyStore, TenantGovernancePolicy,
};
use alayasiki_core::ingest::IngestionRequest;
use ingestion::processor::{IngestionError, IngestionPipeline};
use std::collections::HashMap;
use std::sync::Arc;
use storage::repo::Repository;
use tempfile::tempdir;

fn make_request(region: &str) -> IngestionRequest {
    let mut metadata = HashMap::new();
    metadata.insert("region".to_string(), region.to_string());

    IngestionRequest::Text {
        content: "governed content".to_string(),
        metadata,
        idempotency_key: None,
        model_id: None,
    }
}

#[tokio::test]
async fn ingest_authorized_rejects_region_mismatch_policy() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("governance_region.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    let mut pipeline = IngestionPipeline::new(repo);
    let store = Arc::new(InMemoryGovernancePolicyStore::default());
    store
        .upsert_policy(TenantGovernancePolicy::new("acme", "ap-northeast-1", 30))
        .unwrap();
    pipeline.set_governance_policy_store(store);

    let principal = Principal::new("ingestor-1", "acme").with_roles(["ingestor"]);
    let authorizer = Authorizer::default();
    let resource = ResourceContext::new("acme");

    let err = pipeline
        .ingest_authorized(
            make_request("us-east-1"),
            &principal,
            &authorizer,
            &resource,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        IngestionError::Governance(GovernanceError::ResidencyViolation { .. })
    ));
}

#[tokio::test]
async fn ingest_authorized_stamps_retention_and_kms_metadata() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("governance_retention.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    let mut pipeline = IngestionPipeline::new(repo.clone());
    let store = Arc::new(InMemoryGovernancePolicyStore::default());
    let policy = TenantGovernancePolicy::new("acme", "ap-northeast-1", 7)
        .with_encryption(EncryptionPolicy::kms("kms-key-acme"))
        .unwrap();
    store.upsert_policy(policy).unwrap();
    pipeline.set_governance_policy_store(store);

    let principal = Principal::new("ingestor-1", "acme").with_roles(["ingestor"]);
    let authorizer = Authorizer::default();
    let resource = ResourceContext::new("acme");

    let ids = pipeline
        .ingest_authorized(
            make_request("ap-northeast-1"),
            &principal,
            &authorizer,
            &resource,
        )
        .await
        .unwrap();

    let node = repo.get_node(ids[0]).await.unwrap();
    assert_eq!(node.metadata.get("tenant"), Some(&"acme".to_string()));
    assert_eq!(
        node.metadata.get("residency_region"),
        Some(&"ap-northeast-1".to_string())
    );
    assert_eq!(
        node.metadata.get("kms_key_id"),
        Some(&"kms-key-acme".to_string())
    );

    let retention = node
        .metadata
        .get("retention_until_unix")
        .expect("retention metadata is required")
        .parse::<u64>()
        .unwrap();
    assert!(retention > 0);
}
