use super::*;
use crate::crypto::NoOpCipher;
use crate::hyper_index::HyperIndex;
use crate::session::SessionOwner;
use crate::wal::{Wal, WalError, WalFlushPolicy, WalOptions};
use alayasiki_core::model::{Edge, Node};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

#[tokio::test]
async fn test_repo_put_get() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");

    let repo = Repository::open(&wal_path).await.unwrap();

    let node = Node::new(1, vec![0.1, 0.2, 0.3], "Test Node".to_string());
    repo.put_node(node.clone()).await.unwrap();

    let retrieved = repo.get_node(1).await.unwrap();
    assert_eq!(retrieved, node);
}

#[tokio::test]
async fn test_repo_replay_on_restart() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("replay.wal");

    // 1. Create and populate
    {
        let repo = Repository::open(&wal_path).await.unwrap();
        repo.put_node(Node::new(1, vec![1.0], "Node 1".to_string()))
            .await
            .unwrap();
        repo.put_node(Node::new(2, vec![2.0], "Node 2".to_string()))
            .await
            .unwrap();
    }

    // 2. Reopen and verify replay
    {
        let repo = Repository::open(&wal_path).await.unwrap();
        assert_eq!(repo.get_node(1).await.unwrap().data, "Node 1");
        assert_eq!(repo.get_node(2).await.unwrap().data, "Node 2");
    }
}

#[tokio::test]
async fn test_repo_delete_tombstone() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("delete.wal");

    // 1. Create, put, delete
    {
        let repo = Repository::open(&wal_path).await.unwrap();
        repo.put_node(Node::new(1, vec![1.0], "Node 1".to_string()))
            .await
            .unwrap();
        repo.delete_node(1).await.unwrap();
    }

    // 2. Reopen and verify deleted node is gone
    {
        let repo = Repository::open(&wal_path).await.unwrap();
        assert!(repo.get_node(1).await.is_err());
    }
}

#[tokio::test]
async fn test_repo_edge_and_index_restore() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("edge_index.wal");

    // 1. Create Data (Node + Edge)
    {
        let repo = Repository::open(&wal_path).await.unwrap();
        repo.put_node(Node::new(1, vec![1.0, 0.0], "N1".to_string()))
            .await
            .unwrap();
        repo.put_node(Node::new(2, vec![0.0, 1.0], "N2".to_string()))
            .await
            .unwrap();
        repo.put_edge(Edge::new(1, 2, "links", 1.0)).await.unwrap();

        // Verify in-memory index
        let index = repo.hyper_index.read().await;
        let neighbors = index.expand_graph(1, 1);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].0, 2);
    }

    // 2. Restart and Verify Restoration
    {
        let repo = Repository::open(&wal_path).await.unwrap();

        // Check HyperIndex restored
        let index = repo.hyper_index.read().await;

        // Validation 1: Vector Index
        let search_res = index.search_vector(&[1.0, 0.0], 1);
        assert_eq!(search_res.len(), 1);
        assert_eq!(search_res[0].0, 1);

        // Validation 2: Graph Index
        let neighbors = index.expand_graph(1, 1);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].0, 2);
    }
}

#[tokio::test]
async fn test_index_transaction_commits_all_mutations() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("txn_commit.wal");
    let repo = Repository::open(&wal_path).await.unwrap();

    let n1 = Node::new(1, vec![1.0, 0.0], "N1".to_string());
    let n2 = Node::new(2, vec![0.0, 1.0], "N2".to_string());
    let e = Edge::new(1, 2, "links", 1.0);

    repo.apply_index_transaction(vec![
        IndexMutation::PutNode(n1),
        IndexMutation::PutNode(n2),
        IndexMutation::PutEdge(e),
    ])
    .await
    .unwrap();

    assert_eq!(repo.get_node(1).await.unwrap().data, "N1");
    assert_eq!(repo.get_node(2).await.unwrap().data, "N2");

    let index = repo.hyper_index.read().await;
    let neighbors = index.expand_graph(1, 1);
    assert_eq!(neighbors.len(), 1);
    assert_eq!(neighbors[0].0, 2);
}

#[tokio::test]
async fn test_index_transaction_rollback_on_validation_error() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("txn_rollback.wal");
    let repo = Repository::open(&wal_path).await.unwrap();

    let n1 = Node::new(1, vec![1.0], "N1".to_string());
    let invalid_edge = Edge::new(1, 999, "links", 1.0);

    let result = repo
        .apply_index_transaction(vec![
            IndexMutation::PutNode(n1),
            IndexMutation::PutEdge(invalid_edge),
        ])
        .await;

    assert!(result.is_err());
    assert!(
        repo.get_node(1).await.is_err(),
        "node should not be partially committed"
    );

    let reopened = Repository::open(&wal_path).await.unwrap();
    assert!(
        reopened.get_node(1).await.is_err(),
        "node should not be recoverable after failed transaction"
    );
}

#[tokio::test]
async fn test_index_transaction_persists_single_wal_record() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("txn_single_record.wal");
    let repo = Repository::open(&wal_path).await.unwrap();

    repo.apply_index_transaction(vec![
        IndexMutation::PutNode(Node::new(1, vec![1.0], "N1".to_string())),
        IndexMutation::PutNode(Node::new(2, vec![2.0], "N2".to_string())),
        IndexMutation::PutEdge(Edge::new(1, 2, "links", 1.0)),
    ])
    .await
    .unwrap();

    drop(repo);

    let mut wal = Wal::open(&wal_path).await.unwrap();
    let mut record_count = 0usize;
    let mut tx_mutation_count = 0usize;

    wal.replay(|_lsn, payload| {
        record_count += 1;
        let archived = rkyv::check_archived_root::<WalEntry>(&payload[..])
            .map_err(|_| WalError::CorruptEntry)?;
        let entry: WalEntry = archived
            .deserialize(&mut rkyv::Infallible)
            .expect("infallible deserializer");

        match entry {
            WalEntry::Transaction(entries) => {
                tx_mutation_count = entries.len();
            }
            _ => return Err(WalError::CorruptEntry),
        }

        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(
        record_count, 1,
        "transaction should be written as one WAL record"
    );
    assert_eq!(tx_mutation_count, 3);
}

#[tokio::test]
async fn test_persist_ingest_batch_persists_nodes_and_idempotency_in_single_wal_record() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("ingest_batch_single_record.wal");
    let repo = Repository::open(&wal_path).await.unwrap();

    let node_ids = vec![11_u64, 12_u64];
    repo.persist_ingest_batch(
        vec![
            Node::new(node_ids[0], vec![1.0], "N1".to_string()),
            Node::new(node_ids[1], vec![2.0], "N2".to_string()),
        ],
        vec![
            ("content-hash".to_string(), node_ids.clone()),
            ("request-key".to_string(), node_ids.clone()),
        ],
    )
    .await
    .unwrap();

    assert_eq!(repo.current_snapshot_id().await, "wal-lsn-1");
    assert_eq!(
        repo.check_idempotency("request-key").await,
        Some(node_ids.clone())
    );

    drop(repo);

    let reopened = Repository::open(&wal_path).await.unwrap();
    assert_eq!(reopened.list_node_ids().await, node_ids);
    assert_eq!(
        reopened.check_idempotency("content-hash").await,
        Some(vec![11, 12])
    );

    let mut wal = Wal::open(&wal_path).await.unwrap();
    let mut record_count = 0usize;
    let mut tx_mutation_count = 0usize;

    wal.replay(|_lsn, payload| {
        record_count += 1;
        let archived = rkyv::check_archived_root::<WalEntry>(&payload[..])
            .map_err(|_| WalError::CorruptEntry)?;
        let entry: WalEntry = archived
            .deserialize(&mut rkyv::Infallible)
            .expect("infallible deserializer");

        match entry {
            WalEntry::Transaction(entries) => {
                tx_mutation_count = entries.len();
            }
            _ => return Err(WalError::CorruptEntry),
        }

        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(record_count, 1);
    assert_eq!(tx_mutation_count, 4);
}

#[tokio::test]
async fn test_persist_ingest_batch_keeps_first_content_hash_mapping() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("ingest_batch_first_writer.wal");
    let repo = Repository::open(&wal_path).await.unwrap();

    repo.persist_ingest_batch(
        vec![Node::new(11, vec![1.0], "first".to_string())],
        vec![
            ("content-hash".to_string(), vec![11]),
            ("request-a".to_string(), vec![11]),
        ],
    )
    .await
    .unwrap();

    repo.persist_ingest_batch(
        vec![
            Node::new(11, vec![1.0], "first".to_string()),
            Node::new(12, vec![2.0], "second".to_string()),
        ],
        vec![
            ("content-hash".to_string(), vec![11, 12]),
            ("request-b".to_string(), vec![11, 12]),
        ],
    )
    .await
    .unwrap();

    assert_eq!(repo.check_idempotency("content-hash").await, Some(vec![11]));
    assert_eq!(repo.check_idempotency("request-a").await, Some(vec![11]));
    assert_eq!(
        repo.check_idempotency("request-b").await,
        Some(vec![11, 12])
    );

    drop(repo);

    let reopened = Repository::open(&wal_path).await.unwrap();
    assert_eq!(
        reopened.check_idempotency("content-hash").await,
        Some(vec![11])
    );
    assert_eq!(
        reopened.check_idempotency("request-b").await,
        Some(vec![11, 12])
    );
}

#[tokio::test]
async fn test_index_transaction_flush_and_reopen_preserves_seeded_graph() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("txn_seed_flush_reopen.wal");
    let repo = Repository::open_with_options(
        &wal_path,
        WalOptions {
            flush_policy: WalFlushPolicy::Batch { max_entries: 8 },
            ..WalOptions::default()
        },
    )
    .await
    .unwrap();

    repo.apply_index_transaction(vec![
        IndexMutation::PutNode(Node::new(1, vec![1.0], "N1".to_string())),
        IndexMutation::PutNode(Node::new(2, vec![2.0], "N2".to_string())),
        IndexMutation::PutNode(Node::new(3, vec![3.0], "N3".to_string())),
    ])
    .await
    .unwrap();
    repo.apply_index_transaction(vec![
        IndexMutation::PutEdge(Edge::new(1, 2, "links", 1.0)),
        IndexMutation::PutEdge(Edge::new(1, 3, "links", 0.5)),
    ])
    .await
    .unwrap();
    repo.flush().await.unwrap();

    drop(repo);

    let reopened = Repository::open(&wal_path).await.unwrap();
    assert_eq!(reopened.list_node_ids().await, vec![1, 2, 3]);

    let graph = reopened.graph_index().await;
    assert_eq!(graph.edge_count(), 2);
    let neighbors = graph.neighbors(1);
    assert_eq!(neighbors.len(), 2);
    assert!(neighbors
        .iter()
        .any(|(target, relation, _)| { *target == 2 && relation == "links" }));
    assert!(neighbors
        .iter()
        .any(|(target, relation, _)| { *target == 3 && relation == "links" }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_concurrent_delete_and_put_edge_do_not_leave_dangling_edge() {
    use tokio::sync::Barrier;

    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("txn_concurrency.wal");
    let repo = Arc::new(Repository::open(&wal_path).await.unwrap());

    repo.put_node(Node::new(1, vec![1.0], "N1".to_string()))
        .await
        .unwrap();

    for _ in 0..64 {
        if repo.get_node(2).await.is_err() {
            repo.put_node(Node::new(2, vec![2.0], "N2".to_string()))
                .await
                .unwrap();
        }

        let barrier = Arc::new(Barrier::new(3));

        let repo_put = repo.clone();
        let barrier_put = barrier.clone();
        let put_task = tokio::spawn(async move {
            barrier_put.wait().await;
            let _ = repo_put.put_edge(Edge::new(1, 2, "links", 1.0)).await;
        });

        let repo_delete = repo.clone();
        let barrier_delete = barrier.clone();
        let delete_task = tokio::spawn(async move {
            barrier_delete.wait().await;
            let _ = repo_delete.delete_node(2).await;
        });

        barrier.wait().await;
        put_task.await.unwrap();
        delete_task.await.unwrap();

        assert!(
            repo.get_node(2).await.is_err(),
            "node 2 should be deleted after concurrent operations"
        );

        let index = repo.hyper_index.read().await;
        let neighbors = index.expand_graph(1, 1);
        assert!(
            !neighbors.iter().any(|(id, _)| *id == 2),
            "dangling edge to deleted node must not remain"
        );
    }
}

#[tokio::test]
async fn test_load_snapshot_view_reconstructs_historical_state() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("snapshot_view.wal");
    let repo = Repository::open(&wal_path).await.unwrap();

    repo.put_node(Node::new(1, vec![1.0], "N1".to_string()))
        .await
        .unwrap();
    let snapshot_lsn_1 = repo.current_snapshot_id().await;

    repo.put_node(Node::new(2, vec![2.0], "N2".to_string()))
        .await
        .unwrap();
    repo.delete_node(1).await.unwrap();

    let view_at_lsn_1 = repo.load_snapshot_view(&snapshot_lsn_1).await.unwrap();
    assert_eq!(view_at_lsn_1.snapshot_id(), snapshot_lsn_1);
    assert_eq!(view_at_lsn_1.list_node_ids(), vec![1]);
    assert_eq!(view_at_lsn_1.get_nodes_by_ids(&[1])[0].data, "N1");

    let view_at_lsn_3 = repo.load_snapshot_view("wal-lsn-3").await.unwrap();
    assert_eq!(view_at_lsn_3.list_node_ids(), vec![2]);
}

#[tokio::test]
async fn test_load_snapshot_view_rejects_missing_or_invalid_snapshot_id() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("snapshot_view_errors.wal");
    let repo = Repository::open(&wal_path).await.unwrap();

    repo.put_node(Node::new(1, vec![1.0], "N1".to_string()))
        .await
        .unwrap();

    let missing = repo.load_snapshot_view("wal-lsn-99").await;
    assert!(matches!(missing, Err(RepoError::SnapshotNotFound(_))));

    let invalid = repo.load_snapshot_view("snap-custom").await;
    assert!(matches!(invalid, Err(RepoError::InvalidSnapshotId(_))));
}

#[tokio::test]
async fn test_current_snapshot_id_tracks_durable_lsn_for_buffered_policies() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("durable_snapshot_id_batch.wal");
    let repo = Repository::open_with_options(
        &wal_path,
        WalOptions {
            flush_policy: WalFlushPolicy::Batch { max_entries: 8 },
            ..WalOptions::default()
        },
    )
    .await
    .unwrap();

    repo.put_node(Node::new(1, vec![1.0], "N1".to_string()))
        .await
        .unwrap();
    assert_eq!(repo.current_snapshot_id().await, "wal-lsn-0");

    repo.flush().await.unwrap();
    assert_eq!(repo.current_snapshot_id().await, "wal-lsn-1");

    drop(repo);

    let reopened = Repository::open(&wal_path).await.unwrap();
    assert_eq!(reopened.list_node_ids().await, vec![1]);
}

#[tokio::test]
async fn test_resolve_snapshot_id_at_or_before_uses_persisted_catalog() {
    let before_repo = current_unix_timestamp_ms() - 1;
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("snapshot_catalog_resolution.wal");
    let repo = Repository::open(&wal_path).await.unwrap();

    repo.put_node(Node::new(1, vec![1.0], "N1".to_string()))
        .await
        .unwrap();
    let first_snapshot = repo.current_snapshot_id().await;
    let after_first = current_unix_timestamp_ms();

    thread::sleep(Duration::from_millis(5));

    repo.put_node(Node::new(2, vec![2.0], "N2".to_string()))
        .await
        .unwrap();

    assert!(matches!(
        repo.resolve_snapshot_id_at_or_before(before_repo).await,
        Err(RepoError::SnapshotNotFound(_))
    ));
    assert_eq!(
        repo.resolve_snapshot_id_at_or_before(after_first)
            .await
            .unwrap(),
        first_snapshot
    );
    assert_eq!(repo.snapshot_catalog_entries().await.len(), 3);
}

#[tokio::test]
async fn test_create_backup_snapshot_flushes_pending_wal_before_persisting() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("snapshot_flush_batch.wal");
    let snapshot_dir = dir.path().join("snapshots");

    {
        let repo = Repository::open_with_cipher_and_snapshots_and_options(
            &wal_path,
            Arc::new(NoOpCipher),
            &snapshot_dir,
            WalOptions {
                flush_policy: WalFlushPolicy::Batch { max_entries: 8 },
                ..WalOptions::default()
            },
        )
        .await
        .unwrap();
        repo.put_node(Node::new(1, vec![1.0], "N1".to_string()))
            .await
            .unwrap();
        repo.put_node(Node::new(2, vec![2.0], "N2".to_string()))
            .await
            .unwrap();

        let snapshot_id = repo.create_backup_snapshot().await.unwrap();
        assert_eq!(snapshot_id, "wal-lsn-2");
    }

    let reopened = Repository::open_with_snapshots(&wal_path, &snapshot_dir)
        .await
        .unwrap();
    assert_eq!(reopened.list_node_ids().await, vec![1, 2]);
}

#[tokio::test]
async fn test_open_with_snapshots_restores_snapshot_and_wal_delta() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("snapshot_restore.wal");
    let snapshot_dir = dir.path().join("snapshots");

    {
        let repo = Repository::open_with_snapshots(&wal_path, &snapshot_dir)
            .await
            .unwrap();
        repo.put_node(Node::new(1, vec![1.0], "N1".to_string()))
            .await
            .unwrap();
        repo.put_node(Node::new(2, vec![2.0], "N2".to_string()))
            .await
            .unwrap();

        let backup_id = repo.create_backup_snapshot().await.unwrap();
        assert_eq!(backup_id, "wal-lsn-2");

        repo.put_node(Node::new(3, vec![3.0], "N3".to_string()))
            .await
            .unwrap();
    }

    let reopened = Repository::open_with_snapshots(&wal_path, &snapshot_dir)
        .await
        .unwrap();
    assert_eq!(reopened.list_node_ids().await, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_restore_from_latest_backup_rebuilds_in_memory_state() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("restore_latest_from_backup.wal");
    let snapshot_dir = dir.path().join("snapshots");

    let repo = Repository::open_with_snapshots(&wal_path, &snapshot_dir)
        .await
        .unwrap();
    repo.put_node(Node::new(1, vec![1.0], "N1".to_string()))
        .await
        .unwrap();
    repo.put_node(Node::new(2, vec![2.0], "N2".to_string()))
        .await
        .unwrap();
    repo.create_backup_snapshot().await.unwrap();
    repo.delete_node(2).await.unwrap();

    // Simulate transient in-memory corruption and verify restore recovers from durable state.
    repo.nodes.write().await.clear();
    *repo.hyper_index.write().await = HyperIndex::new();
    repo.idempotency_index.write().await.clear();
    repo.edge_metadata.write().await.clear();

    assert!(repo.list_node_ids().await.is_empty());

    let restored_snapshot = repo.restore_from_latest_backup().await.unwrap();
    assert_eq!(restored_snapshot, "wal-lsn-3");
    assert_eq!(repo.list_node_ids().await, vec![1]);
}

#[tokio::test]
async fn test_backup_requires_snapshot_manager_configuration() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("no_snapshot_manager.wal");
    let repo = Repository::open(&wal_path).await.unwrap();

    let create_backup = repo.create_backup_snapshot().await;
    assert!(matches!(
        create_backup,
        Err(RepoError::SnapshotNotConfigured)
    ));

    let restore = repo.restore_from_latest_backup().await;
    assert!(matches!(restore, Err(RepoError::SnapshotNotConfigured)));
}

#[tokio::test]
async fn test_open_with_snapshots_rejects_snapshot_newer_than_wal() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("snapshot_newer_than_wal.wal");
    let snapshot_dir = dir.path().join("snapshots");

    {
        let repo = Repository::open_with_snapshots(&wal_path, &snapshot_dir)
            .await
            .unwrap();
        repo.put_node(Node::new(1, vec![1.0], "N1".to_string()))
            .await
            .unwrap();
        repo.put_node(Node::new(2, vec![2.0], "N2".to_string()))
            .await
            .unwrap();
        let snapshot_id = repo.create_backup_snapshot().await.unwrap();
        assert_eq!(snapshot_id, "wal-lsn-2");
    }

    tokio::fs::write(&wal_path, &[]).await.unwrap();

    let reopened = Repository::open_with_snapshots(&wal_path, &snapshot_dir).await;
    assert!(matches!(
        reopened,
        Err(RepoError::SnapshotNotFound(ref snapshot_id)) if snapshot_id == "wal-lsn-2"
    ));
}

#[tokio::test]
async fn test_promote_session_restores_data_on_validation_failure() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("session_promote_failure_restore.wal");
    let repo = Repository::open(&wal_path).await.unwrap();

    let session_id = "session-promote-failure";
    repo.ingest_to_session(
        session_id,
        Node::new(1, vec![1.0, 0.0], "session-node".to_string()),
    );
    repo.insert_edge_to_session(session_id, Edge::new(1, 2, "missing_target", 1.0));

    let result = repo.promote_session_to_persistent(session_id).await;
    assert!(matches!(result, Err(RepoError::InvalidTransaction(_))));

    let restored = repo
        .session_manager
        .get(session_id)
        .expect("session should be restored after failed promote");
    assert_eq!(restored.nodes.len(), 1);
    assert_eq!(restored.edges.len(), 1);
}

#[tokio::test]
async fn test_session_owner_enforced_for_ingest_and_query() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("session_owner_enforcement.wal");
    let repo = Repository::open(&wal_path).await.unwrap();

    let owner_a = SessionOwner::new("tenant-a", "user-a");
    let owner_b = SessionOwner::new("tenant-a", "user-b");
    let session_id = "session-owner";

    repo.ingest_to_session_with_owner(
        session_id,
        &owner_a,
        Node::new(1, vec![1.0], "owner-a-node".to_string()),
    )
    .unwrap();

    let denied_write = repo.ingest_to_session_with_owner(
        session_id,
        &owner_b,
        Node::new(2, vec![2.0], "owner-b-node".to_string()),
    );
    assert!(matches!(
        denied_write,
        Err(RepoError::SessionAccessDenied(_))
    ));

    let denied_read = repo.get_session_with_owner(session_id, Some(&owner_b));
    assert!(matches!(
        denied_read,
        Err(RepoError::SessionAccessDenied(_))
    ));

    let allowed_read = repo
        .get_session_with_owner(session_id, Some(&owner_a))
        .unwrap()
        .expect("session should exist for owner");
    assert_eq!(allowed_read.nodes.len(), 1);
    assert!(allowed_read.nodes.contains_key(&1));
}
