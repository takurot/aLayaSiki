use super::replay::{apply_tx_operation, mutations_to_tx_operations, serialize_wal_entry};
use super::{IndexMutation, RepoError, Repository, TxOperation, WalEntry};
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use std::collections::HashSet;

impl Repository {
    pub async fn apply_index_transaction(
        &self,
        mutations: Vec<IndexMutation>,
    ) -> Result<(), RepoError> {
        if mutations.is_empty() {
            return Ok(());
        }

        let _tx_guard = self.tx_lock.lock().await;

        self.validate_index_transaction(&mutations).await?;

        let tx_operations = mutations_to_tx_operations(&mutations);
        let tx_entry = WalEntry::Transaction(tx_operations);
        let tx_bytes = serialize_wal_entry(&tx_entry)?;

        let durable_lsn = {
            let mut wal = self.wal.lock().await;
            wal.append(&tx_bytes).await?;
            wal.durable_lsn()
        };
        self.record_durable_snapshot(durable_lsn).await?;

        let mut nodes = self.nodes.write().await;
        let mut index = self.hyper_index.write().await;
        let mut edge_meta = self.edge_metadata.write().await;

        for mutation in mutations {
            match mutation {
                IndexMutation::PutNode(node) => {
                    let id = node.id;
                    let embedding = node.embedding.clone();
                    nodes.insert(id, node);
                    index.insert_node(id, embedding);
                }
                IndexMutation::PutEdge(edge) => {
                    let key = (edge.source, edge.target, edge.relation.clone());
                    if edge.metadata.is_empty() {
                        edge_meta.remove(&key);
                    } else {
                        edge_meta.insert(key, edge.metadata.clone());
                    }
                    index.upsert_edge(edge.source, edge.target, &edge.relation, edge.weight);
                }
                IndexMutation::DeleteNode(id) => {
                    nodes.remove(&id);
                    index.remove_node(id);
                    edge_meta.retain(|(src, tgt, _), _| *src != id && *tgt != id);
                }
            }
        }

        Ok(())
    }

    /// Persist a batch of ingested nodes and their idempotency keys in one WAL transaction.
    pub async fn persist_ingest_batch(
        &self,
        nodes_to_put: Vec<alayasiki_core::model::Node>,
        idempotency_records: Vec<(String, Vec<u64>)>,
    ) -> Result<(), RepoError> {
        if nodes_to_put.is_empty() && idempotency_records.is_empty() {
            return Ok(());
        }

        let _tx_guard = self.tx_lock.lock().await;

        let node_mutations: Vec<IndexMutation> = nodes_to_put
            .iter()
            .cloned()
            .map(IndexMutation::PutNode)
            .collect();
        self.validate_index_transaction(&node_mutations).await?;

        let mut idempotency_index = self.idempotency_index.write().await;
        let new_idempotency_records: Vec<(String, Vec<u64>)> = idempotency_records
            .into_iter()
            .filter(|(key, _)| !idempotency_index.contains_key(key))
            .collect();

        let mut tx_operations = mutations_to_tx_operations(&node_mutations);
        tx_operations.extend(new_idempotency_records.iter().map(|(key, node_ids)| {
            TxOperation::RecordIdempotency {
                key: key.clone(),
                node_ids: node_ids.clone(),
            }
        }));

        if tx_operations.is_empty() {
            return Ok(());
        }

        let tx_entry = WalEntry::Transaction(tx_operations.clone());
        let tx_bytes = serialize_wal_entry(&tx_entry)?;

        let durable_lsn = {
            let mut wal = self.wal.lock().await;
            wal.append(&tx_bytes).await?;
            wal.durable_lsn()
        };
        self.record_durable_snapshot(durable_lsn).await?;

        let mut nodes = self.nodes.write().await;
        let mut index = self.hyper_index.write().await;
        let mut edge_meta = self.edge_metadata.write().await;

        for operation in &tx_operations {
            apply_tx_operation(
                operation,
                &mut nodes,
                &mut index,
                &mut idempotency_index,
                &mut edge_meta,
            );
        }

        Ok(())
    }

    pub async fn record_idempotency(&self, key: &str, node_ids: Vec<u64>) -> Result<(), RepoError> {
        {
            let mut index = self.idempotency_index.write().await;
            if index.contains_key(key) {
                return Ok(());
            }

            let entry = WalEntry::IdempotencyKey {
                key: key.to_string(),
                node_ids: node_ids.clone(),
            };
            let mut serializer = AllocSerializer::<4096>::default();
            serializer
                .serialize_value(&entry)
                .map_err(|_| RepoError::Serialization)?;
            let bytes = serializer.into_serializer().into_inner();

            let durable_lsn = {
                let mut wal = self.wal.lock().await;
                wal.append(&bytes).await?;
                wal.durable_lsn()
            };
            self.record_durable_snapshot(durable_lsn).await?;

            index.insert(key.to_string(), node_ids);
        }

        Ok(())
    }

    async fn validate_index_transaction(
        &self,
        mutations: &[IndexMutation],
    ) -> Result<(), RepoError> {
        let nodes = self.nodes.read().await;
        let mut visible_nodes: HashSet<u64> = nodes.keys().copied().collect();

        for mutation in mutations {
            match mutation {
                IndexMutation::PutNode(node) => {
                    visible_nodes.insert(node.id);
                }
                IndexMutation::PutEdge(edge) => {
                    if !visible_nodes.contains(&edge.source) {
                        return Err(RepoError::InvalidTransaction(format!(
                            "edge source {} does not exist",
                            edge.source
                        )));
                    }
                    if !visible_nodes.contains(&edge.target) {
                        return Err(RepoError::InvalidTransaction(format!(
                            "edge target {} does not exist",
                            edge.target
                        )));
                    }
                }
                IndexMutation::DeleteNode(id) => {
                    if !visible_nodes.remove(id) {
                        return Err(RepoError::NotFound);
                    }
                }
            }
        }

        Ok(())
    }
}
