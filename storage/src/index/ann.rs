use alayasiki_core::embedding::cosine_similarity;
use std::collections::HashMap;

/// Abstraction over ANN vector index implementations.
///
/// Implementations must be `Send + Sync` so that `Box<dyn VectorIndex>` can
/// be held behind an `Arc<RwLock<HyperIndex>>`.
pub trait VectorIndex: Send + Sync {
    /// Insert or overwrite a vector with the given node `id`.
    fn insert(&mut self, id: u64, embedding: &[f32]);
    /// Delete a vector by `id`. Returns `true` if the id was present.
    fn delete(&mut self, id: u64) -> bool;
    /// Return the top-`k` most similar nodes to `query`, sorted descending by
    /// cosine similarity score (higher = more similar).
    fn search(&self, query: &[f32], k: usize) -> Vec<(u64, f32)>;
    /// Number of vectors currently stored.
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Dimension of stored vectors, or `None` if the index is empty.
    fn dim(&self) -> Option<usize>;
}

/// Simple O(n·d) linear scan ANN index.
///
/// Used as the ground-truth reference in recall@k tests and as a fallback
/// when the `hnsw` feature is disabled.
pub struct LinearAnnIndex {
    embeddings: HashMap<u64, Vec<f32>>,
}

impl LinearAnnIndex {
    pub fn new() -> Self {
        Self {
            embeddings: HashMap::new(),
        }
    }
}

impl VectorIndex for LinearAnnIndex {
    fn insert(&mut self, id: u64, embedding: &[f32]) {
        self.embeddings.insert(id, embedding.to_vec());
    }

    fn delete(&mut self, id: u64) -> bool {
        self.embeddings.remove(&id).is_some()
    }

    /// Find top-k nearest neighbors using cosine similarity.
    fn search(&self, query: &[f32], k: usize) -> Vec<(u64, f32)> {
        let mut scores: Vec<(u64, f32)> = self
            .embeddings
            .iter()
            .filter_map(|(id, emb)| cosine_similarity(query, emb).map(|score| (*id, score)))
            .collect();

        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.truncate(k);
        scores
    }

    fn len(&self) -> usize {
        self.embeddings.len()
    }

    fn dim(&self) -> Option<usize> {
        self.embeddings
            .values()
            .next()
            .map(|v| v.len())
            .filter(|&d| d > 0)
    }
}

impl Default for LinearAnnIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_ann_search() {
        let mut index = LinearAnnIndex::new();

        index.insert(1, &[1.0, 0.0, 0.0]);
        index.insert(2, &[0.0, 1.0, 0.0]);
        index.insert(3, &[0.9, 0.1, 0.0]); // Similar to 1

        let results = index.search(&[1.0, 0.0, 0.0], 2);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 1); // Exact match first
        assert_eq!(results[1].0, 3); // Similar second
    }

    #[test]
    fn test_linear_ann_delete() {
        let mut index = LinearAnnIndex::new();
        index.insert(1, &[1.0, 0.0]);

        assert!(index.delete(1));
        assert!(!index.delete(1)); // Already deleted
        assert!(index.is_empty());
    }

    #[test]
    fn test_linear_ann_dim() {
        let mut index = LinearAnnIndex::new();
        assert_eq!(index.dim(), None);
        index.insert(1, &[1.0, 0.0, 0.0]);
        assert_eq!(index.dim(), Some(3));
    }
}
