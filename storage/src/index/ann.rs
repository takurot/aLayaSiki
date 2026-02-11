use std::collections::HashMap;

/// Simple linear scan ANN index (placeholder for HNSW/IVF)
pub struct LinearAnnIndex {
    embeddings: HashMap<u64, Vec<f32>>,
}

impl LinearAnnIndex {
    pub fn new() -> Self {
        Self {
            embeddings: HashMap::new(),
        }
    }

    pub fn insert(&mut self, id: u64, embedding: Vec<f32>) {
        self.embeddings.insert(id, embedding);
    }

    pub fn delete(&mut self, id: u64) -> bool {
        self.embeddings.remove(&id).is_some()
    }

    /// Find top-k nearest neighbors using cosine similarity
    pub fn search(&self, query: &[f32], k: usize) -> Vec<(u64, f32)> {
        let mut scores: Vec<(u64, f32)> = self
            .embeddings
            .iter()
            .filter_map(|(id, emb)| cosine_similarity(query, emb).map(|score| (*id, score)))
            .collect();

        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.truncate(k);
        scores
    }

    pub fn len(&self) -> usize {
        self.embeddings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.embeddings.is_empty()
    }
}

impl Default for LinearAnnIndex {
    fn default() -> Self {
        Self::new()
    }
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != b.len() || a.is_empty() {
        return None;
    }

    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return Some(0.0);
    }

    Some(dot / (norm_a * norm_b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_ann_search() {
        let mut index = LinearAnnIndex::new();

        index.insert(1, vec![1.0, 0.0, 0.0]);
        index.insert(2, vec![0.0, 1.0, 0.0]);
        index.insert(3, vec![0.9, 0.1, 0.0]); // Similar to 1

        let results = index.search(&[1.0, 0.0, 0.0], 2);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 1); // Exact match first
        assert_eq!(results[1].0, 3); // Similar second
    }

    #[test]
    fn test_linear_ann_delete() {
        let mut index = LinearAnnIndex::new();
        index.insert(1, vec![1.0, 0.0]);

        assert!(index.delete(1));
        assert!(!index.delete(1)); // Already deleted
        assert!(index.is_empty());
    }
}
