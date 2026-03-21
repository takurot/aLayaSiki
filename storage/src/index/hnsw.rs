use super::ann::{LinearAnnIndex, VectorIndex};

#[cfg(not(target_os = "macos"))]
use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

#[cfg(not(target_os = "macos"))]
/// HNSW-based ANN index backed by the `usearch` C++ library.
///
/// # Lazy initialisation
/// The underlying `usearch::Index` requires the vector dimension at creation
/// time. `HnswIndex` delays creation until the first `insert` call so that
/// callers do not need to know the dimension up-front (matching the
/// `LinearAnnIndex` interface). Any subsequent `insert` with a mismatched
/// dimension is silently ignored (same behaviour as `cosine_similarity`).
///
/// # Thread safety
/// `usearch::Index` wraps a C++ object via a raw pointer and therefore does
/// not derive `Send`/`Sync` automatically. The underlying USearch library is
/// documented as thread-safe for concurrent reads; exclusive write access is
/// enforced at the `HyperIndex` level by an `RwLock`. The `unsafe` impls
/// below are therefore sound.
///
/// # ANN Sidecar Snapshot Format (future work)
/// To support fast cold-start without a full WAL replay, the HNSW graph can be
/// serialised alongside a regular storage snapshot. Planned format:
///
/// ```text
/// <snapshot_dir>/ann_sidecar.usearch   – native usearch binary dump
/// <snapshot_dir>/ann_sidecar.meta.json – { "lsn": <u64>, "dim": <usize>,
///                                          "size": <usize>,
///                                          "metric": "cosine",
///                                          "format_version": 1 }
/// ```
///
/// On startup the engine checks whether the sidecar's `lsn` matches the
/// snapshot LSN. If so, the index is loaded directly; otherwise the index is
/// rebuilt from the WAL replay as today. This is tracked as a follow-up task
/// in `docs/PLAN.md`.
pub struct HnswIndex {
    /// Lazily-initialised inner index (None until first insert).
    inner: Option<Index>,
    dim: Option<usize>,
    /// Tracks logical element count so `len()` doesn't need to call into C++
    /// after every mutating operation.
    count: usize,
}

#[cfg(target_os = "macos")]
/// macOS fallback that preserves the HNSW API while avoiding the `usearch`
/// C++ toolchain dependency on Apple Silicon developer machines.
pub struct HnswIndex {
    inner: LinearAnnIndex,
    dim: Option<usize>,
}

#[cfg(not(target_os = "macos"))]
// SAFETY: USearch C++ implementation is thread-safe for concurrent reads.
// Mutable operations are serialised by the RwLock on HyperIndex.
unsafe impl Send for HnswIndex {}
#[cfg(not(target_os = "macos"))]
unsafe impl Sync for HnswIndex {}

#[cfg(not(target_os = "macos"))]
impl HnswIndex {
    pub fn new() -> Self {
        Self {
            inner: None,
            dim: None,
            count: 0,
        }
    }

    fn reset_if_empty(&mut self) {
        if self.count == 0 {
            self.inner = None;
            self.dim = None;
        }
    }

    /// Initialise the inner index for the given dimension if not already done.
    /// Returns `false` if the dimension conflicts with an already-initialised index.
    fn ensure_index(&mut self, dim: usize) -> bool {
        if let Some(existing_dim) = self.dim {
            return existing_dim == dim;
        }
        let options = IndexOptions {
            dimensions: dim,
            metric: MetricKind::Cos,
            quantization: ScalarKind::F32,
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
            multi: false,
        };
        match Index::new(&options) {
            Ok(idx) => {
                self.inner = Some(idx);
                self.dim = Some(dim);
                true
            }
            Err(e) => {
                tracing::error!("HnswIndex: failed to create usearch index: {e}");
                false
            }
        }
    }

    /// Grow the inner index capacity to accommodate at least `needed` elements.
    fn maybe_reserve(&self, needed: usize) {
        let Some(idx) = &self.inner else { return };
        let capacity = idx.capacity();
        if needed > capacity {
            let new_cap = (needed * 2).max(64);
            if let Err(e) = idx.reserve(new_cap) {
                tracing::warn!("HnswIndex: reserve({new_cap}) failed: {e}");
            }
        }
    }

    fn remove_existing(&mut self, id: u64) -> bool {
        let Some(idx) = &self.inner else { return false };
        if !idx.contains(id) {
            return false;
        }
        match idx.remove(id) {
            Ok(removed) if removed > 0 => {
                self.count = self.count.saturating_sub(removed);
                self.reset_if_empty();
                true
            }
            Ok(_) => false,
            Err(e) => {
                tracing::error!("HnswIndex::delete id={id}: {e}");
                false
            }
        }
    }
}

#[cfg(target_os = "macos")]
impl HnswIndex {
    pub fn new() -> Self {
        Self {
            inner: LinearAnnIndex::new(),
            dim: None,
        }
    }
}

impl Default for HnswIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(not(target_os = "macos"))]
impl VectorIndex for HnswIndex {
    fn insert(&mut self, id: u64, embedding: &[f32]) {
        if embedding.is_empty() {
            self.remove_existing(id);
            return;
        }
        self.remove_existing(id);
        if !self.ensure_index(embedding.len()) {
            tracing::warn!(
                "HnswIndex::insert: dimension mismatch (expected {:?}, got {}), skipping id={}",
                self.dim,
                embedding.len(),
                id
            );
            return;
        }
        self.maybe_reserve(self.count + 1);
        let Some(idx) = &self.inner else { return };
        match idx.add(id, embedding) {
            Ok(()) => self.count += 1,
            Err(e) => tracing::error!("HnswIndex::insert id={id}: {e}"),
        }
    }

    fn delete(&mut self, id: u64) -> bool {
        self.remove_existing(id)
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<(u64, f32)> {
        let Some(idx) = &self.inner else {
            return vec![];
        };
        if k == 0 || query.is_empty() {
            return vec![];
        }
        if let Some(d) = self.dim {
            if d != query.len() {
                return vec![];
            }
        }
        let count = k.min(self.count);
        if count == 0 {
            return vec![];
        }
        match idx.search(query, count) {
            Ok(matches) => {
                let mut results: Vec<(u64, f32)> = matches
                    .keys
                    .into_iter()
                    .zip(matches.distances)
                    .map(|(key, dist)| (key, 1.0_f32 - dist))
                    .collect();
                results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                results
            }
            Err(e) => {
                tracing::error!("HnswIndex::search: {e}");
                vec![]
            }
        }
    }

    fn len(&self) -> usize {
        self.count
    }

    fn dim(&self) -> Option<usize> {
        if self.count == 0 {
            None
        } else {
            self.dim
        }
    }
}

#[cfg(target_os = "macos")]
impl VectorIndex for HnswIndex {
    fn insert(&mut self, id: u64, embedding: &[f32]) {
        if embedding.is_empty() {
            self.delete(id);
            return;
        }

        if let Some(expected_dim) = self.dim {
            if expected_dim != embedding.len() {
                tracing::warn!(
                    "HnswIndex::insert: dimension mismatch (expected {:?}, got {}), skipping id={}",
                    self.dim,
                    embedding.len(),
                    id
                );
                return;
            }
        } else {
            self.dim = Some(embedding.len());
        }

        self.inner.insert(id, embedding);
    }

    fn delete(&mut self, id: u64) -> bool {
        let deleted = self.inner.delete(id);
        if self.inner.is_empty() {
            self.dim = None;
        }
        deleted
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<(u64, f32)> {
        self.inner.search(query, k)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn dim(&self) -> Option<usize> {
        self.dim
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hnsw_basic_search() {
        let mut index = HnswIndex::new();
        index.insert(1, &[1.0_f32, 0.0, 0.0]);
        index.insert(2, &[0.0, 1.0, 0.0]);
        index.insert(3, &[0.9, 0.1, 0.0]);

        let results = index.search(&[1.0, 0.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        // Node 1 is the exact match — must be ranked first.
        assert_eq!(results[0].0, 1, "exact match should be first");
        // Node 3 is more similar to the query than node 2.
        assert_eq!(results[1].0, 3);
    }

    #[test]
    fn test_hnsw_delete() {
        let mut index = HnswIndex::new();
        index.insert(1, &[1.0_f32, 0.0]);
        index.insert(2, &[0.0, 1.0]);

        assert!(index.delete(1));
        assert!(!index.delete(1)); // already removed
        assert_eq!(index.len(), 1);

        let results = index.search(&[1.0, 0.0], 5);
        assert!(results.iter().all(|(id, _)| *id != 1));
    }

    #[test]
    fn test_hnsw_dim_mismatch_ignored() {
        let mut index = HnswIndex::new();
        index.insert(1, &[1.0_f32, 0.0]);
        // Different dimension — must be silently skipped.
        index.insert(2, &[0.0, 1.0, 0.0]);
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_hnsw_upsert() {
        let mut index = HnswIndex::new();
        index.insert(1, &[1.0_f32, 0.0]);
        // Re-insert same id with different vector — should not panic.
        index.insert(1, &[0.5, 0.5]);
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_hnsw_empty_search() {
        let index = HnswIndex::new();
        assert_eq!(index.search(&[1.0, 0.0], 5), vec![]);
    }

    #[test]
    fn test_hnsw_dim() {
        let mut index = HnswIndex::new();
        assert_eq!(index.dim(), None);
        index.insert(1, &[1.0_f32, 0.0, 0.0]);
        assert_eq!(index.dim(), Some(3));
    }

    #[test]
    fn test_hnsw_empty_upsert_removes_existing_vector() {
        let mut index = HnswIndex::new();
        index.insert(1, &[1.0_f32, 0.0]);
        index.insert(2, &[0.0, 1.0]);

        index.insert(1, &[]);

        assert_eq!(index.len(), 1);
        let results = index.search(&[1.0, 0.0], 5);
        assert!(results.iter().all(|(id, _)| *id != 1));
    }

    #[test]
    fn test_hnsw_reset_after_last_delete_allows_new_dimension() {
        let mut index = HnswIndex::new();
        index.insert(1, &[1.0_f32, 0.0]);
        assert!(index.delete(1));
        assert_eq!(index.len(), 0);
        assert_eq!(index.dim(), None);

        index.insert(2, &[1.0_f32, 0.0, 0.0]);

        assert_eq!(index.len(), 1);
        assert_eq!(index.dim(), Some(3));
        let results = index.search(&[1.0, 0.0, 0.0], 1);
        assert_eq!(results[0].0, 2);
    }
}
