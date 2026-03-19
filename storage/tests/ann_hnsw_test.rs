/// Integration tests for the HNSW ANN index.
///
/// * Correctness — recall@k against `LinearAnnIndex` ground-truth.
/// * Compatibility — same insert/delete/search semantics as the linear index.
/// * Feature-flag — the default `hnsw` build uses `HnswIndex`; disabling the
///   feature silently falls back to `LinearAnnIndex` via `HyperIndex::new()`.
use storage::index::{LinearAnnIndex, VectorIndex};

#[cfg(feature = "hnsw")]
use storage::index::HnswIndex;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Pseudo-random f32 vector seeded deterministically.
fn make_vector(seed: u64, dim: usize) -> Vec<f32> {
    let mut v = Vec::with_capacity(dim);
    let mut x = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    for _ in 0..dim {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let f = ((x >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0;
        v.push(f);
    }
    // L2-normalise so cosine similarity == dot product (avoids zero-norm edge cases)
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 1e-8 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}

/// Recall@k: fraction of ground-truth top-k neighbours that appear in `found`.
fn recall_at_k(ground_truth: &[(u64, f32)], found: &[(u64, f32)]) -> f32 {
    if ground_truth.is_empty() {
        return 1.0;
    }
    let gt_ids: std::collections::HashSet<u64> = ground_truth.iter().map(|(id, _)| *id).collect();
    let found_ids: std::collections::HashSet<u64> = found.iter().map(|(id, _)| *id).collect();
    let hits = gt_ids.intersection(&found_ids).count();
    hits as f32 / ground_truth.len() as f32
}

// ---------------------------------------------------------------------------
// Recall@k regression gate
// ---------------------------------------------------------------------------

/// Build a corpus of `n` normalised random vectors, run `q` queries, and
/// verify that HNSW recall@k ≥ `min_recall` compared to the linear ground truth.
#[cfg(feature = "hnsw")]
fn recall_experiment(n: usize, dim: usize, k: usize, q: usize, min_recall: f32) {
    let mut linear = LinearAnnIndex::new();
    let mut hnsw = HnswIndex::new();

    for i in 0..n as u64 {
        let v = make_vector(i, dim);
        linear.insert(i, &v);
        hnsw.insert(i, &v);
    }

    let mut total_recall = 0.0_f32;
    for qi in 0..q as u64 {
        let query = make_vector(qi + n as u64 * 17, dim);
        let gt = linear.search(&query, k);
        let found = hnsw.search(&query, k);
        total_recall += recall_at_k(&gt, &found);
    }
    let avg_recall = total_recall / q as f32;
    assert!(
        avg_recall >= min_recall,
        "recall@{k} = {avg_recall:.3} < required {min_recall:.3}  (n={n}, dim={dim}, q={q})"
    );
}

#[cfg(feature = "hnsw")]
#[test]
fn test_hnsw_recall_small() {
    // Small corpus — HNSW should be near-perfect.
    recall_experiment(200, 64, 10, 20, 0.90);
}

#[cfg(feature = "hnsw")]
#[test]
fn test_hnsw_recall_medium() {
    // Medium corpus — enforce recall@10 ≥ 0.85.
    recall_experiment(1_000, 128, 10, 50, 0.85);
}

// ---------------------------------------------------------------------------
// Correctness / API parity with LinearAnnIndex
// ---------------------------------------------------------------------------

#[cfg(feature = "hnsw")]
#[test]
fn test_hnsw_insert_search_delete_parity() {
    let mut linear = LinearAnnIndex::new();
    let mut hnsw = HnswIndex::new();

    for i in 0..10u64 {
        let v = make_vector(i, 32);
        linear.insert(i, &v);
        hnsw.insert(i, &v);
    }

    let query = make_vector(999, 32);
    let gt = linear.search(&query, 5);
    let found = hnsw.search(&query, 5);
    // Expect at least the top-1 to match.
    assert_eq!(
        gt[0].0, found[0].0,
        "top-1 mismatch between Linear and HNSW"
    );

    // Delete node 0 from both and verify it disappears.
    assert!(linear.delete(0));
    assert!(hnsw.delete(0));
    for idx in [&linear as &dyn VectorIndex, &hnsw as &dyn VectorIndex] {
        let r = idx.search(&make_vector(0, 32), 10);
        assert!(
            r.iter().all(|(id, _)| *id != 0),
            "deleted node 0 still returned"
        );
    }
}

#[cfg(feature = "hnsw")]
#[test]
fn test_hnsw_len_and_dim() {
    let mut hnsw = HnswIndex::new();
    assert_eq!(hnsw.len(), 0);
    assert_eq!(hnsw.dim(), None);

    hnsw.insert(1, &[1.0_f32, 0.0, 0.0]);
    assert_eq!(hnsw.len(), 1);
    assert_eq!(hnsw.dim(), Some(3));

    hnsw.insert(2, &[0.0, 1.0, 0.0]);
    assert_eq!(hnsw.len(), 2);

    hnsw.delete(1);
    assert_eq!(hnsw.len(), 1);
}

#[cfg(feature = "hnsw")]
#[test]
fn test_hnsw_dimension_mismatch_skipped() {
    let mut hnsw = HnswIndex::new();
    hnsw.insert(1, &[1.0_f32, 0.0]);
    hnsw.insert(2, &[0.0, 1.0, 0.0]); // wrong dimension — must be skipped
    assert_eq!(hnsw.len(), 1, "mismatched-dim insert must be ignored");
}

// ---------------------------------------------------------------------------
// HyperIndex integration (feature-agnostic)
// ---------------------------------------------------------------------------

use storage::hyper_index::HyperIndex;

#[test]
fn test_hyper_index_uses_vector_index_trait() {
    let mut h = HyperIndex::new();
    h.insert_node(1, vec![1.0, 0.0]);
    h.insert_node(2, vec![0.0, 1.0]);
    h.insert_edge(1, 2, "linked", 1.0);

    let r = h.search_vector(&[1.0, 0.0], 1);
    assert_eq!(r[0].0, 1, "top-1 vector search should return node 1");

    let neighbours = h.expand_graph(1, 1);
    assert_eq!(neighbours[0].0, 2);

    // Delete and verify absence.
    h.remove_node(1);
    let r2 = h.search_vector(&[1.0, 0.0], 5);
    assert!(
        r2.iter().all(|(id, _)| *id != 1),
        "deleted node should not appear"
    );
}

#[test]
fn test_hyper_index_with_linear_fallback() {
    // Explicitly construct with LinearAnnIndex to verify the with_vector_index
    // constructor and the VectorIndex trait path work end-to-end.
    let mut h = HyperIndex::with_vector_index(Box::new(LinearAnnIndex::new()));
    h.insert_node(10, vec![1.0, 0.0, 0.0]);
    h.insert_node(11, vec![0.5, 0.5, 0.0]);

    let r = h.search_vector(&[1.0, 0.0, 0.0], 1);
    assert_eq!(r[0].0, 10);
}
