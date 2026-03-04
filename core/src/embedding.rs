use sha2::{Digest, Sha256};

pub fn deterministic_embedding(text: &str, model_id: &str, dims: usize) -> Vec<f32> {
    let dims = dims.max(1);

    let mut hasher = Sha256::new();
    hasher.update(model_id.as_bytes());
    hasher.update(text.as_bytes());
    let digest = hasher.finalize();

    let mut out = Vec::with_capacity(dims);
    for i in 0..dims {
        let byte = digest[i % digest.len()];
        let value = (byte as f32 / 127.5) - 1.0;
        out.push(value);
    }

    out
}

pub fn cosine_similarity(a: &[f32], b: &[f32]) -> Option<f32> {
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
    fn deterministic_embedding_is_reproducible_for_same_inputs() {
        let a = deterministic_embedding("hello", "embedding-default-v1", 8);
        let b = deterministic_embedding("hello", "embedding-default-v1", 8);
        assert_eq!(a, b);
    }

    #[test]
    fn deterministic_embedding_changes_when_model_changes() {
        let a = deterministic_embedding("hello", "embedding-default-v1", 8);
        let b = deterministic_embedding("hello", "embedding-alt-v1", 8);
        assert_ne!(a, b);
    }
}
