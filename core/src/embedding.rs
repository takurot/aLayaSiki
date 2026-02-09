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
