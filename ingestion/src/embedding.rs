use std::pin::Pin;
use std::future::Future;
use sha2::{Digest, Sha256};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait Embedder: Send + Sync {
    fn embed<'a>(&'a self, text: &'a str, model_id: &'a str) -> BoxFuture<'a, Vec<f32>>;
}

pub struct DeterministicEmbedder {
    dims: usize,
}

impl DeterministicEmbedder {
    pub fn new(dims: usize) -> Self {
        Self { dims: dims.max(1) }
    }
}

impl Default for DeterministicEmbedder {
    fn default() -> Self {
        Self::new(768)
    }
}

impl Embedder for DeterministicEmbedder {
    fn embed<'a>(&'a self, text: &'a str, model_id: &'a str) -> BoxFuture<'a, Vec<f32>> {
        let text = text.to_string();
        let model_id = model_id.to_string();
        let dims = self.dims; // Capture copy
        
        Box::pin(async move {
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
        })
    }
}
