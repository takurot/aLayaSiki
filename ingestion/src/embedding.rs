use std::future::Future;
use std::pin::Pin;

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
            alayasiki_core::embedding::deterministic_embedding(&text, &model_id, dims)
        })
    }
}
