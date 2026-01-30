use alayasiki_core::ingest::Chunk;
use async_trait::async_trait;
use text_splitter::TextSplitter;
use std::collections::HashMap;

#[async_trait]
pub trait Chunker: Send + Sync {
    async fn chunk(&self, content: &str, base_metadata: HashMap<String, String>) -> Vec<Chunk>;
}

pub struct SemanticChunker {
    splitter: TextSplitter<text_splitter::Characters>,
}

impl SemanticChunker {
    pub fn new(_max_chars: usize) -> Self {
        Self {
            splitter: TextSplitter::default().with_trim_chunks(true),
        }
    }
}

impl Default for SemanticChunker {
    fn default() -> Self {
        Self::new(1000) // Default ~1000 chars per chunk
    }
}

#[async_trait]
impl Chunker for SemanticChunker {
    async fn chunk(&self, content: &str, base_metadata: HashMap<String, String>) -> Vec<Chunk> {
        let chunks: Vec<_> = self.splitter.chunks(content, 1000).collect(); // Using default max_chars 1000 for now, should be configurable
        
        chunks.into_iter().enumerate().map(|(i, text)| {
            let mut metadata = base_metadata.clone();
            metadata.insert("chunk_index".to_string(), i.to_string());
            metadata.insert("chunk_chars".to_string(), text.len().to_string());
            
            Chunk {
                content: text.to_string(),
                metadata,
                embedding: None, // Placeholder
            }
        }).collect()
    }
}
