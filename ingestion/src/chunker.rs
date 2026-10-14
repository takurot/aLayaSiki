use alayasiki_core::ingest::Chunk;
use std::collections::HashMap;
use text_splitter::TextSplitter;

#[derive(Debug, Clone)]
/// Character limits apply to base chunks; overlap is added afterward.
/// A zero limit is clamped to one and overlap is capped at the base limit.
pub struct ChunkingConfig {
    pub max_chars: usize,
    pub overlap_chars: usize,
}

impl Default for ChunkingConfig {
    fn default() -> Self {
        Self {
            max_chars: 1000,
            overlap_chars: 100,
        }
    }
}

impl From<alayasiki_core::config::ChunkingSettings> for ChunkingConfig {
    fn from(settings: alayasiki_core::config::ChunkingSettings) -> Self {
        Self {
            max_chars: settings.max_chars,
            overlap_chars: settings.overlap_chars,
        }
    }
}

use std::future::Future;
use std::pin::Pin;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait Chunker: Send + Sync {
    fn chunk<'a>(
        &'a self,
        content: &'a str,
        base_metadata: HashMap<String, String>,
    ) -> BoxFuture<'a, Vec<Chunk>>;
}

/// Deterministic character-limited splitting with fixed tail overlap.
/// Uses text-splitter whitespace/structure heuristics, not a semantic model.
pub struct CharacterChunker {
    splitter: TextSplitter<text_splitter::Characters>,
    config: ChunkingConfig,
}

impl CharacterChunker {
    pub fn new(config: ChunkingConfig) -> Self {
        Self {
            splitter: TextSplitter::default().with_trim_chunks(true),
            config,
        }
    }
}

impl Default for CharacterChunker {
    fn default() -> Self {
        Self::new(ChunkingConfig::default())
    }
}

impl Chunker for CharacterChunker {
    fn chunk<'a>(
        &'a self,
        content: &'a str,
        base_metadata: HashMap<String, String>,
    ) -> BoxFuture<'a, Vec<Chunk>> {
        Box::pin(async move {
            let max_chars = self.config.max_chars.max(1);
            let overlap_chars = self.config.overlap_chars.min(max_chars);

            let base_chunks: Vec<String> = self
                .splitter
                .chunks(content, max_chars)
                .map(|chunk| chunk.to_string())
                .collect();

            let mut out = Vec::with_capacity(base_chunks.len());
            for (i, text) in base_chunks.iter().enumerate() {
                let mut chunk_text = text.clone();
                if overlap_chars > 0 && i > 0 {
                    let overlap = tail_chars(&base_chunks[i - 1], overlap_chars);
                    if !overlap.is_empty() {
                        chunk_text = format!("{}{}", overlap, chunk_text);
                    }
                }

                let mut metadata = base_metadata.clone();
                metadata.insert("chunk_index".to_string(), i.to_string());
                metadata.insert(
                    "chunk_chars".to_string(),
                    chunk_text.chars().count().to_string(),
                );
                metadata.insert("chunk_bytes".to_string(), chunk_text.len().to_string());
                metadata.insert("chunk_strategy".to_string(), "character".to_string());
                metadata.insert("chunk_overlap".to_string(), overlap_chars.to_string());

                out.push(Chunk {
                    content: chunk_text,
                    metadata,
                    embedding: None,
                });
            }

            out
        })
    }
}

fn tail_chars(text: &str, count: usize) -> String {
    if count == 0 {
        return String::new();
    }

    let mut chars: Vec<char> = text.chars().rev().take(count).collect();
    chars.reverse();
    chars.into_iter().collect()
}
