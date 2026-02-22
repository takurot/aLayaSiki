use crate::dsl::{QueryMode, QueryRequest, SearchMode};
use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

const UNICODE_NGRAM_SIZE: usize = 2;

/// Eviction policy for cache entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EvictionPolicy {
    /// Least Recently Used - evict the oldest accessed entry.
    #[default]
    Lru,
    /// Least Frequently Used - evict the least accessed entry.
    Lfu,
}

/// Configuration for semantic cache behavior.
#[derive(Debug, Clone)]
pub struct SemanticCacheConfig {
    /// Maximum number of entries to keep in cache.
    pub max_entries: usize,
    /// Minimum similarity score to consider a cache hit (0.0 - 1.0).
    pub similarity_threshold: f32,
    /// Time-to-live for cache entries. None means no expiration.
    pub ttl_seconds: Option<u64>,
    /// Minimum query length to be eligible for caching.
    pub min_query_length: usize,
    /// Whether the cache is enabled. If false, insert/lookup are no-ops.
    pub enabled: bool,
    /// Eviction policy when max_entries is reached.
    pub eviction_policy: EvictionPolicy,
}

impl Default for SemanticCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 256,
            similarity_threshold: 0.6,
            ttl_seconds: None,
            min_query_length: 3,
            enabled: true,
            eviction_policy: EvictionPolicy::Lru,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SemanticCacheKey {
    pub model_id: String,
    pub snapshot_id: String,
    pub mode: QueryMode,
    pub search_mode: SearchMode,
    pub effective_search_mode: SearchMode,
    pub top_k: usize,
    pub traversal_depth: u8,
    pub entity_type: Vec<String>,
    pub relation_type: Vec<String>,
    pub traversal_relation_types: Vec<String>,
    pub time_range_from: Option<String>,
    pub time_range_to: Option<String>,
    pub time_travel: Option<String>,
}

impl SemanticCacheKey {
    pub fn from_request(
        request: &QueryRequest,
        model_id: &str,
        snapshot_id: &str,
        effective_search_mode: SearchMode,
    ) -> Self {
        let mut entity_type = request.filters.entity_type.clone();
        entity_type.sort();
        entity_type.dedup();

        let mut relation_type = request.filters.relation_type.clone();
        relation_type.sort();
        relation_type.dedup();

        let mut traversal_relation_types = request.traversal.relation_types.clone();
        traversal_relation_types.sort();
        traversal_relation_types.dedup();

        Self {
            model_id: model_id.to_string(),
            snapshot_id: snapshot_id.to_string(),
            mode: request.mode,
            search_mode: request.search_mode,
            effective_search_mode,
            top_k: request.top_k,
            traversal_depth: request.traversal.depth,
            entity_type,
            relation_type,
            traversal_relation_types,
            time_range_from: request
                .filters
                .time_range
                .as_ref()
                .map(|range| range.from.clone()),
            time_range_to: request
                .filters
                .time_range
                .as_ref()
                .map(|range| range.to.clone()),
            time_travel: request.time_travel.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct SemanticCacheEntry<T> {
    key: SemanticCacheKey,
    normalized_query: String,
    query_tokens: HashSet<String>,
    value: T,
    created_at: Instant,
    access_count: usize,
    last_accessed: Instant,
}

#[derive(Debug, Clone)]
pub struct SemanticCache<T> {
    config: SemanticCacheConfig,
    entries: VecDeque<SemanticCacheEntry<T>>,
}

impl<T: Clone> SemanticCache<T> {
    pub fn with_config(config: SemanticCacheConfig) -> Self {
        Self {
            config,
            entries: VecDeque::new(),
        }
    }

    pub fn lookup(&mut self, key: &SemanticCacheKey, query: &str) -> Option<T> {
        if !self.config.enabled || self.entries.is_empty() {
            return None;
        }

        let normalized_query = normalize_query(query);
        let query_tokens = tokenize(&normalized_query);

        // Check min_query_length
        if query.len() < self.config.min_query_length {
            return None;
        }

        let mut best_match: Option<(usize, f32)> = None;

        for (idx, entry) in self.entries.iter().enumerate() {
            if &entry.key != key {
                continue;
            }

            // Check TTL expiration
            if let Some(ttl) = self.config.ttl_seconds {
                if entry.created_at.elapsed() > Duration::from_secs(ttl) {
                    continue;
                }
            }

            let score = query_similarity(
                &entry.normalized_query,
                &entry.query_tokens,
                &normalized_query,
                &query_tokens,
            );
            if score < self.config.similarity_threshold {
                continue;
            }

            let replace = best_match.is_none_or(|(best_idx, best_score)| {
                matches!(score.partial_cmp(&best_score), Some(Ordering::Greater))
                    || (matches!(score.partial_cmp(&best_score), Some(Ordering::Equal))
                        && idx > best_idx)
            });

            if replace {
                best_match = Some((idx, score));
            }
        }

        let (idx, _) = best_match?;
        let mut matched = self.entries.remove(idx)?;

        // Update access metadata
        matched.access_count = matched.access_count.saturating_add(1);
        matched.last_accessed = Instant::now();

        let value = matched.value.clone();
        self.entries.push_back(matched);
        Some(value)
    }

    pub fn insert(&mut self, key: SemanticCacheKey, query: &str, value: T) {
        if !self.config.enabled {
            return;
        }

        // Check min_query_length
        if query.len() < self.config.min_query_length {
            return;
        }

        if self.config.max_entries == 0 {
            return;
        }

        let normalized_query = normalize_query(query);
        let query_tokens = tokenize(&normalized_query);

        if let Some(existing_idx) = self
            .entries
            .iter()
            .position(|entry| entry.key == key && entry.normalized_query == normalized_query)
        {
            self.entries.remove(existing_idx);
        }

        // Evict if necessary based on eviction policy
        while self.entries.len() >= self.config.max_entries {
            self.evict_one();
        }

        let now = Instant::now();
        self.entries.push_back(SemanticCacheEntry {
            key,
            normalized_query,
            query_tokens,
            value,
            created_at: now,
            access_count: 0,
            last_accessed: now,
        });
    }

    fn evict_one(&mut self) {
        if self.entries.is_empty() {
            return;
        }

        let idx = match self.config.eviction_policy {
            EvictionPolicy::Lru => {
                // Find the entry with the oldest last_accessed time
                self.entries
                    .iter()
                    .enumerate()
                    .min_by(|(_, a), (_, b)| a.last_accessed.cmp(&b.last_accessed))
                    .map(|(idx, _)| idx)
                    .unwrap_or(0)
            }
            EvictionPolicy::Lfu => {
                // Find the entry with the lowest access_count
                self.entries
                    .iter()
                    .enumerate()
                    .min_by(|(_, a), (_, b)| a.access_count.cmp(&b.access_count))
                    .map(|(idx, _)| idx)
                    .unwrap_or(0)
            }
        };

        self.entries.remove(idx);
    }
}

fn normalize_query(query: &str) -> String {
    query
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn query_similarity(
    lhs_query: &str,
    lhs_tokens: &HashSet<String>,
    rhs_query: &str,
    rhs_tokens: &HashSet<String>,
) -> f32 {
    if lhs_query == rhs_query {
        return 1.0;
    }
    if lhs_tokens.is_empty() || rhs_tokens.is_empty() {
        return 0.0;
    }

    let intersection = lhs_tokens.intersection(rhs_tokens).count();
    let union = lhs_tokens.len() + rhs_tokens.len() - intersection;
    if union == 0 {
        return 0.0;
    }
    intersection as f32 / union as f32
}

fn tokenize(text: &str) -> HashSet<String> {
    let mut out = HashSet::new();
    let mut buffer = String::new();

    for ch in text.chars().flat_map(|ch| ch.to_lowercase()) {
        if ch.is_alphanumeric() || ch == '_' {
            buffer.push(ch);
        } else if !buffer.is_empty() {
            out.insert(buffer.clone());
            buffer.clear();
        }
    }

    if !buffer.is_empty() {
        out.insert(buffer);
    }

    let unicode_tokens: Vec<String> = out
        .iter()
        .filter(|token| !token.is_ascii())
        .cloned()
        .collect();
    for token in unicode_tokens {
        for ngram in char_ngrams(&token, UNICODE_NGRAM_SIZE) {
            out.insert(ngram);
        }
    }

    out
}

fn char_ngrams(token: &str, n: usize) -> Vec<String> {
    let chars: Vec<char> = token.chars().collect();
    if chars.is_empty() || n == 0 {
        return Vec::new();
    }
    if chars.len() <= n {
        return vec![token.to_string()];
    }

    chars
        .windows(n)
        .map(|window| window.iter().collect::<String>())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache_key(snapshot_id: &str) -> SemanticCacheKey {
        SemanticCacheKey {
            model_id: "embedding-default-v1".to_string(),
            snapshot_id: snapshot_id.to_string(),
            mode: QueryMode::Evidence,
            search_mode: SearchMode::Local,
            effective_search_mode: SearchMode::Local,
            top_k: 5,
            traversal_depth: 2,
            entity_type: Vec::new(),
            relation_type: Vec::new(),
            traversal_relation_types: Vec::new(),
            time_range_from: None,
            time_range_to: None,
            time_travel: None,
        }
    }

    #[test]
    fn cache_hits_for_semantically_equivalent_query() {
        let mut cache = SemanticCache::with_config(SemanticCacheConfig {
            max_entries: 16,
            similarity_threshold: 0.6,
            ..SemanticCacheConfig::default()
        });
        let key = cache_key("wal-lsn-10");
        cache.insert(key.clone(), "Toyota EV strategy in 2024", 42u64);

        let hit = cache.lookup(&key, "2024 Toyota EV strategy overview");
        assert_eq!(hit, Some(42));
    }

    #[test]
    fn cache_isolated_by_snapshot_id() {
        let mut cache = SemanticCache::with_config(SemanticCacheConfig::default());
        cache.insert(cache_key("wal-lsn-10"), "Toyota EV strategy", 1u64);

        let miss = cache.lookup(&cache_key("wal-lsn-11"), "Toyota EV strategy");
        assert_eq!(miss, None);
    }

    #[test]
    fn cache_evicts_old_entries_in_lru_order() {
        let mut cache = SemanticCache::with_config(SemanticCacheConfig {
            max_entries: 2,
            similarity_threshold: 0.6,
            ..SemanticCacheConfig::default()
        });
        let key = cache_key("wal-lsn-10");

        cache.insert(key.clone(), "query one", 1u64);
        cache.insert(key.clone(), "query two", 2u64);
        cache.insert(key.clone(), "query three", 3u64);

        assert_eq!(cache.lookup(&key, "query one"), None);
        assert_eq!(cache.lookup(&key, "query two"), Some(2));
        assert_eq!(cache.lookup(&key, "query three"), Some(3));
    }

    #[test]
    fn cache_respects_ttl_expiration() {
        let mut cache = SemanticCache::with_config(SemanticCacheConfig {
            max_entries: 16,
            similarity_threshold: 0.6,
            ttl_seconds: Some(1),
            ..SemanticCacheConfig::default()
        });
        let key = cache_key("wal-lsn-10");
        cache.insert(key.clone(), "test query", 42u64);

        // Should hit immediately
        let hit = cache.lookup(&key, "test query");
        assert_eq!(hit, Some(42));

        // Wait for TTL to expire
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Should miss after TTL expiration
        let miss = cache.lookup(&key, "test query");
        assert_eq!(miss, None);
    }

    #[test]
    fn cache_respects_min_query_length() {
        let mut cache = SemanticCache::with_config(SemanticCacheConfig {
            max_entries: 16,
            similarity_threshold: 0.6,
            min_query_length: 10,
            ..SemanticCacheConfig::default()
        });
        let key = cache_key("wal-lsn-10");

        // Short query should not be cached
        cache.insert(key.clone(), "hi", 42u64);
        let miss = cache.lookup(&key, "hi");
        assert_eq!(miss, None);

        // Long query should be cached
        cache.insert(key.clone(), "hello world query", 43u64);
        let hit = cache.lookup(&key, "hello world query");
        assert_eq!(hit, Some(43));
    }

    #[test]
    fn cache_can_be_disabled() {
        let mut cache = SemanticCache::with_config(SemanticCacheConfig {
            max_entries: 16,
            similarity_threshold: 0.6,
            enabled: false,
            ..SemanticCacheConfig::default()
        });
        let key = cache_key("wal-lsn-10");

        // Insert should not store when disabled
        cache.insert(key.clone(), "test query", 42u64);
        let miss = cache.lookup(&key, "test query");
        assert_eq!(miss, None);
    }

    #[test]
    fn cache_eviction_policy_lfu() {
        let mut cache = SemanticCache::with_config(SemanticCacheConfig {
            max_entries: 3,
            similarity_threshold: 0.6,
            eviction_policy: EvictionPolicy::Lfu,
            ..SemanticCacheConfig::default()
        });
        let key = cache_key("wal-lsn-10");

        // Insert three entries
        cache.insert(key.clone(), "query one", 1u64);
        cache.insert(key.clone(), "query two", 2u64);
        cache.insert(key.clone(), "query three", 3u64);

        // Access "query one" multiple times to increase frequency
        cache.lookup(&key, "query one");
        cache.lookup(&key, "query one");

        // Insert fourth entry - should evict least frequently used ("query two" or "query three")
        cache.insert(key.clone(), "query four", 4u64);

        // "query one" should still be present (high frequency)
        let hit = cache.lookup(&key, "query one");
        assert_eq!(hit, Some(1));
    }
}
