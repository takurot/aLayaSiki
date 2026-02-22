use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Default)]
pub struct QueryMetrics {
    pub total_queries: u64,
    pub cache_hits: u64,
    pub latencies: VecDeque<u64>, // microseconds
}

#[derive(Debug, Clone, Default)]
pub struct SlmMetrics {
    pub total_extractions: u64,
    pub total_confidence: f32,
    pub gpu_vram_usage_mb: u64,
}

pub struct MetricsCollector {
    state: Arc<Mutex<MetricsState>>,
}

struct MetricsState {
    query_metrics: QueryMetrics,
    slm_metrics: SlmMetrics,
    max_history: usize,
}

impl MetricsCollector {
    pub fn new(max_history: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(MetricsState {
                query_metrics: QueryMetrics::default(),
                slm_metrics: SlmMetrics::default(),
                max_history,
            })),
        }
    }

    pub fn record_query(&self, latency_us: u64, is_cache_hit: bool) {
        let mut state = self.state.lock().unwrap();
        state.query_metrics.total_queries += 1;
        if is_cache_hit {
            state.query_metrics.cache_hits += 1;
        }
        state.query_metrics.latencies.push_back(latency_us);
        if state.query_metrics.latencies.len() > state.max_history {
            state.query_metrics.latencies.pop_front();
        }
    }

    pub fn record_slm_extraction(&self, avg_confidence: f32) {
        let mut state = self.state.lock().unwrap();
        state.slm_metrics.total_extractions += 1;
        state.slm_metrics.total_confidence += avg_confidence;
    }

    pub fn set_gpu_usage(&self, vram_mb: u64) {
        let mut state = self.state.lock().unwrap();
        state.slm_metrics.gpu_vram_usage_mb = vram_mb;
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let state = self.state.lock().unwrap();
        let q = &state.query_metrics;
        let s = &state.slm_metrics;
        
        let mut sorted_latencies: Vec<u64> = q.latencies.iter().copied().collect();
        sorted_latencies.sort_unstable();

        let p50 = percentile(&sorted_latencies, 50.0);
        let p95 = percentile(&sorted_latencies, 95.0);
        let p99 = percentile(&sorted_latencies, 99.0);

        let hit_rate = if q.total_queries > 0 {
            q.cache_hits as f32 / q.total_queries as f32
        } else {
            0.0
        };

        let avg_extraction_confidence = if s.total_extractions > 0 {
            s.total_confidence / s.total_extractions as f32
        } else {
            0.0
        };

        MetricsSnapshot {
            total_queries: q.total_queries,
            hit_rate,
            p50,
            p95,
            p99,
            history_count: q.latencies.len(),
            avg_extraction_confidence,
            gpu_vram_usage_mb: s.gpu_vram_usage_mb,
        }
    }
}

fn percentile(sorted: &[u64], p: f32) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f32)).ceil() as usize;
    sorted[idx.saturating_sub(1).min(sorted.len() - 1)]
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricsSnapshot {
    pub total_queries: u64,
    pub hit_rate: f32,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub history_count: usize,
    pub avg_extraction_confidence: f32,
    pub gpu_vram_usage_mb: u64,
}
