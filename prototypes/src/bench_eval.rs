use serde::Serialize;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

fn average(sum: f64, count: usize) -> f64 {
    if count == 0 {
        0.0
    } else {
        sum / count as f64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct LatencySummary {
    pub p50_ns: u128,
    pub p95_ns: u128,
    pub p99_ns: u128,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

pub fn percentile_ns(samples: &[u128], percentile: f64) -> u128 {
    if samples.is_empty() {
        return 0;
    }

    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[rank]
}

pub fn to_ms(ns: u128) -> f64 {
    ns as f64 / 1_000_000.0
}

pub fn format_ns(ns: u128) -> String {
    if ns >= 1_000_000 {
        format!("{:.3} ms", to_ms(ns))
    } else if ns >= 1_000 {
        format!("{:.3} us", ns as f64 / 1_000.0)
    } else {
        format!("{ns} ns")
    }
}

pub fn build_latency_summary(samples: &[u128]) -> LatencySummary {
    if samples.is_empty() {
        return LatencySummary {
            p50_ns: 0,
            p95_ns: 0,
            p99_ns: 0,
            p50_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
        };
    }

    let mut sorted = samples.to_vec();
    sorted.sort_unstable();

    let get_p = |p: f64| {
        let rank = ((sorted.len() - 1) as f64 * p).round() as usize;
        sorted[rank]
    };

    let p50 = get_p(0.50);
    let p95 = get_p(0.95);
    let p99 = get_p(0.99);

    LatencySummary {
        p50_ns: p50,
        p95_ns: p95,
        p99_ns: p99,
        p50_ms: to_ms(p50),
        p95_ms: to_ms(p95),
        p99_ms: to_ms(p99),
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReadObservation {
    pub has_answer: bool,
    pub evidence_nodes: usize,
    pub citations: usize,
    pub groundedness: f32,
    pub semantic_cache_hit: bool,
}

impl ReadObservation {
    pub const fn new(
        has_answer: bool,
        evidence_nodes: usize,
        citations: usize,
        groundedness: f32,
        semantic_cache_hit: bool,
    ) -> Self {
        Self {
            has_answer,
            evidence_nodes,
            citations,
            groundedness,
            semantic_cache_hit,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct ReadQualitySummary {
    pub read_ops: usize,
    pub answer_reads: usize,
    pub avg_groundedness: f64,
    pub avg_answer_groundedness: f64,
    pub avg_evidence_nodes: f64,
    pub evidence_attachment_rate: f64,
    pub answer_with_evidence_rate: f64,
    pub answer_with_citations_rate: f64,
    pub semantic_cache_hit_rate: f64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReadQualityAccumulator {
    read_ops: usize,
    answer_reads: usize,
    cache_hits: usize,
    groundedness_sum: f64,
    answer_groundedness_sum: f64,
    evidence_nodes_sum: usize,
    evidence_attached_reads: usize,
    answer_with_evidence_reads: usize,
    answer_with_citations_reads: usize,
}

impl ReadQualityAccumulator {
    pub fn record(&mut self, observation: ReadObservation) {
        self.read_ops += 1;
        self.groundedness_sum += observation.groundedness as f64;
        self.evidence_nodes_sum += observation.evidence_nodes;

        if observation.semantic_cache_hit {
            self.cache_hits += 1;
        }
        if observation.evidence_nodes > 0 {
            self.evidence_attached_reads += 1;
        }
        if observation.has_answer {
            self.answer_reads += 1;
            self.answer_groundedness_sum += observation.groundedness as f64;

            if observation.evidence_nodes > 0 {
                self.answer_with_evidence_reads += 1;
            }
            if observation.citations > 0 {
                self.answer_with_citations_reads += 1;
            }
        }
    }

    pub fn merge(&mut self, other: Self) {
        self.read_ops += other.read_ops;
        self.answer_reads += other.answer_reads;
        self.cache_hits += other.cache_hits;
        self.groundedness_sum += other.groundedness_sum;
        self.answer_groundedness_sum += other.answer_groundedness_sum;
        self.evidence_nodes_sum += other.evidence_nodes_sum;
        self.evidence_attached_reads += other.evidence_attached_reads;
        self.answer_with_evidence_reads += other.answer_with_evidence_reads;
        self.answer_with_citations_reads += other.answer_with_citations_reads;
    }

    pub fn summary(&self) -> ReadQualitySummary {
        ReadQualitySummary {
            read_ops: self.read_ops,
            answer_reads: self.answer_reads,
            avg_groundedness: average(self.groundedness_sum, self.read_ops),
            avg_answer_groundedness: average(self.answer_groundedness_sum, self.answer_reads),
            avg_evidence_nodes: average(self.evidence_nodes_sum as f64, self.read_ops),
            evidence_attachment_rate: average(self.evidence_attached_reads as f64, self.read_ops),
            answer_with_evidence_rate: average(
                self.answer_with_evidence_reads as f64,
                self.answer_reads,
            ),
            answer_with_citations_rate: average(
                self.answer_with_citations_reads as f64,
                self.answer_reads,
            ),
            semantic_cache_hit_rate: average(self.cache_hits as f64, self.read_ops),
        }
    }
}

pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn write_json_report<T: Serialize>(path: &Path, report: &T) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }

    let payload = serde_json::to_vec_pretty(report).unwrap();
    std::fs::write(path, payload).unwrap();
}

#[cfg(test)]
mod tests {
    use super::{
        build_latency_summary, format_ns, percentile_ns, to_ms, ReadObservation,
        ReadQualityAccumulator,
    };

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-6,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn latency_summary_reports_percentiles_and_milliseconds() {
        let samples = [100_u128, 200, 300, 400, 500];

        let summary = build_latency_summary(&samples);

        assert_eq!(summary.p50_ns, 300);
        assert_eq!(summary.p95_ns, 500);
        assert_eq!(summary.p99_ns, 500);
        assert_close(summary.p50_ms, 0.0003);
        assert_close(summary.p95_ms, 0.0005);
        assert_close(summary.p99_ms, 0.0005);
        assert_eq!(percentile_ns(&samples, 0.0), 100);
        assert_eq!(format_ns(2_400), "2.400 us");
        assert_close(to_ms(1_250_000), 1.25);
    }

    #[test]
    fn read_quality_summary_tracks_attachment_and_answer_rates() {
        let mut accumulator = ReadQualityAccumulator::default();

        accumulator.record(ReadObservation::new(true, 2, 1, 0.8, true));
        accumulator.record(ReadObservation::new(true, 0, 0, 0.1, false));
        accumulator.record(ReadObservation::new(false, 1, 1, 0.4, false));
        accumulator.record(ReadObservation::new(false, 0, 0, 0.0, true));

        let summary = accumulator.summary();

        assert_eq!(summary.read_ops, 4);
        assert_eq!(summary.answer_reads, 2);
        assert_close(summary.avg_groundedness, 0.325);
        assert_close(summary.avg_answer_groundedness, 0.45);
        assert_close(summary.avg_evidence_nodes, 0.75);
        assert_close(summary.evidence_attachment_rate, 0.5);
        assert_close(summary.answer_with_evidence_rate, 0.5);
        assert_close(summary.answer_with_citations_rate, 0.5);
        assert_close(summary.semantic_cache_hit_rate, 0.5);
    }

    #[test]
    fn read_quality_accumulator_merges_workers() {
        let mut first = ReadQualityAccumulator::default();
        first.record(ReadObservation::new(true, 1, 1, 0.7, true));
        first.record(ReadObservation::new(false, 0, 0, 0.2, false));

        let mut second = ReadQualityAccumulator::default();
        second.record(ReadObservation::new(true, 2, 0, 0.9, false));

        first.merge(second);
        let summary = first.summary();

        assert_eq!(summary.read_ops, 3);
        assert_eq!(summary.answer_reads, 2);
        assert_close(summary.avg_groundedness, 0.6);
        assert_close(summary.avg_answer_groundedness, 0.8);
        assert_close(summary.avg_evidence_nodes, 1.0);
        assert_close(summary.evidence_attachment_rate, 2.0 / 3.0);
        assert_close(summary.answer_with_evidence_rate, 1.0);
        assert_close(summary.answer_with_citations_rate, 0.5);
        assert_close(summary.semantic_cache_hit_rate, 1.0 / 3.0);
    }

    #[test]
    fn read_quality_summary_defaults_to_zero_without_reads() {
        let summary = ReadQualityAccumulator::default().summary();

        assert_eq!(summary.read_ops, 0);
        assert_eq!(summary.answer_reads, 0);
        assert_close(summary.avg_groundedness, 0.0);
        assert_close(summary.avg_answer_groundedness, 0.0);
        assert_close(summary.avg_evidence_nodes, 0.0);
        assert_close(summary.evidence_attachment_rate, 0.0);
        assert_close(summary.answer_with_evidence_rate, 0.0);
        assert_close(summary.answer_with_citations_rate, 0.0);
        assert_close(summary.semantic_cache_hit_rate, 0.0);
    }
}
