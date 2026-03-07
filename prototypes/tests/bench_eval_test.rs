use prototypes::bench_eval::{build_latency_summary, ReadObservation, ReadQualityAccumulator};

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-6,
        "expected {expected}, got {actual}"
    );
}

#[test]
fn test_build_latency_summary_computes_percentiles_and_ms() {
    let summary = build_latency_summary(&[1_000, 2_000, 3_000, 4_000, 5_000]);

    assert_eq!(summary.p50_ns, 3_000);
    assert_eq!(summary.p95_ns, 5_000);
    assert_eq!(summary.p99_ns, 5_000);
    assert_eq!(summary.p50_ms, 0.003);
    assert_eq!(summary.p95_ms, 0.005);
    assert_eq!(summary.p99_ms, 0.005);
}

#[test]
fn test_read_quality_accumulator_derives_attachment_rates() {
    let mut accumulator = ReadQualityAccumulator::default();
    accumulator.record(ReadObservation::new(true, 3, 2, 0.9, true));
    accumulator.record(ReadObservation::new(true, 2, 0, 0.5, false));
    accumulator.record(ReadObservation::new(false, 0, 0, 0.4, false));
    accumulator.record(ReadObservation::new(false, 1, 0, 0.2, true));

    let summary = accumulator.summary();

    assert_close(summary.avg_groundedness, 0.5);
    assert_close(summary.avg_evidence_nodes, 1.5);
    assert_close(summary.semantic_cache_hit_rate, 0.5);
    assert_close(summary.evidence_attachment_rate, 0.75);
    assert_close(summary.answer_with_evidence_rate, 1.0);
    assert_close(summary.answer_with_citations_rate, 0.5);
}
