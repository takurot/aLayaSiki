import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "benchmark_suite.py"
SPEC = importlib.util.spec_from_file_location("benchmark_suite", MODULE_PATH)
benchmark_suite = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = benchmark_suite
SPEC.loader.exec_module(benchmark_suite)


class BenchmarkSuiteTests(unittest.TestCase):
    def test_build_profile_uses_named_baseline_workload(self) -> None:
        profile = benchmark_suite.build_profile("baseline")

        self.assertEqual(profile.name, "baseline")
        self.assertEqual(profile.design_target_nodes, 100_000_000)
        self.assertEqual(profile.design_target_edges, 300_000_000)
        self.assertEqual(profile.operational_env["ALAYASIKI_BENCH_WRITE_EVERY"], "10")
        self.assertEqual(profile.graphrag_env["ALAYASIKI_GRAPHRAG_WRITE_EVERY"], "10")

    def test_build_suite_report_collects_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            results_dir = Path(tmp)
            operational_path = results_dir / "operational.json"
            graphrag_path = results_dir / "graphrag.json"
            ann_path = results_dir / "ann.json"

            operational_path.write_text(
                """
                {
                  "benchmark": "operational_latency_bench",
                  "totals": {"throughput_ops_per_sec": 255.0},
                  "read_latency_ns": {"p95_ms": 21.0},
                  "write_latency_ns": {"p95_ms": 140.0}
                }
                """.strip(),
                encoding="utf-8",
            )
            graphrag_path.write_text(
                """
                {
                  "benchmark": "graphrag_production_bench",
                  "totals": {"throughput_ops_per_sec": 180.0},
                  "read_latency_ns": {"p95_ms": 33.0},
                  "write_latency_ns": {"p95_ms": 160.0},
                  "read_quality": {
                    "avg_groundedness": 0.81,
                    "evidence_attachment_rate": 0.97,
                    "answer_with_evidence_rate": 0.96
                  }
                }
                """.strip(),
                encoding="utf-8",
            )
            ann_path.write_text(
                """
                {
                  "metrics": {
                    "usearch": {"search_sec": 0.12},
                    "faiss_flat": {"search_sec": 0.35}
                  }
                }
                """.strip(),
                encoding="utf-8",
            )

            report = benchmark_suite.build_suite_report(
                profile=benchmark_suite.build_profile("baseline"),
                operational_result=operational_path,
                graphrag_result=graphrag_path,
                ann_result=ann_path,
            )

            self.assertEqual(report["profile"], "baseline")
            self.assertEqual(report["design_target"]["nodes"], 100_000_000)
            self.assertAlmostEqual(
                report["graph_rag"]["evidence_attachment_rate"], 0.97
            )
            self.assertAlmostEqual(
                report["graph_rag"]["answer_with_evidence_rate"], 0.96
            )
            self.assertAlmostEqual(report["ann"]["search_sec"]["usearch"], 0.12)

    def test_build_pr14_6_operational_scenarios_covers_required_sweeps(self) -> None:
        scenarios = benchmark_suite.build_pr14_6_operational_scenarios()

        self.assertEqual(
            [scenario.slug for scenario in scenarios["flush_policy"]],
            ["flush_always", "flush_interval_15ms", "flush_batch_32"],
        )
        self.assertEqual(
            [scenario.env["ALAYASIKI_BENCH_NODES"] for scenario in scenarios["scale"]],
            ["100000", "1000000"],
        )
        self.assertEqual(
            [scenario.env["ALAYASIKI_BENCH_WORKERS"] for scenario in scenarios["workers"]],
            ["8", "32", "128"],
        )

    def test_build_pr14_6_operational_report_computes_deltas(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            results_dir = Path(tmp)
            scenario_groups = benchmark_suite.build_pr14_6_operational_scenarios()
            fixture_rows = {
                "flush_always": {
                    "config": {
                        "nodes": 100000,
                        "workers": 8,
                        "ops_per_worker": 40,
                        "write_every": 10,
                        "read_to_write_ratio": "9:1",
                        "wal_flush_policy": "always",
                        "seed_wal_flush_policy": "batch:2048",
                    },
                    "totals": {"throughput_ops_per_sec": 100.0},
                    "read_latency_ns": {"p95_ms": 20.0, "p99_ms": 25.0},
                    "write_latency_ns": {"p95_ms": 120.0, "p99_ms": 140.0},
                },
                "flush_interval_15ms": {
                    "config": {
                        "nodes": 100000,
                        "workers": 8,
                        "ops_per_worker": 40,
                        "write_every": 10,
                        "read_to_write_ratio": "9:1",
                        "wal_flush_policy": "interval:15ms",
                        "seed_wal_flush_policy": "batch:2048",
                    },
                    "totals": {"throughput_ops_per_sec": 130.0},
                    "read_latency_ns": {"p95_ms": 22.0, "p99_ms": 28.0},
                    "write_latency_ns": {"p95_ms": 90.0, "p99_ms": 100.0},
                },
                "flush_batch_32": {
                    "config": {
                        "nodes": 100000,
                        "workers": 8,
                        "ops_per_worker": 40,
                        "write_every": 10,
                        "read_to_write_ratio": "9:1",
                        "wal_flush_policy": "batch:32",
                        "seed_wal_flush_policy": "batch:2048",
                    },
                    "totals": {"throughput_ops_per_sec": 160.0},
                    "read_latency_ns": {"p95_ms": 21.0, "p99_ms": 24.0},
                    "write_latency_ns": {"p95_ms": 70.0, "p99_ms": 85.0},
                },
                "scale_100k_nodes": {
                    "config": {
                        "nodes": 100000,
                        "workers": 8,
                        "ops_per_worker": 20,
                        "write_every": 10,
                        "read_to_write_ratio": "9:1",
                        "wal_flush_policy": "batch:32",
                        "seed_wal_flush_policy": "batch:2048",
                    },
                    "totals": {"throughput_ops_per_sec": 150.0},
                    "read_latency_ns": {"p95_ms": 18.0, "p99_ms": 21.0},
                    "write_latency_ns": {"p95_ms": 75.0, "p99_ms": 82.0},
                },
                "scale_1m_nodes": {
                    "config": {
                        "nodes": 1000000,
                        "workers": 8,
                        "ops_per_worker": 20,
                        "write_every": 10,
                        "read_to_write_ratio": "9:1",
                        "wal_flush_policy": "batch:32",
                        "seed_wal_flush_policy": "batch:4096",
                    },
                    "totals": {"throughput_ops_per_sec": 120.0},
                    "read_latency_ns": {"p95_ms": 28.0, "p99_ms": 33.0},
                    "write_latency_ns": {"p95_ms": 95.0, "p99_ms": 104.0},
                },
                "workers_8": {
                    "config": {
                        "nodes": 100000,
                        "workers": 8,
                        "ops_per_worker": 30,
                        "write_every": 10,
                        "read_to_write_ratio": "9:1",
                        "wal_flush_policy": "batch:32",
                        "seed_wal_flush_policy": "batch:2048",
                    },
                    "totals": {"throughput_ops_per_sec": 140.0},
                    "read_latency_ns": {"p95_ms": 19.0, "p99_ms": 22.0},
                    "write_latency_ns": {"p95_ms": 78.0, "p99_ms": 86.0},
                },
                "workers_32": {
                    "config": {
                        "nodes": 100000,
                        "workers": 32,
                        "ops_per_worker": 30,
                        "write_every": 10,
                        "read_to_write_ratio": "9:1",
                        "wal_flush_policy": "batch:32",
                        "seed_wal_flush_policy": "batch:2048",
                    },
                    "totals": {"throughput_ops_per_sec": 260.0},
                    "read_latency_ns": {"p95_ms": 26.0, "p99_ms": 31.0},
                    "write_latency_ns": {"p95_ms": 92.0, "p99_ms": 105.0},
                },
                "workers_128": {
                    "config": {
                        "nodes": 100000,
                        "workers": 128,
                        "ops_per_worker": 30,
                        "write_every": 10,
                        "read_to_write_ratio": "9:1",
                        "wal_flush_policy": "batch:32",
                        "seed_wal_flush_policy": "batch:2048",
                    },
                    "totals": {"throughput_ops_per_sec": 210.0},
                    "read_latency_ns": {"p95_ms": 41.0, "p99_ms": 48.0},
                    "write_latency_ns": {"p95_ms": 130.0, "p99_ms": 144.0},
                },
            }

            for slug, payload in fixture_rows.items():
                (results_dir / f"pr14_6_operational_{slug}.json").write_text(
                    benchmark_suite.json.dumps(payload),
                    encoding="utf-8",
                )

            report = benchmark_suite.build_pr14_6_operational_report(
                scenario_groups, results_dir
            )

            flush_rows = report["scenario_groups"]["flush_policy"]
            self.assertEqual(flush_rows[0]["delta_vs_baseline"]["throughput_pct"], 0.0)
            self.assertAlmostEqual(
                flush_rows[1]["delta_vs_baseline"]["throughput_pct"], 30.0
            )
            self.assertAlmostEqual(
                flush_rows[2]["delta_vs_baseline"]["write_p95_ms"], -50.0
            )

            scale_rows = report["scenario_groups"]["scale"]
            self.assertEqual(scale_rows[1]["nodes"], 1_000_000)
            self.assertAlmostEqual(
                scale_rows[1]["delta_vs_baseline"]["read_p95_ms"], 10.0
            )

            worker_rows = report["scenario_groups"]["workers"]
            self.assertEqual(worker_rows[2]["workers"], 128)
            self.assertAlmostEqual(
                worker_rows[2]["delta_vs_baseline"]["write_p95_ms"], 52.0
            )


if __name__ == "__main__":
    unittest.main()
