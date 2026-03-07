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


if __name__ == "__main__":
    unittest.main()
