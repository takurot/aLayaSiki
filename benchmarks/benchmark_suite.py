from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SuiteProfile:
    name: str
    design_target_nodes: int
    design_target_edges: int
    operational_env: dict[str, str]
    graphrag_env: dict[str, str]
    ann_args: dict[str, str]


def build_profile(name: str) -> SuiteProfile:
    profiles = {
        "baseline": SuiteProfile(
            name="baseline",
            design_target_nodes=100_000_000,
            design_target_edges=300_000_000,
            operational_env={
                "ALAYASIKI_BENCH_NODES": "4000",
                "ALAYASIKI_BENCH_WORKERS": "6",
                "ALAYASIKI_BENCH_OPS_PER_WORKER": "100",
                "ALAYASIKI_BENCH_WRITE_EVERY": "10",
                "ALAYASIKI_BENCH_MAX_READ_P95_MS": "30",
                "ALAYASIKI_BENCH_MAX_WRITE_P95_MS": "200",
                "ALAYASIKI_BENCH_MIN_THROUGHPUT": "250",
            },
            graphrag_env={
                "ALAYASIKI_GRAPHRAG_SEED_NODES": "4000",
                "ALAYASIKI_GRAPHRAG_WORKERS": "6",
                "ALAYASIKI_GRAPHRAG_WARMUP_OPS": "20",
                "ALAYASIKI_GRAPHRAG_MEASURED_OPS": "100",
                "ALAYASIKI_GRAPHRAG_WRITE_EVERY": "10",
                "ALAYASIKI_GRAPHRAG_TOP_K": "24",
                "ALAYASIKI_GRAPHRAG_DEPTH": "2",
                "ALAYASIKI_GRAPHRAG_MIN_THROUGHPUT": "120",
                "ALAYASIKI_GRAPHRAG_MAX_READ_P95_MS": "80",
                "ALAYASIKI_GRAPHRAG_MAX_WRITE_P95_MS": "250",
            },
            ann_args={
                "n_samples": "10000",
                "n_dims": "128",
                "n_queries": "100",
                "top_k": "10",
                "seed": "42",
            },
        ),
        "scale": SuiteProfile(
            name="scale",
            design_target_nodes=1_000_000_000,
            design_target_edges=3_000_000_000,
            operational_env={
                "ALAYASIKI_BENCH_NODES": "20000",
                "ALAYASIKI_BENCH_WORKERS": "12",
                "ALAYASIKI_BENCH_OPS_PER_WORKER": "150",
                "ALAYASIKI_BENCH_WRITE_EVERY": "10",
            },
            graphrag_env={
                "ALAYASIKI_GRAPHRAG_SEED_NODES": "20000",
                "ALAYASIKI_GRAPHRAG_WORKERS": "12",
                "ALAYASIKI_GRAPHRAG_WARMUP_OPS": "20",
                "ALAYASIKI_GRAPHRAG_MEASURED_OPS": "150",
                "ALAYASIKI_GRAPHRAG_WRITE_EVERY": "10",
                "ALAYASIKI_GRAPHRAG_TOP_K": "24",
                "ALAYASIKI_GRAPHRAG_DEPTH": "2",
            },
            ann_args={
                "n_samples": "50000",
                "n_dims": "128",
                "n_queries": "200",
                "top_k": "10",
                "seed": "42",
            },
        ),
    }
    try:
        return profiles[name]
    except KeyError as exc:
        raise ValueError(f"unknown profile: {name}") from exc


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        print(f"[warn] JSON result file not found: {path}")
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"[error] Failed to load JSON from {path}: {exc}")
        return {}


def build_suite_report(
    profile: SuiteProfile,
    operational_result: Path,
    graphrag_result: Path,
    ann_result: Path,
) -> dict[str, Any]:
    operational = load_json(operational_result)
    graphrag = load_json(graphrag_result)
    ann = load_json(ann_result)

    # Use .get() to avoid KeyError if load_json returned empty dict
    return {
        "profile": profile.name,
        "generated_at_unix": int(time.time()),
        "design_target": {
            "nodes": profile.design_target_nodes,
            "edges": profile.design_target_edges,
        },
        "operational": {
            "throughput_ops_per_sec": operational.get("totals", {}).get(
                "throughput_ops_per_sec", 0.0
            ),
            "read_p95_ms": operational.get("read_latency_ns", {}).get("p95_ms", 0.0),
            "write_p95_ms": operational.get("write_latency_ns", {}).get("p95_ms", 0.0),
        },
        "graph_rag": {
            "throughput_ops_per_sec": graphrag.get("totals", {}).get(
                "throughput_ops_per_sec", 0.0
            ),
            "read_p95_ms": graphrag.get("read_latency_ns", {}).get("p95_ms", 0.0),
            "write_p95_ms": graphrag.get("write_latency_ns", {}).get("p95_ms", 0.0),
            "avg_groundedness": graphrag.get("read_quality", {}).get(
                "avg_groundedness", 0.0
            ),
            "evidence_attachment_rate": graphrag.get("read_quality", {}).get(
                "evidence_attachment_rate", 0.0
            ),
            "answer_with_evidence_rate": graphrag.get("read_quality", {}).get(
                "answer_with_evidence_rate", 0.0
            ),
        },
        "ann": {
            "search_sec": {
                name: metrics["search_sec"]
                for name, metrics in ann.get("metrics", {}).items()
                if "search_sec" in metrics
            }
        },
    }


def render_markdown_summary(report: dict[str, Any]) -> str:
    lines = [
        "# PR-14 Benchmark Suite Summary",
        "",
        f"- Profile: `{report['profile']}`",
        (
            "- Design target: "
            f"`{report['design_target']['nodes']}` nodes / "
            f"`{report['design_target']['edges']}` edges"
        ),
        "",
        "## Operational Latency",
        f"- Throughput: `{report['operational']['throughput_ops_per_sec']:.2f}` ops/s",
        f"- Read p95: `{report['operational']['read_p95_ms']:.2f}` ms",
        f"- Write p95: `{report['operational']['write_p95_ms']:.2f}` ms",
        "",
        "## GraphRAG Quality",
        f"- Throughput: `{report['graph_rag']['throughput_ops_per_sec']:.2f}` ops/s",
        f"- Read p95: `{report['graph_rag']['read_p95_ms']:.2f}` ms",
        f"- Write p95: `{report['graph_rag']['write_p95_ms']:.2f}` ms",
        f"- Average groundedness: `{report['graph_rag']['avg_groundedness']:.4f}`",
        (
            "- Evidence attachment rate: "
            f"`{report['graph_rag']['evidence_attachment_rate']:.4f}`"
        ),
        (
            "- Answer-with-evidence rate: "
            f"`{report['graph_rag']['answer_with_evidence_rate']:.4f}`"
        ),
        "",
        "## ANN Search",
    ]
    for name, value in report["ann"]["search_sec"].items():
        lines.append(f"- {name}: `{value:.4f}` sec")
    lines.append("")
    return "\n".join(lines)


def run_command(
    label: str,
    command: list[str],
    cwd: Path,
    extra_env: dict[str, str] | None = None,
) -> None:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    print(f"[run] {label}: {' '.join(command)}")
    subprocess.run(command, cwd=cwd, env=env, check=True)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def run_suite(profile: SuiteProfile, repo_root: Path, results_dir: Path) -> dict[str, Any]:
    operational_result = results_dir / f"operational_latency_{profile.name}.json"
    graphrag_result = results_dir / f"graphrag_production_{profile.name}.json"
    ann_result = results_dir / f"ann_{profile.name}.json"
    ann_plot = results_dir / f"ann_{profile.name}.png"

    run_command(
        "operational latency",
        ["cargo", "bench", "-p", "prototypes", "--bench", "operational_latency_bench"],
        repo_root,
        {
            **profile.operational_env,
            "ALAYASIKI_BENCH_RESULTS_PATH": str(operational_result),
        },
    )
    run_command(
        "graphrag production",
        ["cargo", "bench", "-p", "prototypes", "--bench", "graphrag_production_bench"],
        repo_root,
        {
            **profile.graphrag_env,
            "ALAYASIKI_GRAPHRAG_RESULTS_PATH": str(graphrag_result),
        },
    )
    run_command(
        "python ann benchmark",
        [
            sys.executable,
            "benchmarks/ann_benchmark.py",
            "--n-samples",
            profile.ann_args["n_samples"],
            "--n-dims",
            profile.ann_args["n_dims"],
            "--n-queries",
            profile.ann_args["n_queries"],
            "--top-k",
            profile.ann_args["top_k"],
            "--seed",
            profile.ann_args["seed"],
            "--json-output",
            str(ann_result),
            "--png-output",
            str(ann_plot),
        ],
        repo_root,
    )

    report = build_suite_report(profile, operational_result, graphrag_result, ann_result)
    write_json(results_dir / f"pr14_suite_{profile.name}.json", report)
    write_text(
        results_dir / f"pr14_suite_{profile.name}.md",
        render_markdown_summary(report),
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="baseline", choices=["baseline", "scale"])
    parser.add_argument(
        "--results-dir",
        default="benchmarks/results",
        help="Directory for JSON/Markdown benchmark outputs.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    requested_results_dir = Path(args.results_dir)
    if requested_results_dir.is_absolute():
        results_dir = requested_results_dir
    else:
        results_dir = (repo_root / requested_results_dir).resolve()
    profile = build_profile(args.profile)
    report = run_suite(profile, repo_root, results_dir)
    print(render_markdown_summary(report))


if __name__ == "__main__":
    main()
