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


@dataclass(frozen=True)
class OperationalScenario:
    slug: str
    family: str
    description: str
    env: dict[str, str]


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


def base_operational_env(
    *,
    nodes: int,
    workers: int,
    ops_per_worker: int,
    write_every: int = 10,
    flush_policy: str = "always",
    flush_interval_ms: int | None = None,
    flush_batch_entries: int | None = None,
    seed_batch_entries: int = 2048,
) -> dict[str, str]:
    env = {
        "ALAYASIKI_BENCH_NODES": str(nodes),
        "ALAYASIKI_BENCH_WORKERS": str(workers),
        "ALAYASIKI_BENCH_OPS_PER_WORKER": str(ops_per_worker),
        "ALAYASIKI_BENCH_WRITE_EVERY": str(write_every),
        "ALAYASIKI_BENCH_WAL_FLUSH_POLICY": flush_policy,
        "ALAYASIKI_BENCH_SEED_WAL_BATCH_MAX_ENTRIES": str(seed_batch_entries),
    }
    if flush_interval_ms is not None:
        env["ALAYASIKI_BENCH_WAL_FLUSH_INTERVAL_MS"] = str(flush_interval_ms)
    if flush_batch_entries is not None:
        env["ALAYASIKI_BENCH_WAL_FLUSH_BATCH_MAX_ENTRIES"] = str(flush_batch_entries)
    return env


def build_pr14_6_operational_scenarios() -> dict[str, list[OperationalScenario]]:
    return {
        "flush_policy": [
            OperationalScenario(
                slug="flush_always",
                family="flush_policy",
                description="WAL flush policy: always",
                env=base_operational_env(
                    nodes=100_000,
                    workers=8,
                    ops_per_worker=40,
                    flush_policy="always",
                ),
            ),
            OperationalScenario(
                slug="flush_interval_15ms",
                family="flush_policy",
                description="WAL flush policy: interval(15ms)",
                env=base_operational_env(
                    nodes=100_000,
                    workers=8,
                    ops_per_worker=40,
                    flush_policy="interval",
                    flush_interval_ms=15,
                ),
            ),
            OperationalScenario(
                slug="flush_batch_32",
                family="flush_policy",
                description="WAL flush policy: batch(32)",
                env=base_operational_env(
                    nodes=100_000,
                    workers=8,
                    ops_per_worker=40,
                    flush_policy="batch",
                    flush_batch_entries=32,
                ),
            ),
        ],
        "scale": [
            OperationalScenario(
                slug="scale_100k_nodes",
                family="scale",
                description="Scale sweep: 100k nodes",
                env=base_operational_env(
                    nodes=100_000,
                    workers=8,
                    ops_per_worker=20,
                    flush_policy="batch",
                    flush_batch_entries=32,
                ),
            ),
            OperationalScenario(
                slug="scale_1m_nodes",
                family="scale",
                description="Scale sweep: 1M nodes",
                env=base_operational_env(
                    nodes=1_000_000,
                    workers=8,
                    ops_per_worker=20,
                    flush_policy="batch",
                    flush_batch_entries=32,
                    seed_batch_entries=4096,
                ),
            ),
        ],
        "workers": [
            OperationalScenario(
                slug="workers_8",
                family="workers",
                description="Worker sweep: 8 workers",
                env=base_operational_env(
                    nodes=100_000,
                    workers=8,
                    ops_per_worker=30,
                    flush_policy="batch",
                    flush_batch_entries=32,
                ),
            ),
            OperationalScenario(
                slug="workers_32",
                family="workers",
                description="Worker sweep: 32 workers",
                env=base_operational_env(
                    nodes=100_000,
                    workers=32,
                    ops_per_worker=30,
                    flush_policy="batch",
                    flush_batch_entries=32,
                ),
            ),
            OperationalScenario(
                slug="workers_128",
                family="workers",
                description="Worker sweep: 128 workers",
                env=base_operational_env(
                    nodes=100_000,
                    workers=128,
                    ops_per_worker=30,
                    flush_policy="batch",
                    flush_batch_entries=32,
                ),
            ),
        ],
    }


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


def extract_operational_metrics(result: dict[str, Any]) -> dict[str, Any]:
    config = result.get("config", {})
    totals = result.get("totals", {})
    read_latency = result.get("read_latency_ns", {})
    write_latency = result.get("write_latency_ns", {})
    durability_barrier = result.get("durability_barrier", {})
    return {
        "nodes": config.get("nodes", 0),
        "workers": config.get("workers", 0),
        "ops_per_worker": config.get("ops_per_worker", 0),
        "write_every": config.get("write_every", 0),
        "read_to_write_ratio": config.get("read_to_write_ratio", ""),
        "wal_flush_policy": config.get("wal_flush_policy", ""),
        "seed_wal_flush_policy": config.get("seed_wal_flush_policy", ""),
        "write_latency_scope": config.get("write_latency_scope", ""),
        "throughput_ops_per_sec": totals.get("throughput_ops_per_sec", 0.0),
        "read_p95_ms": read_latency.get("p95_ms"),
        "write_p95_ms": write_latency.get("p95_ms"),
        "read_p99_ms": read_latency.get("p99_ms"),
        "write_p99_ms": write_latency.get("p99_ms"),
        "final_flush_ms": durability_barrier.get("final_flush_ms"),
    }


def validate_operational_metrics(metrics: dict[str, Any], scenario_slug: str) -> None:
    required_numeric_fields = ("nodes", "workers", "ops_per_worker", "write_every")
    missing_numeric = [field for field in required_numeric_fields if metrics[field] <= 0]
    required_text_fields = (
        "read_to_write_ratio",
        "wal_flush_policy",
        "seed_wal_flush_policy",
        "write_latency_scope",
    )
    missing_text = [field for field in required_text_fields if not metrics[field]]
    required_latency_fields = (
        "read_p95_ms",
        "write_p95_ms",
        "read_p99_ms",
        "write_p99_ms",
    )
    missing_latency = [
        field
        for field in required_latency_fields
        if not isinstance(metrics[field], (int, float)) or metrics[field] <= 0.0
    ]
    invalid_final_flush = not isinstance(metrics["final_flush_ms"], (int, float))
    if (
        missing_numeric
        or missing_text
        or missing_latency
        or invalid_final_flush
        or metrics["throughput_ops_per_sec"] <= 0.0
    ):
        details = []
        if missing_numeric:
            details.append(f"numeric={','.join(missing_numeric)}")
        if missing_text:
            details.append(f"text={','.join(missing_text)}")
        if missing_latency:
            details.append(f"latency={','.join(missing_latency)}")
        if invalid_final_flush:
            details.append("final_flush_ms_missing")
        if metrics["throughput_ops_per_sec"] <= 0.0:
            details.append("throughput_ops_per_sec<=0")
        raise ValueError(
            f"invalid operational result for scenario '{scenario_slug}': {'; '.join(details)}"
        )


def compute_relative_delta(value: float, baseline: float) -> float:
    if baseline == 0.0:
        return 0.0
    return ((value - baseline) / baseline) * 100.0


def build_pr14_6_operational_report(
    scenario_groups: dict[str, list[OperationalScenario]],
    results_dir: Path,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "analysis": "pr14_6_operational_matrix",
        "generated_at_unix": int(time.time()),
        "scenario_groups": {},
    }

    for family, scenarios in scenario_groups.items():
        family_rows: list[dict[str, Any]] = []
        baseline_metrics: dict[str, Any] | None = None

        for scenario in scenarios:
            scenario_path = results_dir / f"pr14_6_operational_{scenario.slug}.json"
            metrics = extract_operational_metrics(load_json(scenario_path))
            validate_operational_metrics(metrics, scenario.slug)
            row = {
                "slug": scenario.slug,
                "description": scenario.description,
                **metrics,
            }
            if baseline_metrics is None:
                baseline_metrics = metrics
                row["write_latency_comparable_to_baseline"] = True
                row["delta_vs_baseline"] = {
                    "throughput_pct": 0.0,
                    "read_p95_ms": 0.0,
                    "write_p95_ms": 0.0,
                    "final_flush_ms": 0.0,
                }
            else:
                write_latency_comparable = (
                    metrics["write_latency_scope"]
                    == baseline_metrics["write_latency_scope"]
                )
                row["write_latency_comparable_to_baseline"] = write_latency_comparable
                row["delta_vs_baseline"] = {
                    "throughput_pct": compute_relative_delta(
                        metrics["throughput_ops_per_sec"],
                        baseline_metrics["throughput_ops_per_sec"],
                    ),
                    "read_p95_ms": metrics["read_p95_ms"]
                    - baseline_metrics["read_p95_ms"],
                    "write_p95_ms": (
                        metrics["write_p95_ms"] - baseline_metrics["write_p95_ms"]
                        if write_latency_comparable
                        else None
                    ),
                    "final_flush_ms": metrics["final_flush_ms"]
                    - baseline_metrics["final_flush_ms"],
                }
            family_rows.append(row)

        report["scenario_groups"][family] = family_rows

    return report


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


def render_pr14_6_operational_summary(report: dict[str, Any]) -> str:
    lines = [
        "# PR-14.6 Operational Matrix Summary",
        "",
        "- Scope: WAL flush policy comparison, `10^5 -> 10^6` scale sweep, worker degradation curve (`8/32/128`).",
        "",
    ]

    labels = {
    }
    lines = [
        "# PR-14.6 Operational Matrix Summary",
        "",
        "- Scope: WAL flush policy comparison, `10^5 -> 10^6` scale sweep, worker degradation curve (`8/32/128`).",
        "",
    ]
    labels = {
        "flush_policy": "WAL Flush Policy",
        "scale": "Scale Sweep",
        "workers": "Worker Sweep",
    }

    for family in ("flush_policy", "scale", "workers"):
        rows = report["scenario_groups"].get(family, [])
        lines.append(f"## {labels[family]}")
        for row in rows:
            delta = row["delta_vs_baseline"]
            if delta["write_p95_ms"] is None:
                write_delta = "write p95=n/a (scope mismatch)"
            else:
                write_delta = f"write p95={delta['write_p95_ms']:+.2f} ms"
            lines.extend(
                [
                    f"- {row['description']}",
                    (
                        f"  nodes={row['nodes']}, workers={row['workers']}, "
                        f"wal={row['wal_flush_policy']}, throughput={row['throughput_ops_per_sec']:.2f} ops/s, "
                        f"read p95={row['read_p95_ms']:.2f} ms, "
                        f"write p95={row['write_p95_ms']:.2f} ms ({row['write_latency_scope']}), "
                        f"final flush={row['final_flush_ms']:.2f} ms"
                    ),
                    (
                        f"  vs baseline: throughput={delta['throughput_pct']:+.2f}%, "
                        f"read p95={delta['read_p95_ms']:+.2f} ms, "
                        f"{write_delta}, final flush={delta['final_flush_ms']:+.2f} ms"
                    ),
                ]
            )
        lines.append("")
    return "\n".join(lines)


def run_command(
    label: str,
    command: list[str],
    cwd: Path,
    extra_env: dict[str, str] | None = None,
    cleared_env_prefixes: tuple[str, ...] = (),
) -> None:
    env = {
        key: value
        for key, value in os.environ.items()
        if not any(key.startswith(prefix) for prefix in cleared_env_prefixes)
    }
    if extra_env:
        env.update(extra_env)
    print(f"[run] {label}: {' '.join(command)}")
    subprocess.run(command, cwd=cwd, env=env, check=True)


def run_operational_scenario(
    scenario: OperationalScenario,
    repo_root: Path,
    results_dir: Path,
) -> None:
    run_command(
        scenario.description,
        ["cargo", "bench", "-p", "prototypes", "--bench", "operational_latency_bench"],
        repo_root,
        {
            **scenario.env,
            "ALAYASIKI_BENCH_RESULTS_PATH": str(
                results_dir / f"pr14_6_operational_{scenario.slug}.json"
            ),
        },
        cleared_env_prefixes=("ALAYASIKI_BENCH_",),
    )


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
        cleared_env_prefixes=("ALAYASIKI_BENCH_",),
    )
    run_command(
        "graphrag production",
        ["cargo", "bench", "-p", "prototypes", "--bench", "graphrag_production_bench"],
        repo_root,
        {
            **profile.graphrag_env,
            "ALAYASIKI_GRAPHRAG_RESULTS_PATH": str(graphrag_result),
        },
        cleared_env_prefixes=("ALAYASIKI_GRAPHRAG_",),
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


def run_pr14_6_operational_analysis(repo_root: Path, results_dir: Path) -> dict[str, Any]:
    scenario_groups = build_pr14_6_operational_scenarios()
    for family in ("flush_policy", "scale", "workers"):
        for scenario in scenario_groups[family]:
            run_operational_scenario(scenario, repo_root, results_dir)

    report = build_pr14_6_operational_report(scenario_groups, results_dir)
    write_json(results_dir / "pr14_6_operational_matrix.json", report)
    write_text(
        results_dir / "pr14_6_operational_matrix.md",
        render_pr14_6_operational_summary(report),
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        default="suite",
        choices=["suite", "pr14-6-operational"],
        help="Run the original PR-14 benchmark suite or the PR-14.6 operational matrix.",
    )
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
    if args.mode == "suite":
        profile = build_profile(args.profile)
        report = run_suite(profile, repo_root, results_dir)
        print(render_markdown_summary(report))
        return

    report = run_pr14_6_operational_analysis(repo_root, results_dir)
    print(render_pr14_6_operational_summary(report))


if __name__ == "__main__":
    main()
