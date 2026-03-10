# Benchmark Suite

`benchmarks/benchmark_suite.py` runs the PR-14 benchmark set and writes normalized artifacts into `benchmarks/results/`.

## Setup

Create a local virtual environment for the Python ANN benchmark:

```bash
python3 -m venv .venv-benchmarks
.venv-benchmarks/bin/pip install -r benchmarks/requirements.txt
```

## Run

Execute the baseline suite:

```bash
.venv-benchmarks/bin/python benchmarks/benchmark_suite.py --profile baseline
```

Run the larger manual profile:

```bash
.venv-benchmarks/bin/python benchmarks/benchmark_suite.py --profile scale
```

Run the `PR-14.6` operational matrix:

```bash
.venv-benchmarks/bin/python benchmarks/benchmark_suite.py --mode pr14-6-operational
```

## Outputs

The runner writes:

- `benchmarks/results/operational_latency_<profile>.json`
- `benchmarks/results/graphrag_production_<profile>.json`
- `benchmarks/results/ann_<profile>.json`
- `benchmarks/results/ann_<profile>.png`
- `benchmarks/results/pr14_suite_<profile>.json`
- `benchmarks/results/pr14_suite_<profile>.md`

`pr14_suite_<profile>.md` is the top-level summary artifact for PR-14.

When `--mode pr14-6-operational` is used, the runner also writes:

- `benchmarks/results/pr14_6_operational_<scenario>.json`
- `benchmarks/results/pr14_6_operational_matrix.json`
- `benchmarks/results/pr14_6_operational_matrix.md`

The PR-14.6 matrix covers:

- WAL flush policy comparison (`always`, `interval(15ms)`, `batch(32)`)
- Scale sweep (`10^5`, `10^6` nodes)
- Worker sweep (`8`, `32`, `128` workers)
