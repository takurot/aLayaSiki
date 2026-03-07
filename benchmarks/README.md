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

## Outputs

The runner writes:

- `benchmarks/results/operational_latency_<profile>.json`
- `benchmarks/results/graphrag_production_<profile>.json`
- `benchmarks/results/ann_<profile>.json`
- `benchmarks/results/ann_<profile>.png`
- `benchmarks/results/pr14_suite_<profile>.json`
- `benchmarks/results/pr14_suite_<profile>.md`

`pr14_suite_<profile>.md` is the top-level summary artifact for PR-14.
