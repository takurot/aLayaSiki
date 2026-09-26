# End-to-End Evaluation Setup

This document covers environment setup for running the Python benchmark
suite (`benchmarks/`) end to end, including the packaging pitfall that
motivated the root `pyproject.toml`.

## Python dependency constraints

The repository root `pyproject.toml` mirrors the pinned dependency set that
`benchmarks/requirements.txt` actually installs from:

```toml
[project]
name = "alayasiki-benchmarks"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "numpy<2.0.0",
    "usearch",
    "faiss-cpu",
    "matplotlib",
]
```

The root `pyproject.toml` is dependency metadata only (no `[build-system]`
table) — it is not installed directly by anything in this repository.
`requires-python = ">=3.11"` matches the Python version CI's
`python-ann-bench` job actually runs (`.github/workflows/ci.yml`).

The `numpy<2.0.0` pin exists because numpy 2.0 broke ABI compatibility with
C extensions (such as `faiss-cpu`) built against the numpy 1.x C-API. An
unpinned `numpy` can resolve a 2.x wheel that is incompatible with the
installed `faiss-cpu` wheel, installing successfully but failing at import
time with errors such as:

```
ModuleNotFoundError: No module named 'numpy.core._multiarray_umath'
```

## Installing into a virtualenv

Use an explicit interpreter version when creating the venv so the wheels
resolved match the interpreter actually used, rather than relying on
whatever `python3` happens to point to:

```bash
python3.11 -m venv .venv-benchmarks
.venv-benchmarks/bin/pip install -r benchmarks/requirements.txt
```

Verify the resolved numpy version before running the suite:

```bash
.venv-benchmarks/bin/python -c "import numpy; print(numpy.__version__)"
```

## Running the benchmark suite

Once dependencies are installed into the target virtualenv, follow
`benchmarks/README.md` for the actual benchmark invocations, e.g.:

```bash
.venv-benchmarks/bin/python benchmarks/benchmark_suite.py --profile baseline
```
