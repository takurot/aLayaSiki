# End-to-End Evaluation Setup

This document covers environment setup for running the Python benchmark
suite (`benchmarks/`) end to end, including the packaging pitfall that
motivated the root `pyproject.toml`.

## Python dependency constraints

The repository root `pyproject.toml` pins the benchmark dependency set:

```toml
[project]
name = "alayasiki-benchmarks"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "numpy<2.0.0",
    "usearch",
    "faiss-cpu",
    "matplotlib",
]
```

`requires-python = ">=3.12"` and the `numpy<2.0.0` pin exist to prevent
`PyBun` (and other PEP 621-aware resolvers) from silently resolving wheels
built for an older Python ABI (e.g. Python 3.10) than the interpreter
actually running the benchmarks. A mismatched wheel installs successfully
but fails at import time with errors such as:

```
ModuleNotFoundError: No module named 'numpy.core._multiarray_umath'
```

## Binding PyBun to the correct interpreter

Declaring `requires-python` is necessary but not sufficient: PyBun still
needs to be told which interpreter/virtualenv to target so it resolves
wheels against that interpreter's ABI rather than a default or system
Python. Explicitly bind `PYBUN_PYTHON` to the target virtualenv's
interpreter before installing packages:

```bash
python3 -m venv .venv-benchmarks
export PYBUN_PYTHON="$(pwd)/.venv-benchmarks/bin/python"
pybun install
```

Verify the resolved wheels match the intended interpreter before running
the suite:

```bash
"$PYBUN_PYTHON" -c "import numpy; print(numpy.__version__)"
```

## Running the benchmark suite

Once dependencies are installed into the target virtualenv, follow
`benchmarks/README.md` for the actual benchmark invocations, e.g.:

```bash
.venv-benchmarks/bin/python benchmarks/benchmark_suite.py --profile baseline
```
