import argparse
import json
import time
from pathlib import Path

import faiss
import matplotlib.pyplot as plt
import numpy as np
import usearch


def benchmark_ann(n_samples=10000, n_dims=128, n_queries=100, top_k=10, seed=42):
    print(f"Benchmarking with N={n_samples}, Dims={n_dims}, Queries={n_queries}")
    np.random.seed(seed)

    data = np.random.rand(n_samples, n_dims).astype(np.float32)
    queries = np.random.rand(n_queries, n_dims).astype(np.float32)

    faiss.normalize_L2(data)
    faiss.normalize_L2(queries)

    results = {
        "config": {
            "n_samples": n_samples,
            "n_dims": n_dims,
            "n_queries": n_queries,
            "top_k": top_k,
            "seed": seed,
        },
        "metrics": {},
    }

    print("Benchmarking USEARCH...")
    start = time.time()
    index_usearch = usearch.Index(ndim=n_dims, metric="cos", dtype="f32")
    index_usearch.add(np.arange(n_samples), data)
    build_time = time.time() - start

    start = time.time()
    index_usearch.search(queries, top_k)
    search_time = time.time() - start
    results["metrics"]["usearch"] = {"build_sec": build_time, "search_sec": search_time}
    print(f"USEARCH: Build={build_time:.4f}s, Search={search_time:.4f}s")

    print("Benchmarking FAISS (Flat)...")
    start = time.time()
    index_faiss = faiss.IndexFlatIP(n_dims)
    index_faiss.add(data)
    build_time = time.time() - start

    start = time.time()
    index_faiss.search(queries, top_k)
    search_time = time.time() - start
    results["metrics"]["faiss_flat"] = {"build_sec": build_time, "search_sec": search_time}
    print(f"FAISS Flat: Build={build_time:.4f}s, Search={search_time:.4f}s")

    return results


def write_outputs(results, json_output: Path, png_output: Path):
    json_output.parent.mkdir(parents=True, exist_ok=True)
    with json_output.open("w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"Saved results to {json_output}")

    names = list(results["metrics"].keys())
    search_times = [results["metrics"][name]["search_sec"] for name in names]
    plt.bar(names, search_times)
    plt.title("Search Time")
    plt.ylabel("Seconds")
    plt.savefig(png_output)
    print(f"Saved plot to {png_output}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n-samples", type=int, default=10000)
    parser.add_argument("--n-dims", type=int, default=128)
    parser.add_argument("--n-queries", type=int, default=100)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--json-output",
        default="benchmarks/results/ann_latest.json",
    )
    parser.add_argument(
        "--png-output",
        default="benchmarks/results/ann_benchmark_results.png",
    )
    args = parser.parse_args()

    results = benchmark_ann(
        n_samples=args.n_samples,
        n_dims=args.n_dims,
        n_queries=args.n_queries,
        top_k=args.top_k,
        seed=args.seed,
    )
    write_outputs(results, Path(args.json_output), Path(args.png_output))


if __name__ == "__main__":
    main()
