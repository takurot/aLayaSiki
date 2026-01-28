import numpy as np
import time
import usearch
import faiss
import matplotlib.pyplot as plt

def benchmark_ann(n_samples=10000, n_dims=128):
    print(f"Benchmarking with N={n_samples}, Dims={n_dims}")
    
    # Generate random data
    data = np.random.rand(n_samples, n_dims).astype(np.float32)
    queries = np.random.rand(100, n_dims).astype(np.float32)

    # Normalize vectors for Cosine Similarity validity (Dot Product == Cosine)
    faiss.normalize_L2(data)
    faiss.normalize_L2(queries)

    results = {}

    # USEARCH
    print("Benchmarking USEARCH...")
    start = time.time()
    index_usearch = usearch.Index(ndim=n_dims, metric="cos", dtype="f32")
    index_usearch.add(np.arange(n_samples), data)
    build_time = time.time() - start
    
    start = time.time()
    matches = index_usearch.search(queries, 10)
    search_time = time.time() - start
    results['usearch'] = {'build': build_time, 'search': search_time}
    print(f"USEARCH: Build={build_time:.4f}s, Search={search_time:.4f}s")

    # FAISS (Flat IP - Brute Force for baseline)
    print("Benchmarking FAISS (Flat)...")
    start = time.time()
    index_faiss = faiss.IndexFlatIP(n_dims)
    index_faiss.add(data)
    build_time = time.time() - start

    start = time.time()
    D, I = index_faiss.search(queries, 10)
    search_time = time.time() - start
    results['faiss_flat'] = {'build': build_time, 'search': search_time}
    print(f"FAISS Flat: Build={build_time:.4f}s, Search={search_time:.4f}s")
    
    # Simple plot
    names = list(results.keys())
    search_times = [results[n]['search'] for n in names]
    
    plt.bar(names, search_times)
    plt.title('Search Time (100 queries)')
    plt.ylabel('Seconds')
    plt.savefig('ann_benchmark_results.png')
    print("Saved plot to ann_benchmark_results.png")

if __name__ == "__main__":
    benchmark_ann()
