# PR-14 Benchmark Suite Summary

- Profile: `baseline`
- Design target: `100000000` nodes / `300000000` edges

## Operational Latency
- Throughput: `389.38` ops/s
- Read p95: `16.96` ms
- Write p95: `188.67` ms

## GraphRAG Quality
- Throughput: `221.73` ops/s
- Read p95: `32.89` ms
- Write p95: `141.74` ms
- Average groundedness: `0.5932`
- Evidence attachment rate: `1.0000`
- Answer-with-evidence rate: `1.0000`

## ANN Search
- usearch: `0.0035` sec
- faiss_flat: `0.0306` sec
