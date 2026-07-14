# PR-14.6 Operational Matrix Summary

- Scope: WAL flush policy comparison, `10^5 -> 10^6` scale sweep, worker degradation curve (`8/32/128`).

## WAL Flush Policy
- WAL flush policy: always
  nodes=100000, workers=8, wal=always, throughput=91.92 ops/s, read p95=176.66 ms, write p95=469.56 ms (durable), final flush=5.12 ms
  vs baseline: throughput=+0.00%, read p95=+0.00 ms, write p95=+0.00 ms, final flush=+0.00 ms
- WAL flush policy: interval(15ms)
  nodes=100000, workers=8, wal=interval:15ms, throughput=88.20 ops/s, read p95=121.05 ms, write p95=627.34 ms (submit_only), final flush=5.13 ms
  vs baseline: throughput=-4.04%, read p95=-55.61 ms, write p95=n/a (scope mismatch), final flush=+0.00 ms
- WAL flush policy: batch(32)
  nodes=100000, workers=8, wal=batch:32, throughput=684.92 ops/s, read p95=24.41 ms, write p95=137.20 ms (submit_only), final flush=3.53 ms
  vs baseline: throughput=+645.17%, read p95=-152.25 ms, write p95=n/a (scope mismatch), final flush=-1.59 ms

## Scale Sweep
- Scale sweep: 100k nodes
  nodes=100000, workers=8, wal=batch:32, throughput=517.44 ops/s, read p95=36.63 ms, write p95=157.19 ms (submit_only), final flush=6.11 ms
  vs baseline: throughput=+0.00%, read p95=+0.00 ms, write p95=+0.00 ms, final flush=+0.00 ms
- Scale sweep: 1M nodes
  nodes=1000000, workers=8, wal=batch:32, throughput=53.94 ops/s, read p95=255.78 ms, write p95=1560.09 ms (submit_only), final flush=7.21 ms
  vs baseline: throughput=-89.58%, read p95=+219.15 ms, write p95=+1402.91 ms, final flush=+1.10 ms

## Worker Sweep
- Worker sweep: 8 workers
  nodes=100000, workers=8, wal=batch:32, throughput=828.37 ops/s, read p95=33.29 ms, write p95=125.30 ms (submit_only), final flush=5.86 ms
  vs baseline: throughput=+0.00%, read p95=+0.00 ms, write p95=+0.00 ms, final flush=+0.00 ms
- Worker sweep: 32 workers
  nodes=100000, workers=32, wal=batch:32, throughput=433.18 ops/s, read p95=350.21 ms, write p95=475.68 ms (submit_only), final flush=4.21 ms
  vs baseline: throughput=-47.71%, read p95=+316.92 ms, write p95=+350.38 ms, final flush=-1.65 ms
- Worker sweep: 128 workers
  nodes=100000, workers=128, wal=batch:32, throughput=243.24 ops/s, read p95=230.63 ms, write p95=5786.22 ms (submit_only), final flush=4.38 ms
  vs baseline: throughput=-70.64%, read p95=+197.34 ms, write p95=+5660.92 ms, final flush=-1.48 ms
