# PR-14.6 Operational Matrix Summary

- Scope: WAL flush policy comparison, `10^5 -> 10^6` scale sweep, worker degradation curve (`8/32/128`).

## WAL Flush Policy
- WAL flush policy: always
  nodes=100000, workers=8, wal=always, throughput=126.52 ops/s, read p95=148.56 ms, write p95=252.12 ms (durable), final flush=4.33 ms
  vs baseline: throughput=+0.00%, read p95=+0.00 ms, write p95=+0.00 ms, final flush=+0.00 ms
- WAL flush policy: interval(15ms)
  nodes=100000, workers=8, wal=interval:15ms, throughput=140.45 ops/s, read p95=128.30 ms, write p95=290.10 ms (submit_only), final flush=5.40 ms
  vs baseline: throughput=+11.01%, read p95=-20.26 ms, write p95=n/a (scope mismatch), final flush=+1.08 ms
- WAL flush policy: batch(32)
  nodes=100000, workers=8, wal=batch:32, throughput=930.40 ops/s, read p95=21.01 ms, write p95=116.63 ms (submit_only), final flush=5.24 ms
  vs baseline: throughput=+635.39%, read p95=-127.55 ms, write p95=n/a (scope mismatch), final flush=+0.91 ms

## Scale Sweep
- Scale sweep: 100k nodes
  nodes=100000, workers=8, wal=batch:32, throughput=653.82 ops/s, read p95=26.57 ms, write p95=138.57 ms (submit_only), final flush=5.02 ms
  vs baseline: throughput=+0.00%, read p95=+0.00 ms, write p95=+0.00 ms, final flush=+0.00 ms
- Scale sweep: 1M nodes
  nodes=1000000, workers=8, wal=batch:32, throughput=45.40 ops/s, read p95=304.22 ms, write p95=2084.98 ms (submit_only), final flush=9.09 ms
  vs baseline: throughput=-93.06%, read p95=+277.65 ms, write p95=+1946.41 ms, final flush=+4.08 ms

## Worker Sweep
- Worker sweep: 8 workers
  nodes=100000, workers=8, wal=batch:32, throughput=803.39 ops/s, read p95=21.97 ms, write p95=123.68 ms (submit_only), final flush=5.15 ms
  vs baseline: throughput=+0.00%, read p95=+0.00 ms, write p95=+0.00 ms, final flush=+0.00 ms
- Worker sweep: 32 workers
  nodes=100000, workers=32, wal=batch:32, throughput=288.90 ops/s, read p95=528.05 ms, write p95=674.86 ms (submit_only), final flush=0.02 ms
  vs baseline: throughput=-64.04%, read p95=+506.08 ms, write p95=+551.18 ms, final flush=-5.13 ms
- Worker sweep: 128 workers
  nodes=100000, workers=128, wal=batch:32, throughput=189.47 ops/s, read p95=363.14 ms, write p95=7496.58 ms (submit_only), final flush=3.61 ms
  vs baseline: throughput=-76.42%, read p95=+341.17 ms, write p95=+7372.91 ms, final flush=-1.54 ms
