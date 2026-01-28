use criterion::{criterion_group, criterion_main, Criterion};

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("fib 20", |b| b.iter(|| 20 + 20)); // Placeholder
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
