//! Chunker throughput benchmark — legacy shim vs direct RecursiveChunker.
//!
//! # Why
//! Phase 1 preserves the legacy API through a deprecated shim. This bench
//! confirms the shim does not introduce a meaningful overhead relative to a
//! direct `Chunker` trait call, and measures throughput across a range of
//! document sizes for future regression tracking.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
#[allow(deprecated)]
use ragloom::transform::chunker::{ChunkerConfig, chunk_document};
use ragloom::transform::chunker::{
    Chunker,
    recursive::{RecursiveChunker, RecursiveConfig},
    size::SizeMetric,
};

fn sample(size: usize) -> String {
    let base = "The quick brown fox jumps over the lazy dog. ";
    let mut s = String::with_capacity(size);
    let mut i = 0usize;
    while s.len() < size {
        s.push_str(base);
        i += 1;
        if i % 5 == 0 {
            s.push_str("\n\n");
        }
    }
    s.truncate(size);
    s
}

fn bench(c: &mut Criterion) {
    let sizes = [4 * 1024usize, 64 * 1024, 512 * 1024, 2 * 1024 * 1024];
    let mut group = c.benchmark_group("chunker");
    for &n in &sizes {
        let text = sample(n);
        group.throughput(Throughput::Bytes(text.len() as u64));

        group.bench_with_input(BenchmarkId::new("legacy_shim", n), &text, |b, text| {
            #[allow(deprecated)]
            let cfg = ChunkerConfig::new(512);
            b.iter(|| {
                #[allow(deprecated)]
                let _ = chunk_document(text, &cfg);
            });
        });

        group.bench_with_input(BenchmarkId::new("recursive_chars_512", n), &text, |b, text| {
            let chk = RecursiveChunker::new(RecursiveConfig {
                metric: SizeMetric::Chars,
                max_size: 512,
                min_size: 0,
                overlap: 0,
            })
            .unwrap();
            b.iter(|| chk.chunk(text).unwrap());
        });
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
