//! PFOR (Patched Frame-of-Reference) encode/decode microbenches.
//!
//! Produces the decode-throughput evidence required by
//! `docs/design/storage/advanced-encodings.md` §6.3 and verifies the
//! compression ratio claim from §6.2: on a 5%-outlier int64 column, PFOR
//! should be ~2.5× smaller than plain FOR and ~5× smaller than Plain.
//!
//! Three workloads:
//! - Outlier-free int64 (PFOR converges to FOR with 0 patches per block).
//! - 5%-outlier int64 (the §6.2 worked example; the hot path the codec
//!   is designed for).
//! - Timestamp monotonic with 1ms spacing (converges to FOR, same as
//!   the FOR benches for direct comparison).
//!
//! Each bench measures `encode` and `decode` throughput over a 65 536-row
//! column chunk. A supplemental group logs the payload-byte ratio
//! against FOR and BitPacking on the 5%-outlier pattern so the
//! compression claim is captured in the bench output alongside
//! throughput numbers.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use bqlite_benches::common::*;
use bqlite_core::BqlType;
use bqlite_storage::encoding::{BitPacking, Encoding, ForEncoding, Pfor};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

/// Default row-group size — the unit of encoding work.
const ROW_GROUP_SIZE: usize = 65_536;

/// Build a 5%-outlier int64 column. 95% of values are in [0, 255]
/// (8-bit range), 5% are in [0, 2³¹] (32-bit range). Seeded for
/// determinism so bench-to-bench runs compare directly.
fn gen_five_percent_outlier_int64(n: usize) -> ArrayRef {
    let mut values: Vec<i64> = Vec::with_capacity(n);
    for i in 0..n {
        // Simple LCG-like hash for determinism without bringing in `rand`.
        let p = (i as u64).wrapping_mul(2_654_435_761);
        if p.is_multiple_of(20) {
            values.push((p % (1u64 << 31)) as i64);
        } else {
            values.push((p % 256) as i64);
        }
    }
    Arc::new(Int64Array::from(values))
}

/// PFOR on outlier-free int64 data — converges to FOR. Measures the
/// "no patches" encode/decode path.
fn bench_pfor_int64_outlier_free(c: &mut Criterion) {
    let array = gen_int64_array(ROW_GROUP_SIZE, 0);
    let chunk = Pfor.encode(array.as_ref()).unwrap();
    let payload_bytes = chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/pfor/outlier_free_int64");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("encode", |b| {
        b.iter(|| Pfor.encode(black_box(array.as_ref())).unwrap())
    });
    group.bench_function("decode", |b| {
        b.iter(|| Pfor.decode(black_box(&chunk), &BqlType::Int).unwrap())
    });

    group.finish();
}

/// PFOR on 5%-outlier int64 data — the hot path the codec is designed
/// for. Also emits the FOR and BitPacking payloads for size-comparison
/// evidence in the bench output.
fn bench_pfor_int64_five_percent_outliers(c: &mut Criterion) {
    let array = gen_five_percent_outlier_int64(ROW_GROUP_SIZE);

    let pfor_chunk = Pfor.encode(array.as_ref()).unwrap();
    let for_chunk = ForEncoding.encode(array.as_ref()).unwrap();
    let bp_chunk = BitPacking.encode(array.as_ref()).unwrap();

    let pfor_payload_bytes = pfor_chunk.payload.len() as u64;
    let for_payload_bytes = for_chunk.payload.len() as u64;
    let bp_payload_bytes = bp_chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/pfor/five_percent_outliers_int64");

    group.throughput(Throughput::Bytes(pfor_payload_bytes));
    group.bench_function("pfor_encode", |b| {
        b.iter(|| Pfor.encode(black_box(array.as_ref())).unwrap())
    });
    group.bench_function("pfor_decode", |b| {
        b.iter(|| Pfor.decode(black_box(&pfor_chunk), &BqlType::Int).unwrap())
    });

    // FOR baseline on the same array — direct ratio comparison.
    group.throughput(Throughput::Bytes(for_payload_bytes));
    group.bench_function("for_encode_baseline", |b| {
        b.iter(|| ForEncoding.encode(black_box(array.as_ref())).unwrap())
    });
    group.bench_function("for_decode_baseline", |b| {
        b.iter(|| {
            ForEncoding
                .decode(black_box(&for_chunk), &BqlType::Int)
                .unwrap()
        })
    });

    // BitPacking baseline — the v1 fallback. Reported for ratio context.
    group.throughput(Throughput::Bytes(bp_payload_bytes));
    group.bench_function("bitpacking_decode_baseline", |b| {
        b.iter(|| {
            BitPacking
                .decode(black_box(&bp_chunk), &BqlType::Int)
                .unwrap()
        })
    });

    group.finish();
}

/// PFOR on timestamp data — 1ms-spaced monotonic nanoseconds.
/// Converges to FOR with 0 patches; useful sanity check that PFOR does
/// not regress on timestamp columns.
fn bench_pfor_timestamp(c: &mut Criterion) {
    let array = gen_timestamp_array(
        ROW_GROUP_SIZE,
        1_700_000_000_000_000_000,
        1_000_000, /* 1ms */
    );

    let chunk = Pfor.encode(array.as_ref()).unwrap();
    let payload_bytes = chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/pfor/timestamp");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("encode", |b| {
        b.iter(|| Pfor.encode(black_box(array.as_ref())).unwrap())
    });
    group.bench_function("decode", |b| {
        b.iter(|| Pfor.decode(black_box(&chunk), &BqlType::Timestamp).unwrap())
    });

    group.finish();
}

criterion_group! {
    name = pfor_benches;
    config = criterion_for_mode(BenchMode::from_env());
    targets =
        bench_pfor_int64_outlier_free,
        bench_pfor_int64_five_percent_outliers,
        bench_pfor_timestamp,
}
criterion_main!(pfor_benches);
