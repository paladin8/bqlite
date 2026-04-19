//! Wave 4 advanced-encoding comparison matrix (TASK-441).
//!
//! This bench file covers two complementary stories:
//!
//! 1. **ALP encode / decode throughput + compression ratio.** The ALP
//!    codec (TASK-417, `docs/design/storage/advanced-encodings.md` §8)
//!    has no per-encoding microbench yet; Wave 2 `encoding.rs` covers
//!    the v1 set plus RLE / DoubleDelta / FOR / FSST, and `wave4/pfor.rs`
//!    covers PFOR. This file closes the ALP gap.
//!
//! 2. **Same-fixture head-to-head matrix.** Side-by-side decode and
//!    payload-size measurements across the Wave 4 integer and string
//!    encodings on a single shared fixture per profile, so Criterion
//!    plots the comparison directly and `BenchResultCollector`
//!    captures the size-ratio invariants the selector (TASK-419)
//!    depends on.
//!
//! Frequency encoding (`advanced-encodings.md` §9.5) is explicitly
//! skipped — that document's recommendation is **NO-GO**, and there is
//! no shipping implementation to bench.
//!
//! Run with:
//! ```bash
//! cargo bench -p bqlite-benches --bench encoding_matrix
//! ```

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use bqlite_benches::common::*;
use bqlite_core::BqlType;
use bqlite_storage::encoding::{
    Alp, BitPacking, Delta, Dictionary, DoubleDelta, Encoding, ForEncoding, Plain, Rle,
};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

/// Default row-group size — the unit of encoding work. Matches
/// `bqlite_storage::writer::DEFAULT_ROW_GROUP_SIZE`.
const ROW_GROUP_SIZE: usize = 65_536;

// ─────────────────────────────────────────────────────────────────────────────
// Fixture generators
// ─────────────────────────────────────────────────────────────────────────────

/// Round-decimal f64 array — values drawn from a 5-element "price
/// catalogue" set. This is the ALP hot path per `advanced-encodings.md`
/// §8.2 (round values decompose at exponent 2 or 3 into a small
/// integer mantissa).
fn gen_round_float64(n: usize) -> ArrayRef {
    const CATALOGUE: [f64; 5] = [9.99, 19.95, 29.5, 49.0, 0.99];
    let values: Vec<f64> = (0..n).map(|i| CATALOGUE[i % CATALOGUE.len()]).collect();
    Arc::new(Float64Array::from(values))
}

/// Random-looking f64 array — values spread across the mantissa range
/// with no decimal structure. Exercises ALP's exception path.
///
/// Deterministic LCG-like generator (no `rand` dependency) so the
/// bench is reproducible across machines.
fn gen_random_float64(n: usize) -> ArrayRef {
    let values: Vec<f64> = (0..n)
        .map(|i| {
            let bits = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            // Normalise into [0, 1) then stretch into a broad range.
            (bits as f64) / (u64::MAX as f64) * 1e6
        })
        .collect();
    Arc::new(Float64Array::from(values))
}

/// Clustered int64 array — first half in [100, 116), second half in
/// [10_000, 10_016). Matches the `advanced-encodings.md` §5.2 worked
/// example (FOR beats global BitPacking because per-block minimums
/// shrink the range dramatically).
fn gen_clustered_int64(n: usize) -> ArrayRef {
    let values: Vec<i64> = (0..n as i64)
        .map(|i| {
            if i < (n as i64) / 2 {
                100 + (i % 16)
            } else {
                10_000 + (i % 16)
            }
        })
        .collect();
    Arc::new(Int64Array::from(values))
}

// ─────────────────────────────────────────────────────────────────────────────
// ALP microbenches
// ─────────────────────────────────────────────────────────────────────────────

/// ALP on round-decimal f64 — the codec's hot path.
///
/// Reports:
/// - `alp_decode_gb_per_sec` — decode throughput, floor ≥ 2.0 GB/s
///   (regression tripwire; see the plan's [floor] target table row).
/// - `alp_compression_ratio` — payload bytes / Plain payload bytes,
///   ceiling ≤ 0.40 ([spec] per `advanced-encodings.md` §8.2 — round
///   floats compress 3–5×).
fn bench_alp_round_f64(c: &mut Criterion) {
    let array = gen_round_float64(ROW_GROUP_SIZE);
    let source_bytes = (ROW_GROUP_SIZE * std::mem::size_of::<f64>()) as u64;

    let alp_chunk = Alp.encode(array.as_ref()).unwrap();
    let plain_chunk = Plain.encode(array.as_ref()).unwrap();
    let alp_payload_bytes = alp_chunk.payload.len() as u64;
    let plain_payload_bytes = plain_chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding_matrix/alp/round_f64");

    // Throughput reported in uncompressed source bytes so GB/s numbers
    // are directly comparable to the other float encodings below.
    group.throughput(Throughput::Bytes(source_bytes));
    group.bench_function("encode", |b| {
        b.iter(|| Alp.encode(black_box(array.as_ref())).unwrap())
    });
    group.bench_function("decode", |b| {
        b.iter(|| Alp.decode(black_box(&alp_chunk), &BqlType::Float).unwrap())
    });

    // Plain decode on the same array so the comparison is cache-fair.
    group.bench_function("plain_decode_baseline", |b| {
        b.iter(|| {
            Plain
                .decode(black_box(&plain_chunk), &BqlType::Float)
                .unwrap()
        })
    });
    group.finish();

    // Record the compression ratio as a bench target.
    let ratio = alp_payload_bytes as f64 / plain_payload_bytes.max(1) as f64;
    let mut collector = BenchResultCollector::new(BenchMode::from_env());
    collector.record(
        "encoding_matrix/alp/round_f64/compression_ratio",
        ratio,
        "ratio",
        Some(BenchTarget::at_most(0.40)),
    );
    // Standalone decode-throughput sample so the regression-floor
    // assertion has something to compare against in reference mode.
    if BenchMode::from_env().is_reference() {
        let runs = 32;
        let start = std::time::Instant::now();
        for _ in 0..runs {
            let out = Alp.decode(&alp_chunk, &BqlType::Float).unwrap();
            black_box(out);
        }
        let elapsed_s = start.elapsed().as_secs_f64();
        let gb_per_sec = (source_bytes as f64 * runs as f64) / elapsed_s / 1e9;
        collector.record(
            "encoding_matrix/alp/round_f64/decode_gb_per_sec",
            gb_per_sec,
            "GB/s",
            Some(BenchTarget::at_least(2.0)),
        );
    }
    collector.finish();
}

/// ALP on high-entropy f64 — exercises the patch-list path. No hard
/// target; regression tracking only.
fn bench_alp_random_f64(c: &mut Criterion) {
    let array = gen_random_float64(ROW_GROUP_SIZE);
    let source_bytes = (ROW_GROUP_SIZE * std::mem::size_of::<f64>()) as u64;

    let alp_chunk = Alp.encode(array.as_ref()).unwrap();
    let plain_chunk = Plain.encode(array.as_ref()).unwrap();

    let mut group = c.benchmark_group("encoding_matrix/alp/random_f64");
    group.throughput(Throughput::Bytes(source_bytes));

    group.bench_function("encode", |b| {
        b.iter(|| Alp.encode(black_box(array.as_ref())).unwrap())
    });
    group.bench_function("decode", |b| {
        b.iter(|| Alp.decode(black_box(&alp_chunk), &BqlType::Float).unwrap())
    });
    group.bench_function("plain_decode_baseline", |b| {
        b.iter(|| {
            Plain
                .decode(black_box(&plain_chunk), &BqlType::Float)
                .unwrap()
        })
    });
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Side-by-side integer comparison matrix
// ─────────────────────────────────────────────────────────────────────────────

/// Same-fixture head-to-head comparison across Plain / BitPacking /
/// Delta / DoubleDelta / FOR on a clustered int64 column.
///
/// The `[spec]` invariant from `advanced-encodings.md` §5.2 is also
/// enforced: on the clustered profile, FOR's payload must be smaller
/// than 75 % of BitPacking's payload.
fn bench_int_matrix_clustered(c: &mut Criterion) {
    let array = gen_clustered_int64(ROW_GROUP_SIZE);
    let source_bytes = (ROW_GROUP_SIZE * std::mem::size_of::<i64>()) as u64;

    let plain_chunk = Plain.encode(array.as_ref()).unwrap();
    let bp_chunk = BitPacking.encode(array.as_ref()).unwrap();
    let delta_chunk = Delta.encode(array.as_ref()).unwrap();
    let dd_chunk = DoubleDelta.encode(array.as_ref()).unwrap();
    let for_chunk = ForEncoding.encode(array.as_ref()).unwrap();

    let payload_sizes: [(&str, usize); 5] = [
        ("plain", plain_chunk.payload.len()),
        ("bitpacking", bp_chunk.payload.len()),
        ("delta", delta_chunk.payload.len()),
        ("double_delta", dd_chunk.payload.len()),
        ("for", for_chunk.payload.len()),
    ];

    let mut group = c.benchmark_group("encoding_matrix/int_clustered");
    group.throughput(Throughput::Bytes(source_bytes));

    // Decode throughput, one BenchmarkId per encoding.
    group.bench_with_input(BenchmarkId::new("decode", "plain"), &plain_chunk, |b, c| {
        b.iter(|| Plain.decode(black_box(c), &BqlType::Int).unwrap())
    });
    group.bench_with_input(
        BenchmarkId::new("decode", "bitpacking"),
        &bp_chunk,
        |b, c| b.iter(|| BitPacking.decode(black_box(c), &BqlType::Int).unwrap()),
    );
    group.bench_with_input(BenchmarkId::new("decode", "delta"), &delta_chunk, |b, c| {
        b.iter(|| Delta.decode(black_box(c), &BqlType::Int).unwrap())
    });
    group.bench_with_input(
        BenchmarkId::new("decode", "double_delta"),
        &dd_chunk,
        |b, c| b.iter(|| DoubleDelta.decode(black_box(c), &BqlType::Int).unwrap()),
    );
    group.bench_with_input(BenchmarkId::new("decode", "for"), &for_chunk, |b, c| {
        b.iter(|| ForEncoding.decode(black_box(c), &BqlType::Int).unwrap())
    });
    group.finish();

    // Payload-size snapshot (one row per encoding) — reported via
    // `BenchResultCollector` so the CI gate picks up size regressions,
    // not just throughput regressions.
    let mut collector = BenchResultCollector::new(BenchMode::from_env());
    for (name, payload_len) in &payload_sizes {
        let ratio = *payload_len as f64 / source_bytes as f64;
        collector.record(
            &format!("encoding_matrix/int_clustered/{name}/bytes_per_source_byte"),
            ratio,
            "ratio",
            None,
        );
    }

    // [spec] advanced-encodings.md §5.2 — FOR beats BitPacking on
    // the clustered profile. Enforced as a ratio, not a raw byte
    // count, so the assertion survives any future row-group-size
    // change.
    let for_vs_bp = for_chunk.payload.len() as f64 / bp_chunk.payload.len().max(1) as f64;
    collector.record(
        "encoding_matrix/int_clustered/for_vs_bitpacking_payload_ratio",
        for_vs_bp,
        "ratio",
        Some(BenchTarget::at_most(0.75)),
    );
    collector.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Side-by-side string comparison matrix
// ─────────────────────────────────────────────────────────────────────────────

/// Same-fixture head-to-head on low-cardinality strings across
/// Plain / Dictionary / RLE. FSST is intentionally out of this matrix
/// — `wave2/encoding.rs::bench_fsst_url_strings` and `bench_fsst_vs_baselines`
/// already cover FSST on the high-cardinality URL / user-agent
/// fixtures that FSST is actually designed for, and the codec returns
/// a corruption error on tiny-alphabet inputs like this 20-label
/// cycle. Adding it here would duplicate measurement without new
/// signal.
fn bench_string_matrix_low_cardinality(c: &mut Criterion) {
    let array = gen_low_cardinality_string_array(ROW_GROUP_SIZE, 20);
    // Source-byte count is the sum of element byte lengths; the
    // StringView array doesn't expose a single-number "logical" size,
    // so approximate it from the 20-label cycle.
    let avg_label_len = "event_19".len();
    let source_bytes = (ROW_GROUP_SIZE * avg_label_len) as u64;

    let plain_chunk = Plain.encode(array.as_ref()).unwrap();
    let dict_chunk = Dictionary.encode(array.as_ref()).unwrap();
    let rle_chunk = Rle.encode(array.as_ref()).unwrap();

    let mut group = c.benchmark_group("encoding_matrix/string_low_cardinality");
    group.throughput(Throughput::Bytes(source_bytes));

    group.bench_with_input(BenchmarkId::new("decode", "plain"), &plain_chunk, |b, c| {
        b.iter(|| Plain.decode(black_box(c), &BqlType::String).unwrap())
    });
    group.bench_with_input(
        BenchmarkId::new("decode", "dictionary"),
        &dict_chunk,
        |b, c| b.iter(|| Dictionary.decode(black_box(c), &BqlType::String).unwrap()),
    );
    group.bench_with_input(BenchmarkId::new("decode", "rle"), &rle_chunk, |b, c| {
        b.iter(|| Rle.decode(black_box(c), &BqlType::String).unwrap())
    });
    group.finish();

    // Payload-size snapshot.
    let mut collector = BenchResultCollector::new(BenchMode::from_env());
    for (name, payload) in [
        ("plain", &plain_chunk.payload),
        ("dictionary", &dict_chunk.payload),
        ("rle", &rle_chunk.payload),
    ] {
        let ratio = payload.len() as f64 / source_bytes as f64;
        collector.record(
            &format!("encoding_matrix/string_low_cardinality/{name}/bytes_per_source_byte"),
            ratio,
            "ratio",
            None,
        );
    }
    collector.finish();
}

criterion_group! {
    name = encoding_matrix_benches;
    config = criterion_for_mode(BenchMode::from_env());
    targets =
        bench_alp_round_f64,
        bench_alp_random_f64,
        bench_int_matrix_clustered,
        bench_string_matrix_low_cardinality,
}
criterion_main!(encoding_matrix_benches);
