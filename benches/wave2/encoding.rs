//! Per-encoding encode/decode microbenches for the v1 encoding set.
//!
//! Covers the Wave 2 performance gate "encoding" line:
//! Plain, Dictionary, Delta, BitPacking, Constant, and the LZ4
//! post-encoding wrapper.
//!
//! Each benchmark measures `encode` and `decode` throughput on a
//! 65,536-row column chunk (the default row-group size) and reports
//! the `gb_per_sec_scanned` and `bytes_decoded_to_scanned` metrics
//! required by execution-model.md §14.1.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array, StringViewArray};
use bqlite_benches::common::*;
use bqlite_core::BqlType;
use bqlite_storage::encoding::{
    compress_lz4, decompress_lz4, BitPacking, Constant, Delta, Dictionary, Encoding, Plain,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

/// Default row-group size — the unit of encoding work.
const ROW_GROUP_SIZE: usize = 65_536;

// ── Plain ────────────────────────────────────────────────────────────────────

fn bench_plain_int64(c: &mut Criterion) {
    let array = gen_int64_array(ROW_GROUP_SIZE, 0);
    let plain = Plain::new();
    let chunk = plain.encode(array.as_ref()).unwrap();
    let payload_bytes = chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/plain/int64");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("encode", |b| {
        b.iter(|| plain.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("decode", |b| {
        b.iter(|| plain.decode(black_box(&chunk), &BqlType::Int).unwrap())
    });

    group.finish();
}

fn bench_plain_float64(c: &mut Criterion) {
    let array = gen_float64_array(ROW_GROUP_SIZE, 0.0);
    let plain = Plain::new();
    let chunk = plain.encode(array.as_ref()).unwrap();
    let payload_bytes = chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/plain/float64");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("encode", |b| {
        b.iter(|| plain.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("decode", |b| {
        b.iter(|| plain.decode(black_box(&chunk), &BqlType::Float).unwrap())
    });

    group.finish();
}

fn bench_plain_string(c: &mut Criterion) {
    let array = gen_low_cardinality_string_array(ROW_GROUP_SIZE, 20);
    let plain = Plain::new();
    let chunk = plain.encode(array.as_ref()).unwrap();
    let payload_bytes = chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/plain/string");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("encode", |b| {
        b.iter(|| plain.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("decode", |b| {
        b.iter(|| plain.decode(black_box(&chunk), &BqlType::String).unwrap())
    });

    group.finish();
}

// ── Dictionary ───────────────────────────────────────────────────────────────

fn bench_dictionary_int64(c: &mut Criterion) {
    // Low-cardinality int: 20 distinct values cycled over 65k rows.
    let values: Vec<i64> = (0..ROW_GROUP_SIZE as i64).map(|i| i % 20).collect();
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    let dict = Dictionary::new();
    let chunk = dict.encode(array.as_ref()).unwrap();
    let payload_bytes = chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/dictionary/int64");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("encode", |b| {
        b.iter(|| dict.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("decode", |b| {
        b.iter(|| dict.decode(black_box(&chunk), &BqlType::Int).unwrap())
    });

    group.finish();
}

fn bench_dictionary_string(c: &mut Criterion) {
    let array = gen_low_cardinality_string_array(ROW_GROUP_SIZE, 20);
    let dict = Dictionary::new();
    let chunk = dict.encode(array.as_ref()).unwrap();
    let payload_bytes = chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/dictionary/string");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("encode", |b| {
        b.iter(|| dict.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("decode", |b| {
        b.iter(|| dict.decode(black_box(&chunk), &BqlType::String).unwrap())
    });

    group.finish();
}

// ── Delta ────────────────────────────────────────────────────────────────────

fn bench_delta_timestamp(c: &mut Criterion) {
    // Monotonic timestamps with small deltas — Delta's sweet spot.
    let array = gen_timestamp_array(ROW_GROUP_SIZE, 1_700_000_000_000_000_000, 1_000_000);
    let delta = Delta::new();
    let chunk = delta.encode(array.as_ref()).unwrap();
    let payload_bytes = chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/delta/timestamp");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("encode", |b| {
        b.iter(|| delta.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("decode", |b| {
        b.iter(|| {
            delta
                .decode(black_box(&chunk), &BqlType::Timestamp)
                .unwrap()
        })
    });

    group.finish();
}

fn bench_delta_int64(c: &mut Criterion) {
    // Sequential integers — small constant deltas.
    let array = gen_int64_array(ROW_GROUP_SIZE, 0);
    let delta = Delta::new();
    let chunk = delta.encode(array.as_ref()).unwrap();
    let payload_bytes = chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/delta/int64");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("encode", |b| {
        b.iter(|| delta.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("decode", |b| {
        b.iter(|| delta.decode(black_box(&chunk), &BqlType::Int).unwrap())
    });

    group.finish();
}

// ── BitPacking ───────────────────────────────────────────────────────────────

fn bench_bitpacking_int64(c: &mut Criterion) {
    // Small-range integers — BitPacking's sweet spot.
    let values: Vec<i64> = (0..ROW_GROUP_SIZE as i64)
        .map(|i| 1000 + (i % 256))
        .collect();
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    let bp = BitPacking::new();
    let chunk = bp.encode(array.as_ref()).unwrap();
    let payload_bytes = chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/bitpacking/int64");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("encode", |b| {
        b.iter(|| bp.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("decode", |b| {
        b.iter(|| bp.decode(black_box(&chunk), &BqlType::Int).unwrap())
    });

    group.finish();
}

fn bench_bitpacking_timestamp(c: &mut Criterion) {
    // Timestamps in a narrow range — BitPacking with frame-of-reference.
    let array = gen_timestamp_array(ROW_GROUP_SIZE, 1_700_000_000_000_000_000, 1_000_000);
    let bp = BitPacking::new();
    let chunk = bp.encode(array.as_ref()).unwrap();
    let payload_bytes = chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/bitpacking/timestamp");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("encode", |b| {
        b.iter(|| bp.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("decode", |b| {
        b.iter(|| bp.decode(black_box(&chunk), &BqlType::Timestamp).unwrap())
    });

    group.finish();
}

// ── Constant ─────────────────────────────────────────────────────────────────

fn bench_constant_int64(c: &mut Criterion) {
    let values: Vec<i64> = vec![42_i64; ROW_GROUP_SIZE];
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    let constant = Constant::new();
    let chunk = constant.encode(array.as_ref()).unwrap();

    // Constant has zero payload; use params size for throughput measurement.
    let chunk_bytes = (chunk.params.len() + chunk.payload.len()) as u64;

    let mut group = c.benchmark_group("encoding/constant/int64");
    group.throughput(Throughput::Bytes(chunk_bytes.max(1)));

    group.bench_function("encode", |b| {
        b.iter(|| constant.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("decode", |b| {
        b.iter(|| constant.decode(black_box(&chunk), &BqlType::Int).unwrap())
    });

    group.finish();
}

fn bench_constant_string(c: &mut Criterion) {
    let values: Vec<String> = vec!["constant_value".to_string(); ROW_GROUP_SIZE];
    let array: ArrayRef = Arc::new(StringViewArray::from(values));
    let constant = Constant::new();
    let chunk = constant.encode(array.as_ref()).unwrap();
    let chunk_bytes = (chunk.params.len() + chunk.payload.len()) as u64;

    let mut group = c.benchmark_group("encoding/constant/string");
    group.throughput(Throughput::Bytes(chunk_bytes.max(1)));

    group.bench_function("encode", |b| {
        b.iter(|| constant.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("decode", |b| {
        b.iter(|| {
            constant
                .decode(black_box(&chunk), &BqlType::String)
                .unwrap()
        })
    });

    group.finish();
}

// ── LZ4 wrapper ──────────────────────────────────────────────────────────────

fn bench_lz4_compress(c: &mut Criterion) {
    // Compress a representative encoded payload (Plain int64).
    let array = gen_int64_array(ROW_GROUP_SIZE, 0);
    let plain = Plain::new();
    let chunk = plain.encode(array.as_ref()).unwrap();
    let payload = &chunk.payload;
    let payload_bytes = payload.len() as u64;

    let mut group = c.benchmark_group("encoding/lz4");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("compress", |b| b.iter(|| compress_lz4(black_box(payload))));

    let compressed = compress_lz4(payload);
    let uncompressed_len = payload.len();
    group.bench_function("decompress", |b| {
        b.iter(|| decompress_lz4(black_box(&compressed), uncompressed_len).unwrap())
    });

    group.finish();
}

fn bench_lz4_repetitive(c: &mut Criterion) {
    // LZ4 on highly-compressible data (low-cardinality string encoding).
    let array = gen_low_cardinality_string_array(ROW_GROUP_SIZE, 5);
    let plain = Plain::new();
    let chunk = plain.encode(array.as_ref()).unwrap();
    let payload = &chunk.payload;
    let payload_bytes = payload.len() as u64;

    let mut group = c.benchmark_group("encoding/lz4_repetitive");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("compress", |b| b.iter(|| compress_lz4(black_box(payload))));

    let compressed = compress_lz4(payload);
    let uncompressed_len = payload.len();
    group.bench_function("decompress", |b| {
        b.iter(|| decompress_lz4(black_box(&compressed), uncompressed_len).unwrap())
    });

    group.finish();
}

// ── Encoding selector throughput ─────────────────────────────────────────────

fn bench_selector_throughput(c: &mut Criterion) {
    // Measure the full select_encoding pipeline on representative data.
    let int_array = gen_int64_array(ROW_GROUP_SIZE, 0);
    let string_array = gen_low_cardinality_string_array(ROW_GROUP_SIZE, 20);
    let ts_array = gen_timestamp_array(ROW_GROUP_SIZE, 1_700_000_000_000_000_000, 1_000_000);
    let float_array = gen_float64_array(ROW_GROUP_SIZE, 0.0);

    let mut group = c.benchmark_group("encoding/selector");

    group.bench_function("int64", |b| {
        b.iter(|| {
            bqlite_storage::select_encoding(black_box(int_array.as_ref()), &BqlType::Int).unwrap()
        })
    });

    group.bench_function("string_low_card", |b| {
        b.iter(|| {
            bqlite_storage::select_encoding(black_box(string_array.as_ref()), &BqlType::String)
                .unwrap()
        })
    });

    group.bench_function("timestamp", |b| {
        b.iter(|| {
            bqlite_storage::select_encoding(black_box(ts_array.as_ref()), &BqlType::Timestamp)
                .unwrap()
        })
    });

    group.bench_function("float64", |b| {
        b.iter(|| {
            bqlite_storage::select_encoding(black_box(float_array.as_ref()), &BqlType::Float)
                .unwrap()
        })
    });

    group.finish();
}

// ── Metric reporting ─────────────────────────────────────────────────────────

fn bench_encoding_with_metrics(c: &mut Criterion) {
    // A single representative encode/decode cycle that exercises the
    // metric-reporting pathway from execution-model.md §14.1.
    let array = gen_int64_array(ROW_GROUP_SIZE, 0);
    let plain = Plain::new();
    let chunk = plain.encode(array.as_ref()).unwrap();
    let payload_bytes = chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/metrics");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("encode_decode_with_report", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let encoded = plain.encode(black_box(array.as_ref())).unwrap();
                let _ = plain.decode(black_box(&encoded), &BqlType::Int).unwrap();
            }
            let elapsed = start.elapsed();
            let total_bytes = payload_bytes * iters;
            report_metrics(total_bytes, total_bytes, elapsed);
            elapsed
        })
    });

    group.finish();
}

criterion_group! {
    name = encoding_benches;
    config = wave2_criterion();
    targets =
        bench_plain_int64,
        bench_plain_float64,
        bench_plain_string,
        bench_dictionary_int64,
        bench_dictionary_string,
        bench_delta_timestamp,
        bench_delta_int64,
        bench_bitpacking_int64,
        bench_bitpacking_timestamp,
        bench_constant_int64,
        bench_constant_string,
        bench_lz4_compress,
        bench_lz4_repetitive,
        bench_selector_throughput,
        bench_encoding_with_metrics,
}
criterion_main!(encoding_benches);
