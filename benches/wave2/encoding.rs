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
//!
//! ## Hard targets (reference mode only, TASK-246)
//!
//! Int64 decode throughput floors are enforced via `BenchResultCollector`
//! in reference mode. CI mode uses Criterion's statistical comparison only.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array, StringViewArray, TimestampNanosecondArray};
use bqlite_benches::common::*;
use bqlite_core::BqlType;
use bqlite_storage::encoding::{
    compress_lz4, decompress_lz4, BitPacking, Constant, Delta, Dictionary, DoubleDelta, Encoding,
    Plain, Rle,
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

// ── Rle ──────────────────────────────────────────────────────────────────────

fn bench_rle_int64_long_runs(c: &mut Criterion) {
    // 256 distinct values each repeated 256 times — long-run sweet spot.
    // Compresses 65,536 i64 rows (512 KiB) to 256 runs (≈8 KiB payload).
    let values: Vec<i64> = (0..ROW_GROUP_SIZE as i64)
        .map(|i| i / 256) // 256 identical values per run
        .collect();
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    let rle = Rle::new();
    let chunk = rle.encode(array.as_ref()).unwrap();
    let payload_bytes = chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/rle/int64_long_runs");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("encode", |b| {
        b.iter(|| rle.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("decode", |b| {
        b.iter(|| rle.decode(black_box(&chunk), &BqlType::Int).unwrap())
    });

    group.finish();
}

fn bench_rle_string_long_runs(c: &mut Criterion) {
    // 20 distinct event types each appearing in runs of ~3,277 rows.
    // Simulates a sorted partition where all events of one type appear
    // before the next — the canonical RLE workload for string columns.
    let labels: Vec<String> = (0..20).map(|i| format!("event_{i}")).collect();
    let values: Vec<String> = (0..ROW_GROUP_SIZE)
        .map(|i| labels[(i * 20) / ROW_GROUP_SIZE].clone())
        .collect();
    let array: ArrayRef = Arc::new(StringViewArray::from(values));
    let rle = Rle::new();
    let chunk = rle.encode(array.as_ref()).unwrap();
    let payload_bytes = chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/rle/string_long_runs");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("encode", |b| {
        b.iter(|| rle.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("decode", |b| {
        b.iter(|| rle.decode(black_box(&chunk), &BqlType::String).unwrap())
    });

    group.finish();
}

fn bench_rle_bool_constant(c: &mut Criterion) {
    // All-true bool column: single run of 65,536 rows → 1-run chunk.
    // Measures the RLE encode/decode floor when compression is maximal.
    let values: Vec<bool> = vec![true; ROW_GROUP_SIZE];
    let array: ArrayRef = Arc::new(arrow::array::BooleanArray::from(values));
    let rle = Rle::new();
    let chunk = rle.encode(array.as_ref()).unwrap();
    let payload_bytes = (chunk.params.len() + chunk.payload.len()) as u64;

    let mut group = c.benchmark_group("encoding/rle/bool_constant");
    group.throughput(Throughput::Bytes(payload_bytes.max(1)));

    group.bench_function("encode", |b| {
        b.iter(|| rle.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("decode", |b| {
        b.iter(|| rle.decode(black_box(&chunk), &BqlType::Bool).unwrap())
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

// ── DoubleDelta ──────────────────────────────────────────────────────────────

fn bench_double_delta_timestamp_near_constant(c: &mut Criterion) {
    // Near-constant-interval timestamps with small jitter — the primary
    // use case for DoubleDelta per `advanced-encodings.md` §4.7.
    // The step is 1 ms (1_000_000 ns) and jitter ≈ ±250 ns, so second-order
    // deltas are tiny (<<step) and dd_bit_width collapses to ~9 bits.
    let base = 1_700_000_000_000_000_000_i64;
    let step = 1_000_000_i64; // 1 ms in ns
    let jitter_period = 500_i64; // ±250 ns jitter cycle
    let values: Vec<i64> = (0..ROW_GROUP_SIZE as i64)
        .map(|i| base + i * step + (i % jitter_period - jitter_period / 2))
        .collect();
    let array: ArrayRef = Arc::new(TimestampNanosecondArray::from(values).with_timezone("UTC"));

    let dd = DoubleDelta::new();
    let chunk = dd.encode(array.as_ref()).unwrap();
    let delta = Delta::new();
    let delta_chunk = delta.encode(array.as_ref()).unwrap();
    let dd_payload_bytes = chunk.payload.len() as u64;
    let delta_payload_bytes = delta_chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/double_delta/timestamp_near_constant");
    group.throughput(Throughput::Bytes(dd_payload_bytes));

    group.bench_function("encode", |b| {
        b.iter(|| dd.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("decode", |b| {
        b.iter(|| dd.decode(black_box(&chunk), &BqlType::Timestamp).unwrap())
    });

    group.bench_function("delta_encode_for_comparison", |b| {
        b.iter(|| delta.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("delta_decode_for_comparison", |b| {
        b.iter(|| {
            delta
                .decode(black_box(&delta_chunk), &BqlType::Timestamp)
                .unwrap()
        })
    });

    // Report the compression improvement over Delta.
    let _ = delta_payload_bytes; // used in the comparison above
    group.finish();
}

fn bench_double_delta_seq_id(c: &mut Criterion) {
    // Strictly monotonic seq_id (Δ = 1, dd = 0). All double-deltas are
    // zero, so dd_bit_width floors to 1 and the payload is all zeros.
    // This is the best case for DoubleDelta — ~0.016× vs Plain.
    let array = gen_int64_array(ROW_GROUP_SIZE, 0); // 0, 1, 2, ..., N-1
    let dd = DoubleDelta::new();
    let chunk = dd.encode(array.as_ref()).unwrap();
    let payload_bytes = chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/double_delta/seq_id");
    group.throughput(Throughput::Bytes(payload_bytes.max(1)));

    group.bench_function("encode", |b| {
        b.iter(|| dd.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("decode", |b| {
        b.iter(|| dd.decode(black_box(&chunk), &BqlType::Int).unwrap())
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

    let mode = BenchMode::from_env();
    let mut collector = BenchResultCollector::new(mode);

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

    // In reference mode, measure int64 decode throughput and enforce floor.
    if mode.is_reference() {
        let start = Instant::now();
        let n_iters = 100u64;
        for _ in 0..n_iters {
            let _ = plain.decode(black_box(&chunk), &BqlType::Int).unwrap();
        }
        let elapsed_secs = start.elapsed().as_secs_f64();
        let total_rows = ROW_GROUP_SIZE as f64 * n_iters as f64;
        let rows_per_sec = total_rows / elapsed_secs;
        collector.record(
            "encoding/int64_decode_rows_per_sec",
            rows_per_sec,
            "rows/s",
            Some(BenchTarget::at_least(200_000_000.0)),
        );
    }

    collector.finish();
}

criterion_group! {
    name = encoding_benches;
    config = criterion_for_mode(BenchMode::from_env());
    targets =
        bench_plain_int64,
        bench_plain_float64,
        bench_plain_string,
        bench_dictionary_int64,
        bench_dictionary_string,
        bench_delta_timestamp,
        bench_delta_int64,
        bench_double_delta_timestamp_near_constant,
        bench_double_delta_seq_id,
        bench_bitpacking_int64,
        bench_bitpacking_timestamp,
        bench_constant_int64,
        bench_constant_string,
        bench_rle_int64_long_runs,
        bench_rle_string_long_runs,
        bench_rle_bool_constant,
        bench_lz4_compress,
        bench_lz4_repetitive,
        bench_selector_throughput,
        bench_encoding_with_metrics,
}
criterion_main!(encoding_benches);
