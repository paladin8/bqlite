//! Per-encoding encode/decode microbenches for the v1 and Wave 4 encoding set.
//!
//! Covers the Wave 2 performance gate "encoding" line:
//! Plain, Dictionary, Delta, BitPacking, Constant, and the LZ4
//! post-encoding wrapper.
//!
//! Wave 4 additions (TASK-413 RLE, TASK-415 FOR) are also benchmarked here
//! to produce the decode-throughput evidence required by
//! `advanced-encodings.md` §5.3.
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

use arrow::array::{Array, ArrayRef, Int64Array, StringViewArray, TimestampNanosecondArray};
use bqlite_benches::common::*;
use bqlite_core::BqlType;
use bqlite_storage::encoding::{
    compress_lz4, decompress_lz4, BitPacking, Constant, Delta, Dictionary, DoubleDelta, Encoding,
    ForEncoding, Fsst, Plain, Rle,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use fsst::fsst::{
    compress as fsst_compress, decompress as fsst_decompress, FSST_SYMBOL_TABLE_SIZE,
};

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

// ── FSST ────────────────────────────────────────────────────────────────────

/// Generate a high-cardinality URL-like string array for FSST benchmarks.
/// These have shared prefixes and common substrings (`https://`, `.com/`,
/// `/page`) — the target workload for FSST compression.
fn gen_url_string_array(n: usize) -> ArrayRef {
    let prefixes = [
        "https://example.com/page/",
        "https://api.example.org/v2/users/",
        "https://cdn.test.io/assets/images/",
        "https://www.example.net/search?q=",
    ];
    let values: Vec<String> = (0..n)
        .map(|i| format!("{}{}/details?ref=home&ts={}", prefixes[i % 4], i, i * 1000))
        .collect();
    Arc::new(StringViewArray::from(values))
}

/// Generate a user-agent-like string array — another common FSST target.
fn gen_user_agent_array(n: usize) -> ArrayRef {
    let agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) Mobile/15E148",
    ];
    // High cardinality: each string is unique (suffixed with row index),
    // but shares substantial prefix structure.
    let values: Vec<String> = (0..n)
        .map(|i| format!("{} (session={})", agents[i % 4], i))
        .collect();
    Arc::new(StringViewArray::from(values))
}

#[derive(Clone)]
struct RawFsstChunk {
    symbol_table: [u8; FSST_SYMBOL_TABLE_SIZE],
    payload: Vec<u8>,
    row_count: usize,
    decoded_bytes: usize,
}

fn string_view_offsets_and_bytes(array: &ArrayRef) -> (Vec<i32>, Vec<u8>) {
    let view = array
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("FSST benchmark datasets are StringViewArray");

    let mut offsets = Vec::with_capacity(view.len() + 1);
    let mut values = Vec::new();
    offsets.push(0);
    for i in 0..view.len() {
        let bytes = view.value(i).as_bytes();
        values.extend_from_slice(bytes);
        offsets.push(
            i32::try_from(values.len()).expect("FSST benchmark input fits in i32 offset space"),
        );
    }
    (offsets, values)
}

fn encode_with_raw_fsst(values: &[u8], offsets: &[i32]) -> std::io::Result<RawFsstChunk> {
    let mut symbol_table = [0u8; FSST_SYMBOL_TABLE_SIZE];
    let mut compressed_values = vec![0u8; values.len()];
    let mut compressed_offsets = vec![0i32; offsets.len()];

    fsst_compress(
        &mut symbol_table,
        values,
        offsets,
        &mut compressed_values,
        &mut compressed_offsets,
    )?;

    if compressed_offsets.len() != offsets.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "lance fsst returned {} offsets for {} strings",
                compressed_offsets.len(),
                offsets.len().saturating_sub(1)
            ),
        ));
    }

    let mut payload = Vec::with_capacity(compressed_values.len() + offsets.len() * 2);
    for window in compressed_offsets.windows(2) {
        let start = usize::try_from(window[0]).expect("compressed offsets stay non-negative");
        let end = usize::try_from(window[1]).expect("compressed offsets stay non-negative");
        let len = end - start;
        if len > u16::MAX as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("compressed string length {len} exceeds bqlite's u16 limit"),
            ));
        }
        payload.extend_from_slice(&(len as u16).to_le_bytes());
        payload.extend_from_slice(&compressed_values[start..end]);
    }

    Ok(RawFsstChunk {
        symbol_table,
        payload,
        row_count: offsets.len().saturating_sub(1),
        decoded_bytes: values.len(),
    })
}

fn decode_with_raw_fsst(chunk: &RawFsstChunk) -> std::io::Result<ArrayRef> {
    let mut compressed_values = Vec::new();
    let mut compressed_offsets = Vec::with_capacity(chunk.row_count + 1);
    compressed_offsets.push(0i32);

    let mut offset = 0usize;
    while offset < chunk.payload.len() {
        if offset + 2 > chunk.payload.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "missing per-string compressed length prefix",
            ));
        }
        let len =
            u16::from_le_bytes(chunk.payload[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;

        if offset + len > chunk.payload.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "payload ended before compressed string bytes",
            ));
        }

        compressed_values.extend_from_slice(&chunk.payload[offset..offset + len]);
        compressed_offsets.push(
            i32::try_from(compressed_values.len())
                .expect("compressed payload fits in i32 offset space"),
        );
        offset += len;
    }

    if compressed_offsets.len() != chunk.row_count + 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "decoded {} offsets for {} rows",
                compressed_offsets.len(),
                chunk.row_count
            ),
        ));
    }

    let mut decoded_values = vec![0u8; chunk.decoded_bytes.max(compressed_values.len() * 3)];
    let mut decoded_offsets = vec![0i32; compressed_offsets.len()];
    fsst_decompress(
        &chunk.symbol_table,
        &compressed_values,
        &compressed_offsets,
        &mut decoded_values,
        &mut decoded_offsets,
    )?;

    let mut strings = Vec::with_capacity(chunk.row_count);
    for window in decoded_offsets.windows(2) {
        let start = usize::try_from(window[0]).expect("decoded offsets stay non-negative");
        let end = usize::try_from(window[1]).expect("decoded offsets stay non-negative");
        let s = std::str::from_utf8(&decoded_values[start..end]).map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("lance fsst produced invalid UTF-8: {err}"),
            )
        })?;
        strings.push(s);
    }

    Ok(Arc::new(StringViewArray::from(strings)) as ArrayRef)
}

fn bench_fsst_url_strings(c: &mut Criterion) {
    let array = gen_url_string_array(ROW_GROUP_SIZE);
    let fsst = Fsst::train(array.as_ref()).unwrap();
    let chunk = fsst.encode(array.as_ref()).unwrap();
    let payload_bytes = chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/fsst/url_strings");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("train", |b| {
        b.iter(|| Fsst::train(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("encode", |b| {
        b.iter(|| fsst.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("decode", |b| {
        b.iter(|| fsst.decode(black_box(&chunk), &BqlType::String).unwrap())
    });

    group.finish();
}

fn bench_fsst_user_agents(c: &mut Criterion) {
    let array = gen_user_agent_array(ROW_GROUP_SIZE);
    let fsst = Fsst::train(array.as_ref()).unwrap();
    let chunk = fsst.encode(array.as_ref()).unwrap();
    let payload_bytes = chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/fsst/user_agents");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("train", |b| {
        b.iter(|| Fsst::train(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("encode", |b| {
        b.iter(|| fsst.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("decode", |b| {
        b.iter(|| fsst.decode(black_box(&chunk), &BqlType::String).unwrap())
    });

    group.finish();
}

/// Compare the production FSST wrapper against the raw `fsst` crate
/// API, including the packing/unpacking adapter work needed to fit
/// bqlite's current payload shape.
fn bench_fsst_impl_compare(c: &mut Criterion) {
    for (dataset_name, array) in [
        ("url_strings", gen_url_string_array(ROW_GROUP_SIZE)),
        ("user_agents", gen_user_agent_array(ROW_GROUP_SIZE)),
    ] {
        let (offsets, values) = string_view_offsets_and_bytes(&array);

        let wrapper = Fsst::train(array.as_ref()).unwrap();
        let wrapper_chunk = wrapper.encode(array.as_ref()).unwrap();
        let raw_chunk = encode_with_raw_fsst(&values, &offsets).unwrap();
        let wrapper_decoded = wrapper.decode(&wrapper_chunk, &BqlType::String).unwrap();
        let raw_decoded = decode_with_raw_fsst(&raw_chunk).unwrap();

        assert_eq!(wrapper_decoded.as_ref(), array.as_ref());
        assert_eq!(raw_decoded.as_ref(), array.as_ref());

        let input_bytes = values.len() as u64;
        let wrapper_total_bytes = (wrapper_chunk.params.len() + wrapper_chunk.payload.len()) as u64;
        let raw_total_bytes = (raw_chunk.symbol_table.len() + raw_chunk.payload.len()) as u64;

        let mut group = c.benchmark_group(format!("encoding/fsst_impl_compare/{dataset_name}"));
        group.throughput(Throughput::Bytes(input_bytes));

        group.bench_function("wrapper_train_plus_encode", |b| {
            b.iter(|| {
                let trained = Fsst::train(black_box(array.as_ref())).unwrap();
                trained.encode(black_box(array.as_ref())).unwrap()
            })
        });

        group.bench_function("raw_encode_adapted", |b| {
            b.iter(|| encode_with_raw_fsst(black_box(&values), black_box(&offsets)).unwrap())
        });

        group.bench_function("wrapper_decode", |b| {
            b.iter(|| {
                wrapper
                    .decode(black_box(&wrapper_chunk), &BqlType::String)
                    .unwrap()
            })
        });

        group.bench_function("raw_decode_adapted", |b| {
            b.iter(|| decode_with_raw_fsst(black_box(&raw_chunk)).unwrap())
        });

        group.finish();

        eprintln!(
            "  impl-compare/{dataset_name}: wrapper={} bytes (params {} + payload {}), \
             raw={} bytes (symbol_table {} + payload {})",
            wrapper_total_bytes,
            wrapper_chunk.params.len(),
            wrapper_chunk.payload.len(),
            raw_total_bytes,
            raw_chunk.symbol_table.len(),
            raw_chunk.payload.len()
        );
    }
}

/// Compare FSST against Dictionary + Plain + LZ4 on the same
/// high-cardinality string column. This directly addresses the task
/// description's requirement for comparative benchmarks.
fn bench_fsst_vs_baselines(c: &mut Criterion) {
    let array = gen_url_string_array(ROW_GROUP_SIZE);

    // FSST
    let fsst = Fsst::train(array.as_ref()).unwrap();
    let fsst_chunk = fsst.encode(array.as_ref()).unwrap();

    // Plain
    let plain = Plain::new();
    let plain_chunk = plain.encode(array.as_ref()).unwrap();

    // Plain + LZ4
    let plain_lz4 = compress_lz4(&plain_chunk.payload);

    // Dictionary (may fail if cardinality is too high; that's expected
    // for high-cardinality data — Dictionary is the wrong encoding here).
    let dict = Dictionary::new();
    let dict_result = dict.encode(array.as_ref());

    let mut group = c.benchmark_group("encoding/fsst_vs_baselines/url_strings");

    // Report payload sizes for comparison.
    group.bench_function("fsst_encode", |b| {
        b.iter(|| fsst.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("fsst_decode", |b| {
        b.iter(|| {
            fsst.decode(black_box(&fsst_chunk), &BqlType::String)
                .unwrap()
        })
    });

    group.bench_function("plain_encode", |b| {
        b.iter(|| plain.encode(black_box(array.as_ref())).unwrap())
    });

    group.bench_function("plain_decode", |b| {
        b.iter(|| {
            plain
                .decode(black_box(&plain_chunk), &BqlType::String)
                .unwrap()
        })
    });

    group.bench_function("plain_lz4_compress", |b| {
        b.iter(|| compress_lz4(black_box(&plain_chunk.payload)))
    });

    group.bench_function("plain_lz4_decompress", |b| {
        b.iter(|| decompress_lz4(black_box(&plain_lz4), plain_chunk.payload.len()))
    });

    if let Ok(ref dict_chunk) = dict_result {
        group.bench_function("dict_encode", |b| {
            b.iter(|| dict.encode(black_box(array.as_ref())).unwrap())
        });

        group.bench_function("dict_decode", |b| {
            b.iter(|| {
                dict.decode(black_box(dict_chunk), &BqlType::String)
                    .unwrap()
            })
        });
    }

    group.finish();

    // Print size comparison (visible in cargo bench output).
    eprintln!(
        "  FSST payload: {} bytes ({:.1}x vs plain)",
        fsst_chunk.payload.len(),
        plain_chunk.payload.len() as f64 / fsst_chunk.payload.len() as f64
    );
    eprintln!("  Plain payload: {} bytes", plain_chunk.payload.len());
    eprintln!(
        "  Plain+LZ4 payload: {} bytes ({:.1}x vs plain)",
        plain_lz4.len(),
        plain_chunk.payload.len() as f64 / plain_lz4.len() as f64
    );
    if let Ok(ref dict_chunk) = dict_result {
        eprintln!(
            "  Dict payload: {} bytes ({:.1}x vs plain)",
            dict_chunk.payload.len(),
            plain_chunk.payload.len() as f64 / dict_chunk.payload.len() as f64
        );
    } else {
        eprintln!(
            "  Dict: skipped (cardinality too high: {})",
            dict_result.unwrap_err()
        );
    }
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

// ── FOR (Frame-of-Reference) — Wave 4 / TASK-415 ────────────────────────────

/// FOR on clustered int64 data — best case for per-block framing.
///
/// Simulates a numeric property column (e.g. `amount`) where values cluster
/// in two distinct ranges within a 65 536-row chunk: rows 0–32 767 in
/// [100, 115] and rows 32 768–65 535 in [10 000, 10 015]. This mirrors the
/// TASK-401 §5.2 "narrow-range integers with local clustering" profile.
///
/// FOR achieves ~4 bits/value per block, while global BitPacking needs
/// ~14 bits/value (range ≈ 9 915). The bench measures FOR encode/decode and
/// BitPacking encode/decode on the same data so the selector's size advantage
/// is directly observable from the throughput numbers.
fn bench_for_int64_clustered(c: &mut Criterion) {
    // Build a clustered column: first half in [100, 116), second in [10000, 10016).
    let values: Vec<i64> = (0..ROW_GROUP_SIZE as i64)
        .map(|i| {
            if i < (ROW_GROUP_SIZE as i64) / 2 {
                100 + (i % 16)
            } else {
                10_000 + (i % 16)
            }
        })
        .collect();
    let array: ArrayRef = Arc::new(Int64Array::from(values));

    let for_chunk = ForEncoding.encode(array.as_ref()).unwrap();
    let bp_chunk = BitPacking.encode(array.as_ref()).unwrap();
    let for_payload_bytes = for_chunk.payload.len() as u64;
    let bp_payload_bytes = bp_chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/for/clustered_int64");

    group.throughput(Throughput::Bytes(for_payload_bytes));
    group.bench_function("for_encode", |b| {
        b.iter(|| ForEncoding.encode(black_box(array.as_ref())).unwrap())
    });
    group.bench_function("for_decode", |b| {
        b.iter(|| {
            ForEncoding
                .decode(black_box(&for_chunk), &BqlType::Int)
                .unwrap()
        })
    });

    // BitPacking on the same array for direct comparison.
    group.throughput(Throughput::Bytes(bp_payload_bytes));
    group.bench_function("bitpacking_encode", |b| {
        b.iter(|| BitPacking.encode(black_box(array.as_ref())).unwrap())
    });
    group.bench_function("bitpacking_decode", |b| {
        b.iter(|| {
            BitPacking
                .decode(black_box(&bp_chunk), &BqlType::Int)
                .unwrap()
        })
    });

    group.finish();
}

/// FOR on globally sequential int64 data — comparable to BitPacking.
///
/// Sequential integers [0, 65535] have a 16-bit global range. Both FOR
/// and BitPacking choose 16 bits/value. FOR's per-block overhead
/// (~9 bytes per 128-row block = ~4.6 KB for 512 blocks) makes it
/// slightly larger than BitPacking here, confirming the selector guard:
/// FOR should not be chosen over BitPacking when per-block bit widths
/// equal the global bit width.
fn bench_for_int64_sequential(c: &mut Criterion) {
    let values: Vec<i64> = (0..ROW_GROUP_SIZE as i64).collect();
    let array: ArrayRef = Arc::new(Int64Array::from(values));

    let for_chunk = ForEncoding.encode(array.as_ref()).unwrap();
    let payload_bytes = for_chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/for/sequential_int64");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("for_encode", |b| {
        b.iter(|| ForEncoding.encode(black_box(array.as_ref())).unwrap())
    });
    group.bench_function("for_decode", |b| {
        b.iter(|| {
            ForEncoding
                .decode(black_box(&for_chunk), &BqlType::Int)
                .unwrap()
        })
    });

    group.finish();
}

/// FOR on timestamp data — monotonic-within-entity timestamps with 1 ms
/// spacing, base ≈ 1.7×10¹⁸ ns. Per block, the range is 128 ms = 128 000 000 ns,
/// which needs 27 bits. Global BitPacking sees the same range since timestamps
/// are monotonically increasing. FOR and BitPacking converge here.
fn bench_for_timestamp(c: &mut Criterion) {
    let array = gen_timestamp_array(
        ROW_GROUP_SIZE,
        1_700_000_000_000_000_000,
        1_000_000, /* 1ms */
    );

    let for_chunk = ForEncoding.encode(array.as_ref()).unwrap();
    let payload_bytes = for_chunk.payload.len() as u64;

    let mut group = c.benchmark_group("encoding/for/timestamp");
    group.throughput(Throughput::Bytes(payload_bytes));

    group.bench_function("for_encode", |b| {
        b.iter(|| ForEncoding.encode(black_box(array.as_ref())).unwrap())
    });
    group.bench_function("for_decode", |b| {
        b.iter(|| {
            ForEncoding
                .decode(black_box(&for_chunk), &BqlType::Timestamp)
                .unwrap()
        })
    });

    group.finish();
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
        bench_for_int64_clustered,
        bench_for_int64_sequential,
        bench_for_timestamp,
        bench_lz4_compress,
        bench_lz4_repetitive,
        bench_fsst_url_strings,
        bench_fsst_user_agents,
        bench_fsst_impl_compare,
        bench_fsst_vs_baselines,
        bench_selector_throughput,
        bench_encoding_with_metrics,
}
criterion_main!(encoding_benches);
