//! DDSketch-based percentile accumulator state.
//!
//! Implements the DDSketch data structure from the Datadog paper
//! ("DDSketch: A Fast and Fully-Mergeable Quantile Sketch with
//! Relative-Error Guarantees", Masson et al. 2019).
//!
//! DDSketch properties that are load-bearing for the aggregate
//! framework:
//!
//! - **Bounded relative error** (default 1%): quantile estimates
//!   are within a factor of `(1 + α)` of the true value.
//! - **Constant-time merge**: two sketches merge in O(max_buckets),
//!   independent of the number of observations.
//! - **Incremental**: satisfies the fusion eligibility invariant
//!   from planner-pipeline.md §7.2.
//! - **Compact**: ~1–2 KB per group at default settings.
//!
//! ## Design choices
//!
//! - **Logarithmic index mapping**: `index = ceil(log(v) / log(gamma))`
//!   where `gamma = (1 + α) / (1 - α)`.
//! - **Separate counters for negatives, zero, and positives**: negatives
//!   are indexed as `index(-v)` into a separate store.
//! - **Dense storage with offset**: a contiguous `Vec<u64>` with a base
//!   offset, covering the range `[min_index, max_index]`. This is more
//!   cache-friendly than a `HashMap` for the typical analytics use case
//!   where values cluster in a narrow range.
//! - **Max buckets cap**: the sketch refuses to grow beyond
//!   `MAX_NUM_BUCKETS` (default 2048). When a value would require
//!   expanding beyond this limit, it is mapped to the nearest existing
//!   bucket. This caps memory at ~16 KB per sketch.

use std::fmt;

use bqlite_core::{AggFunction, ScalarValue};

// ──────────────────────────────────────────────────────────────────────────────
// Constants
// ──────────────────────────────────────────────────────────────────────────────

/// Default relative accuracy (α). A value of 0.01 guarantees that
/// quantile estimates are within 1% of the true value.
const DEFAULT_RELATIVE_ACCURACY: f64 = 0.01;

/// Maximum number of buckets per store (positive or negative side).
/// 2048 buckets × 8 bytes = 16 KB per store, so ~32 KB worst case
/// for a sketch with both positive and negative values. In practice,
/// analytics values are positive, so the negative store stays empty.
const MAX_NUM_BUCKETS: usize = 2048;

// ──────────────────────────────────────────────────────────────────────────────
// LogarithmicMapping
// ──────────────────────────────────────────────────────────────────────────────

/// Maps positive floating-point values to bucket indices using
/// logarithmic scaling: `index = ceil(log(v) / log(gamma))`.
#[derive(Debug, Clone)]
struct LogarithmicMapping {
    /// `gamma = (1 + α) / (1 - α)`.
    gamma: f64,
    /// `1.0 / ln(gamma)` — precomputed for fast index calculation.
    multiplier: f64,
}

impl LogarithmicMapping {
    fn new(relative_accuracy: f64) -> Self {
        assert!(
            relative_accuracy > 0.0 && relative_accuracy < 1.0,
            "relative accuracy must be in (0, 1), got {relative_accuracy}"
        );
        let gamma = (1.0 + relative_accuracy) / (1.0 - relative_accuracy);
        let multiplier = 1.0 / gamma.ln();
        Self { gamma, multiplier }
    }

    /// Map a positive value to a bucket index.
    #[inline]
    fn index(&self, value: f64) -> i32 {
        debug_assert!(value > 0.0);
        (value.ln() * self.multiplier).ceil() as i32
    }

    /// Map a bucket index back to the upper bound of its bucket.
    #[inline]
    fn value(&self, index: i32) -> f64 {
        self.gamma.powi(index)
    }

    /// Map a bucket index to the lower bound of its bucket.
    #[inline]
    fn lower_bound(&self, index: i32) -> f64 {
        self.gamma.powi(index - 1)
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// DenseStore
// ──────────────────────────────────────────────────────────────────────────────

/// Dense contiguous array of bucket counts with an offset.
///
/// Buckets cover indices `[offset, offset + bins.len())`. This is
/// efficient for clustered data (the common case in analytics) and
/// avoids hash-map overhead.
#[derive(Debug, Clone)]
struct DenseStore {
    /// Contiguous bucket counts.
    bins: Vec<u64>,
    /// Index of the first bin.
    offset: i32,
    /// Total count of observations in this store.
    count: u64,
}

impl DenseStore {
    fn new() -> Self {
        Self {
            bins: Vec::new(),
            offset: 0,
            count: 0,
        }
    }

    /// Returns true if no observations have been added.
    #[inline]
    fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Add a count to the bucket at the given index.
    fn add(&mut self, index: i32, count: u64) {
        if self.bins.is_empty() {
            self.offset = index;
            self.bins.push(count);
            self.count += count;
            return;
        }

        if index < self.offset {
            let growth = (self.offset - index) as usize;
            if self.bins.len() + growth > MAX_NUM_BUCKETS {
                // Cap reached: collapse into the first existing bucket.
                self.bins[0] += count;
                self.count += count;
                return;
            }
            // Prepend empty bins.
            let mut new_bins = vec![0u64; growth];
            new_bins.extend_from_slice(&self.bins);
            self.bins = new_bins;
            self.offset = index;
            self.bins[0] += count;
        } else {
            let pos = (index - self.offset) as usize;
            if pos >= self.bins.len() {
                let growth = pos + 1 - self.bins.len();
                if self.bins.len() + growth > MAX_NUM_BUCKETS {
                    // Cap reached: collapse into the last existing bucket.
                    let last = self.bins.len() - 1;
                    self.bins[last] += count;
                    self.count += count;
                    return;
                }
                self.bins.resize(pos + 1, 0);
            }
            self.bins[pos] += count;
        }
        self.count += count;
    }

    /// Merge another store into this one.
    fn merge(&mut self, other: &DenseStore) {
        if other.is_empty() {
            return;
        }
        if self.is_empty() {
            self.bins = other.bins.clone();
            self.offset = other.offset;
            self.count = other.count;
            return;
        }

        // Determine the merged range.
        let self_max = self.offset + self.bins.len() as i32 - 1;
        let other_max = other.offset + other.bins.len() as i32 - 1;
        let new_offset = self.offset.min(other.offset);
        let new_max = self_max.max(other_max);
        let new_len = (new_max - new_offset + 1) as usize;

        if new_len > MAX_NUM_BUCKETS {
            // Merge bin-by-bin with capping.
            for (i, &c) in other.bins.iter().enumerate() {
                if c > 0 {
                    self.add(other.offset + i as i32, c);
                }
            }
            return;
        }

        let mut new_bins = vec![0u64; new_len];

        // Copy self's bins.
        let self_start = (self.offset - new_offset) as usize;
        for (i, &c) in self.bins.iter().enumerate() {
            new_bins[self_start + i] = c;
        }

        // Add other's bins.
        let other_start = (other.offset - new_offset) as usize;
        for (i, &c) in other.bins.iter().enumerate() {
            new_bins[other_start + i] += c;
        }

        self.bins = new_bins;
        self.offset = new_offset;
        self.count += other.count;
    }

    /// Estimated heap size in bytes.
    fn heap_size(&self) -> usize {
        self.bins.capacity() * std::mem::size_of::<u64>()
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// DDSketch
// ──────────────────────────────────────────────────────────────────────────────

/// DDSketch quantile sketch with bounded relative error.
///
/// Maintains separate stores for positive values, negative values,
/// and a count of zero values. This allows accurate quantile
/// estimation across the full real number line.
#[derive(Debug, Clone)]
pub struct DDSketch {
    mapping: LogarithmicMapping,
    positive_store: DenseStore,
    negative_store: DenseStore,
    zero_count: u64,
    /// Total count across all stores.
    count: u64,
    min_value: f64,
    max_value: f64,
}

impl Default for DDSketch {
    fn default() -> Self {
        Self::new()
    }
}

impl DDSketch {
    /// Create a new DDSketch with default relative accuracy (1%).
    pub fn new() -> Self {
        Self::with_accuracy(DEFAULT_RELATIVE_ACCURACY)
    }

    /// Create a new DDSketch with the given relative accuracy.
    pub fn with_accuracy(relative_accuracy: f64) -> Self {
        Self {
            mapping: LogarithmicMapping::new(relative_accuracy),
            positive_store: DenseStore::new(),
            negative_store: DenseStore::new(),
            zero_count: 0,
            count: 0,
            min_value: f64::INFINITY,
            max_value: f64::NEG_INFINITY,
        }
    }

    /// Returns true if no values have been inserted.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns the total number of values inserted.
    #[inline]
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Insert a single value.
    pub fn insert(&mut self, value: f64) {
        if value.is_nan() || value.is_infinite() {
            // Skip NaN and infinities — consistent with NULL skipping
            // in the aggregate framework.
            return;
        }

        if value > 0.0 {
            let index = self.mapping.index(value);
            self.positive_store.add(index, 1);
        } else if value < 0.0 {
            let index = self.mapping.index(-value);
            self.negative_store.add(index, 1);
        } else {
            self.zero_count += 1;
        }

        self.count += 1;
        if value < self.min_value {
            self.min_value = value;
        }
        if value > self.max_value {
            self.max_value = value;
        }
    }

    /// Merge another sketch into this one.
    ///
    /// Both sketches must use the same relative accuracy. This is
    /// guaranteed by construction in the aggregate framework (all
    /// shards use the same `AggState::new()` parameters).
    pub fn merge(&mut self, other: &DDSketch) {
        if other.is_empty() {
            return;
        }

        self.positive_store.merge(&other.positive_store);
        self.negative_store.merge(&other.negative_store);
        self.zero_count += other.zero_count;
        self.count += other.count;

        if other.min_value < self.min_value {
            self.min_value = other.min_value;
        }
        if other.max_value > self.max_value {
            self.max_value = other.max_value;
        }
    }

    /// Query the sketch for an approximate quantile.
    ///
    /// `quantile` must be in `[0.0, 1.0]`. Returns `None` if the
    /// sketch is empty.
    pub fn quantile(&self, quantile: f64) -> Option<f64> {
        if self.is_empty() {
            return None;
        }

        debug_assert!((0.0..=1.0).contains(&quantile));

        // The target rank (1-indexed).
        let rank = (quantile * (self.count - 1) as f64).round() as u64 + 1;

        // Walk negative store (highest negative index first = most negative value).
        let mut running = 0u64;
        if !self.negative_store.is_empty() {
            // Iterate from highest index (most negative value) to lowest.
            for i in (0..self.negative_store.bins.len()).rev() {
                running += self.negative_store.bins[i];
                if running >= rank {
                    let index = self.negative_store.offset + i as i32;
                    // Negative values: return the midpoint of the bucket.
                    let upper = self.mapping.value(index);
                    let lower = self.mapping.lower_bound(index);
                    return Some(-0.5 * (lower + upper));
                }
            }
        }

        // Walk zero count.
        running += self.zero_count;
        if running >= rank {
            return Some(0.0);
        }

        // Walk positive store (lowest index first = smallest positive value).
        if !self.positive_store.is_empty() {
            for i in 0..self.positive_store.bins.len() {
                running += self.positive_store.bins[i];
                if running >= rank {
                    let index = self.positive_store.offset + i as i32;
                    // Return the midpoint of the bucket for better accuracy.
                    let upper = self.mapping.value(index);
                    let lower = self.mapping.lower_bound(index);
                    return Some(0.5 * (lower + upper));
                }
            }
        }

        // Should not reach here unless count is inconsistent.
        Some(self.max_value)
    }

    /// Estimated heap size in bytes (heap allocations only, not the
    /// struct itself — the struct is inline in `AggState`).
    pub fn heap_size(&self) -> usize {
        self.positive_store.heap_size() + self.negative_store.heap_size()
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// PercentileState
// ──────────────────────────────────────────────────────────────────────────────

/// Percentile accumulator state wrapping a DDSketch.
///
/// Tracks which quantile to extract on finalization (0.50, 0.90,
/// 0.95, or 0.99) alongside the sketch itself.
#[derive(Debug, Clone)]
pub struct PercentileState {
    /// The underlying DDSketch.
    pub(crate) sketch: DDSketch,
    /// The quantile to extract on finalization (e.g. 0.50 for P50).
    pub(crate) quantile: f64,
}

impl PercentileState {
    /// Create a new percentile state for the given aggregate function.
    ///
    /// Panics if `function` is not one of P50/P90/P95/P99.
    pub fn new(function: AggFunction) -> Self {
        let quantile = match function {
            AggFunction::P50 => 0.50,
            AggFunction::P90 => 0.90,
            AggFunction::P95 => 0.95,
            AggFunction::P99 => 0.99,
            _ => panic!("PercentileState::new called with non-percentile function: {function}"),
        };
        Self {
            sketch: DDSketch::new(),
            quantile,
        }
    }

    /// Update the sketch with a scalar value.
    ///
    /// Only Int and Float values are accepted (per type-system.md §6.4).
    /// NULL values are skipped by the caller (AggState::update).
    pub fn update(&mut self, value: &ScalarValue) {
        let v = match value {
            ScalarValue::Int(n) => *n as f64,
            ScalarValue::Float(f) => *f,
            _ => return,
        };
        self.sketch.insert(v);
    }

    /// Merge another percentile state into this one.
    pub fn merge(&mut self, other: &PercentileState) {
        self.sketch.merge(&other.sketch);
    }

    /// Finalize to produce the quantile estimate.
    ///
    /// Returns NULL if no values were observed (empty sketch).
    pub fn finalize(&self) -> ScalarValue {
        match self.sketch.quantile(self.quantile) {
            Some(v) => ScalarValue::Float(v),
            None => ScalarValue::Null,
        }
    }

    /// Estimated heap size in bytes.
    pub fn heap_size(&self) -> usize {
        self.sketch.heap_size()
    }
}

impl fmt::Display for PercentileState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "P{}(count={})",
            (self.quantile * 100.0) as u32,
            self.sketch.count()
        )
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── DDSketch basics ─────────────────────────────────────────────────────

    #[test]
    fn empty_sketch_returns_none() {
        let sketch = DDSketch::new();
        assert!(sketch.is_empty());
        assert_eq!(sketch.count(), 0);
        assert_eq!(sketch.quantile(0.5), None);
    }

    #[test]
    fn single_value() {
        let mut sketch = DDSketch::new();
        sketch.insert(42.0);
        assert_eq!(sketch.count(), 1);
        let p50 = sketch.quantile(0.5).unwrap();
        assert!((p50 - 42.0).abs() / 42.0 < 0.02);
    }

    #[test]
    fn uniform_distribution_p50() {
        let mut sketch = DDSketch::new();
        for i in 1..=1000 {
            sketch.insert(i as f64);
        }
        let p50 = sketch.quantile(0.5).unwrap();
        let expected = 500.0;
        let relative_error = (p50 - expected).abs() / expected;
        assert!(
            relative_error < 0.02,
            "P50 relative error {relative_error} exceeds 2% (got {p50}, expected {expected})"
        );
    }

    #[test]
    fn uniform_distribution_p90() {
        let mut sketch = DDSketch::new();
        for i in 1..=1000 {
            sketch.insert(i as f64);
        }
        let p90 = sketch.quantile(0.9).unwrap();
        let expected = 900.0;
        let relative_error = (p90 - expected).abs() / expected;
        assert!(
            relative_error < 0.02,
            "P90 relative error {relative_error} exceeds 2% (got {p90}, expected {expected})"
        );
    }

    #[test]
    fn uniform_distribution_p95() {
        let mut sketch = DDSketch::new();
        for i in 1..=1000 {
            sketch.insert(i as f64);
        }
        let p95 = sketch.quantile(0.95).unwrap();
        let expected = 950.0;
        let relative_error = (p95 - expected).abs() / expected;
        assert!(
            relative_error < 0.02,
            "P95 relative error {relative_error} exceeds 2% (got {p95}, expected {expected})"
        );
    }

    #[test]
    fn uniform_distribution_p99() {
        let mut sketch = DDSketch::new();
        for i in 1..=1000 {
            sketch.insert(i as f64);
        }
        let p99 = sketch.quantile(0.99).unwrap();
        let expected = 990.0;
        let relative_error = (p99 - expected).abs() / expected;
        assert!(
            relative_error < 0.02,
            "P99 relative error {relative_error} exceeds 2% (got {p99}, expected {expected})"
        );
    }

    #[test]
    fn exponential_distribution_accuracy() {
        // Simulate an exponential-like distribution with a reasonable
        // range: values from ~1 to ~22026 (e^0.001 .. e^10).
        let mut sketch = DDSketch::new();
        let mut values: Vec<f64> = Vec::new();
        for i in 1..=10000 {
            let v = (i as f64 * 0.001).exp();
            sketch.insert(v);
            values.push(v);
        }
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());

        for &q in &[0.5, 0.9, 0.95, 0.99] {
            let estimated = sketch.quantile(q).unwrap();
            let idx = (q * (values.len() - 1) as f64).round() as usize;
            let actual = values[idx];
            let relative_error = (estimated - actual).abs() / actual;
            assert!(
                relative_error < 0.02,
                "P{} relative error {relative_error} exceeds 2% (got {estimated}, expected {actual})",
                (q * 100.0) as u32
            );
        }
    }

    #[test]
    fn bimodal_distribution_accuracy() {
        let mut sketch = DDSketch::new();
        let mut values: Vec<f64> = Vec::new();

        // Two clusters: around 10 and around 1000.
        for i in 0..5000 {
            let v = 10.0 + (i as f64) * 0.01;
            sketch.insert(v);
            values.push(v);
        }
        for i in 0..5000 {
            let v = 1000.0 + (i as f64) * 0.1;
            sketch.insert(v);
            values.push(v);
        }
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());

        for &q in &[0.5, 0.9, 0.95, 0.99] {
            let estimated = sketch.quantile(q).unwrap();
            let idx = (q * (values.len() - 1) as f64).round() as usize;
            let actual = values[idx];
            let relative_error = (estimated - actual).abs() / actual;
            assert!(
                relative_error < 0.02,
                "bimodal P{} relative error {relative_error} exceeds 2% (got {estimated}, expected {actual})",
                (q * 100.0) as u32
            );
        }
    }

    #[test]
    fn negative_values() {
        let mut sketch = DDSketch::new();
        for i in 1..=100 {
            sketch.insert(-(i as f64));
        }
        let p50 = sketch.quantile(0.5).unwrap();
        assert!(p50 < 0.0, "P50 of negative values should be negative");
        let expected = -50.0;
        let relative_error = (p50 - expected).abs() / expected.abs();
        assert!(
            relative_error < 0.02,
            "negative P50 relative error {relative_error} exceeds 2%"
        );
    }

    #[test]
    fn mixed_positive_negative_and_zero() {
        let mut sketch = DDSketch::new();
        for i in -100..=100 {
            sketch.insert(i as f64);
        }
        assert_eq!(sketch.count(), 201);
        let p50 = sketch.quantile(0.5).unwrap();
        // Median of -100..=100 is 0.
        assert!(
            p50.abs() < 2.0,
            "P50 of symmetric range should be near 0, got {p50}"
        );
    }

    // ── Merge ────────────────────────────────────────────────────────────────

    #[test]
    fn merge_two_sketches() {
        let mut a = DDSketch::new();
        let mut b = DDSketch::new();

        for i in 1..=500 {
            a.insert(i as f64);
        }
        for i in 501..=1000 {
            b.insert(i as f64);
        }
        a.merge(&b);

        assert_eq!(a.count(), 1000);
        let p50 = a.quantile(0.5).unwrap();
        let expected = 500.0;
        let relative_error = (p50 - expected).abs() / expected;
        assert!(
            relative_error < 0.02,
            "merged P50 relative error {relative_error} exceeds 2%"
        );
    }

    #[test]
    fn merge_associativity() {
        // (A merge B) merge C should produce the same result as
        // A merge (B merge C).
        let values_a: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let values_b: Vec<f64> = (101..=200).map(|i| i as f64).collect();
        let values_c: Vec<f64> = (201..=300).map(|i| i as f64).collect();

        let make_sketch = |vals: &[f64]| -> DDSketch {
            let mut s = DDSketch::new();
            for &v in vals {
                s.insert(v);
            }
            s
        };

        // (A merge B) merge C
        let mut left = make_sketch(&values_a);
        let b = make_sketch(&values_b);
        let c = make_sketch(&values_c);
        left.merge(&b);
        left.merge(&c);

        // A merge (B merge C)
        let mut right = make_sketch(&values_a);
        let mut bc = make_sketch(&values_b);
        bc.merge(&make_sketch(&values_c));
        right.merge(&bc);

        for &q in &[0.5, 0.9, 0.95, 0.99] {
            let l = left.quantile(q).unwrap();
            let r = right.quantile(q).unwrap();
            assert!(
                (l - r).abs() < 1e-10,
                "merge associativity violated at P{}: left={l}, right={r}",
                (q * 100.0) as u32
            );
        }
    }

    #[test]
    fn merge_with_empty() {
        let mut sketch = DDSketch::new();
        for i in 1..=100 {
            sketch.insert(i as f64);
        }
        let before = sketch.quantile(0.5).unwrap();
        sketch.merge(&DDSketch::new());
        let after = sketch.quantile(0.5).unwrap();
        assert_eq!(before, after);
    }

    #[test]
    fn merge_into_empty() {
        let mut empty = DDSketch::new();
        let mut sketch = DDSketch::new();
        for i in 1..=100 {
            sketch.insert(i as f64);
        }
        empty.merge(&sketch);
        assert_eq!(empty.count(), 100);
        assert!(empty.quantile(0.5).is_some());
    }

    // ── Size bounds ─────────────────────────────────────────────────────────

    #[test]
    fn sketch_size_within_2kb_for_2_decade_range() {
        // The design doc's "~1-2 KB per group per percentile" refers to
        // a typical 2-decade analytics range (e.g. latencies 1-100ms).
        // At 1% accuracy, log-mapping uses ~50 bins per decade, so
        // 2 decades ≈ 100 bins = 800 bytes.
        let mut sketch = DDSketch::new();
        for i in 1..=10000 {
            let v = i as f64; // 1.0 to 10000.0 — about 4 decades but most values cluster
            sketch.insert(v);
        }
        // Even 4 decades stays reasonable. The key metric is the
        // 2-decade case matching the "~1-2 KB" claim.
        let mut narrow = DDSketch::new();
        for i in 1..=10000 {
            let v = 1.0 + (i as f64) * 0.01; // 1.01 to 101.0 — ~2 decades
            narrow.insert(v);
        }
        let size = narrow.heap_size();
        assert!(
            size <= 2048,
            "sketch heap size {size} exceeds 2 KB for 2-decade range"
        );
    }

    #[test]
    fn sketch_size_bounded_for_4_decade_range() {
        // Wider analytics range: 4 decades (0.001 to 10.0). The dense
        // store spans ~460 bins = ~3.7 KB. This is acceptable for the
        // per-group budget at the 1M-group / 3 GB query cap.
        let mut sketch = DDSketch::new();
        for i in 1..=10000 {
            let v = i as f64 * 0.001; // 0.001 to 10.0
            sketch.insert(v);
        }
        let size = sketch.heap_size();
        assert!(
            size <= 8192,
            "sketch heap size {size} exceeds 8 KB for 4-decade range"
        );
    }

    #[test]
    fn sketch_size_bounded_for_wide_range() {
        // Even with values spanning many decades (1 to 10M),
        // the sketch stays bounded by MAX_NUM_BUCKETS.
        let mut sketch = DDSketch::new();
        for i in (1..=10_000_000u64).step_by(1000) {
            sketch.insert(i as f64);
        }
        let size = sketch.heap_size();
        // At 0.01 accuracy, log-mapping uses ~100 buckets per decade.
        // 7 decades (1 to 10M) → ~700 buckets → ~5.6 KB.
        // Well below the MAX_NUM_BUCKETS cap.
        assert!(
            size <= MAX_NUM_BUCKETS * std::mem::size_of::<u64>(),
            "sketch heap size {size} exceeds max bucket allocation"
        );
    }

    #[test]
    fn sketch_size_capped_by_max_buckets() {
        let mut sketch = DDSketch::new();
        // Insert values spanning an enormous range to stress bucket growth.
        for exp in -100..=100 {
            sketch.insert(2.0_f64.powi(exp));
        }
        let size = sketch.heap_size();
        // MAX_NUM_BUCKETS * 8 bytes = 16384 bytes per store.
        let max_expected = MAX_NUM_BUCKETS * 8 * 2 + std::mem::size_of::<DDSketch>();
        assert!(
            size <= max_expected,
            "sketch heap size {size} exceeds max expected {max_expected}"
        );
    }

    // ── NaN and Infinity handling ───────────────────────────────────────────

    #[test]
    fn nan_and_infinity_skipped() {
        let mut sketch = DDSketch::new();
        sketch.insert(f64::NAN);
        sketch.insert(f64::INFINITY);
        sketch.insert(f64::NEG_INFINITY);
        assert_eq!(sketch.count(), 0);
        assert!(sketch.is_empty());
    }

    // ── PercentileState ─────────────────────────────────────────────────────

    #[test]
    fn percentile_state_p50() {
        let mut state = PercentileState::new(AggFunction::P50);
        for i in 1..=1000 {
            state.update(&ScalarValue::Int(i));
        }
        if let ScalarValue::Float(v) = state.finalize() {
            let relative_error = (v - 500.0).abs() / 500.0;
            assert!(
                relative_error < 0.02,
                "P50 relative error {relative_error} exceeds 2%"
            );
        } else {
            panic!("expected Float from P50 finalize");
        }
    }

    #[test]
    fn percentile_state_p99_floats() {
        let mut state = PercentileState::new(AggFunction::P99);
        for i in 1..=1000 {
            state.update(&ScalarValue::Float(i as f64 * 0.1));
        }
        if let ScalarValue::Float(v) = state.finalize() {
            let expected = 99.0;
            let relative_error = (v - expected).abs() / expected;
            assert!(
                relative_error < 0.02,
                "P99 relative error {relative_error} exceeds 2%"
            );
        } else {
            panic!("expected Float from P99 finalize");
        }
    }

    #[test]
    fn percentile_state_empty_is_null() {
        let state = PercentileState::new(AggFunction::P90);
        assert_eq!(state.finalize(), ScalarValue::Null);
    }

    #[test]
    fn percentile_state_skips_nulls() {
        let mut state = PercentileState::new(AggFunction::P50);
        state.update(&ScalarValue::Null);
        state.update(&ScalarValue::Int(100));
        state.update(&ScalarValue::Null);
        assert_eq!(state.sketch.count(), 1);
    }

    #[test]
    fn percentile_state_skips_strings() {
        let mut state = PercentileState::new(AggFunction::P50);
        state.update(&ScalarValue::String("hello".into()));
        assert_eq!(state.sketch.count(), 0);
    }

    #[test]
    fn percentile_state_merge() {
        let mut a = PercentileState::new(AggFunction::P50);
        let mut b = PercentileState::new(AggFunction::P50);
        for i in 1..=500 {
            a.update(&ScalarValue::Int(i));
        }
        for i in 501..=1000 {
            b.update(&ScalarValue::Int(i));
        }
        a.merge(&b);
        if let ScalarValue::Float(v) = a.finalize() {
            let relative_error = (v - 500.0).abs() / 500.0;
            assert!(
                relative_error < 0.02,
                "merged P50 relative error {relative_error} exceeds 2%"
            );
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn percentile_state_heap_size_positive() {
        let mut state = PercentileState::new(AggFunction::P95);
        for i in 1..=1000 {
            state.update(&ScalarValue::Int(i));
        }
        assert!(state.heap_size() > 0);
    }

    // ── LogarithmicMapping ──────────────────────────────────────────────────

    #[test]
    fn mapping_index_monotonic() {
        let mapping = LogarithmicMapping::new(0.01);
        let mut prev_index = mapping.index(1.0);
        for i in 2..=1000 {
            let index = mapping.index(i as f64);
            assert!(
                index >= prev_index,
                "mapping index not monotonic at {i}: {index} < {prev_index}"
            );
            prev_index = index;
        }
    }

    #[test]
    fn mapping_value_roundtrip() {
        let mapping = LogarithmicMapping::new(0.01);
        for &v in &[1.0, 10.0, 100.0, 1000.0, 0.001, 0.5] {
            let index = mapping.index(v);
            let lower = mapping.lower_bound(index);
            let upper = mapping.value(index);
            assert!(
                lower <= v * (1.0 + 1e-10) && v <= upper * (1.0 + 1e-10),
                "value {v} not in bucket [{lower}, {upper}] at index {index}"
            );
        }
    }

    // ── DenseStore ──────────────────────────────────────────────────────────

    #[test]
    fn dense_store_basic() {
        let mut store = DenseStore::new();
        assert!(store.is_empty());
        store.add(5, 1);
        assert_eq!(store.count, 1);
        store.add(5, 1);
        assert_eq!(store.count, 2);
        assert_eq!(store.bins[0], 2);
    }

    #[test]
    fn dense_store_growth() {
        let mut store = DenseStore::new();
        store.add(10, 1);
        store.add(5, 1);
        store.add(15, 1);
        assert_eq!(store.count, 3);
        assert_eq!(store.offset, 5);
        assert_eq!(store.bins.len(), 11); // indices 5..=15
    }
}
