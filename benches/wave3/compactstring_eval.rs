//! CompactString evaluation microbenchmark (TASK-332).
//!
//! Measures the cost of the three candidate string representations in the
//! matcher hot-path usage patterns:
//!
//! 1. **Event-type comparison** (`transition.event_type == event_type`):
//!    owned string vs borrowed `&str` equality check, executed once per event
//!    per active NFA state.
//!
//! 2. **Binding value extraction** (Arrow `StringViewArray` → owned string):
//!    the allocation cost of extracting a string from an Arrow column and
//!    storing it in a `BindingValue` variant.
//!
//! 3. **Binding value clone** (`BindingValue::clone()` for track creation,
//!    step0 entry insertion, and completion collection).
//!
//! 4. **Binding key hash + equality** (HashMap lookup keyed by binding values).
//!
//! Candidates:
//! - `String` (std): 24 bytes on stack (ptr + len + capacity)
//! - `Box<str>` (current): 16 bytes on stack (ptr + len), always heap
//! - `CompactString` (compact_str 0.9): 24 bytes on stack, inline for ≤ 24 bytes
//!
//! Run with:
//! ```bash
//! cargo bench -p bqlite-benches --bench compactstring_eval
//! ```

use std::collections::HashMap;
use std::hint::black_box;

use compact_str::CompactString;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

// ─────────────────────────────────────────────────────────────────────────────
// Test strings representative of real workloads
// ─────────────────────────────────────────────────────────────────────────────

/// Short event-type names (≤ 24 bytes) — the common case in analytics.
const SHORT_STRINGS: &[&str] = &[
    "signup",
    "activation",
    "purchase",
    "page_view",
    "checkout",
    "login",
    "add_to_cart",
    "remove_from_cart",
];

/// Medium binding values (8–24 bytes) — plan names, categories.
const MEDIUM_STRINGS: &[&str] = &[
    "premium",
    "basic_plan",
    "enterprise",
    "free_trial_monthly",
    "annual_subscription",
];

/// Long strings (> 24 bytes) — URLs, free-form text.
const LONG_STRINGS: &[&str] = &[
    "this_is_a_longer_event_type_name_that_exceeds_inline",
    "https://example.com/path/to/resource?query=value",
    "very_long_category_name_used_in_enterprise_analytics",
];

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark: event-type equality comparison
// ─────────────────────────────────────────────────────────────────────────────

fn bench_event_type_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_type_comparison");

    for &label in &["short", "medium", "long"] {
        let strings: &[&str] = match label {
            "short" => SHORT_STRINGS,
            "medium" => MEDIUM_STRINGS,
            _ => LONG_STRINGS,
        };

        // Pre-build owned representations (simulating compiled NFA transitions).
        let owned_strings: Vec<String> = strings.iter().map(|s| s.to_string()).collect();
        let box_strs: Vec<Box<str>> = strings.iter().map(|s| Box::from(*s)).collect();
        let compact_strs: Vec<CompactString> =
            strings.iter().map(|s| CompactString::new(s)).collect();

        // Compare each owned string against a borrowed &str (simulating
        // `transition.event_type == event_type` in the hot loop).
        let borrowed: Vec<&str> = strings.to_vec();

        group.bench_function(BenchmarkId::new("String", label), |b| {
            b.iter(|| {
                let mut matches = 0u32;
                for owned in &owned_strings {
                    for &borrow in &borrowed {
                        if owned.as_str() == borrow {
                            matches += 1;
                        }
                    }
                }
                black_box(matches)
            })
        });

        group.bench_function(BenchmarkId::new("Box<str>", label), |b| {
            b.iter(|| {
                let mut matches = 0u32;
                for owned in &box_strs {
                    for &borrow in &borrowed {
                        if &**owned == borrow {
                            matches += 1;
                        }
                    }
                }
                black_box(matches)
            })
        });

        group.bench_function(BenchmarkId::new("CompactString", label), |b| {
            b.iter(|| {
                let mut matches = 0u32;
                for owned in &compact_strs {
                    for &borrow in &borrowed {
                        if owned.as_str() == borrow {
                            matches += 1;
                        }
                    }
                }
                black_box(matches)
            })
        });
    }

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark: string extraction (simulating Arrow → BindingValue)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_extraction(c: &mut Criterion) {
    let mut group = c.benchmark_group("binding_extraction");

    for &label in &["short", "medium", "long"] {
        let source: &str = match label {
            "short" => "premium",
            "medium" => "free_trial_monthly",
            _ => "this_is_a_longer_event_type_name_that_exceeds_inline",
        };

        let iterations = 10_000;

        group.bench_function(BenchmarkId::new("String", label), |b| {
            b.iter(|| {
                let mut v = Vec::with_capacity(iterations);
                for _ in 0..iterations {
                    v.push(black_box(source).to_string());
                }
                black_box(v)
            })
        });

        group.bench_function(BenchmarkId::new("Box<str>", label), |b| {
            b.iter(|| {
                let mut v = Vec::with_capacity(iterations);
                for _ in 0..iterations {
                    v.push(Box::<str>::from(black_box(source)));
                }
                black_box(v)
            })
        });

        group.bench_function(BenchmarkId::new("CompactString", label), |b| {
            b.iter(|| {
                let mut v = Vec::with_capacity(iterations);
                for _ in 0..iterations {
                    v.push(CompactString::new(black_box(source)));
                }
                black_box(v)
            })
        });
    }

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark: clone cost (simulating BindingValue clone for track creation)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("binding_clone");

    for &label in &["short", "medium", "long"] {
        let source: &str = match label {
            "short" => "premium",
            "medium" => "free_trial_monthly",
            _ => "this_is_a_longer_event_type_name_that_exceeds_inline",
        };

        let iterations = 10_000;

        let string_val = source.to_string();
        let box_val = Box::<str>::from(source);
        let compact_val = CompactString::new(source);

        group.bench_function(BenchmarkId::new("String", label), |b| {
            b.iter(|| {
                let mut v = Vec::with_capacity(iterations);
                for _ in 0..iterations {
                    v.push(black_box(&string_val).clone());
                }
                black_box(v)
            })
        });

        group.bench_function(BenchmarkId::new("Box<str>", label), |b| {
            b.iter(|| {
                let mut v = Vec::with_capacity(iterations);
                for _ in 0..iterations {
                    v.push(black_box(&box_val).clone());
                }
                black_box(v)
            })
        });

        group.bench_function(BenchmarkId::new("CompactString", label), |b| {
            b.iter(|| {
                let mut v = Vec::with_capacity(iterations);
                for _ in 0..iterations {
                    v.push(black_box(&compact_val).clone());
                }
                black_box(v)
            })
        });
    }

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark: HashMap lookup (simulating BindingKey → track index)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_hashmap_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("binding_key_hashmap");

    let keys: Vec<&str> = vec!["premium", "basic", "enterprise", "free_trial"];

    // Build hashmaps for each representation.
    let mut string_map: HashMap<Vec<String>, usize> = HashMap::new();
    let mut box_map: HashMap<Vec<Box<str>>, usize> = HashMap::new();
    let mut compact_map: HashMap<Vec<CompactString>, usize> = HashMap::new();

    for (i, &k) in keys.iter().enumerate() {
        string_map.insert(vec![k.to_string()], i);
        box_map.insert(vec![Box::from(k)], i);
        compact_map.insert(vec![CompactString::new(k)], i);
    }

    let iterations = 10_000;

    group.bench_function("String", |b| {
        b.iter(|| {
            let mut sum = 0usize;
            for _ in 0..iterations {
                for &k in &keys {
                    let key = vec![k.to_string()];
                    if let Some(&idx) = string_map.get(&key) {
                        sum += idx;
                    }
                }
            }
            black_box(sum)
        })
    });

    group.bench_function("Box<str>", |b| {
        b.iter(|| {
            let mut sum = 0usize;
            for _ in 0..iterations {
                for &k in &keys {
                    let key = vec![Box::<str>::from(k)];
                    if let Some(&idx) = box_map.get(&key) {
                        sum += idx;
                    }
                }
            }
            black_box(sum)
        })
    });

    group.bench_function("CompactString", |b| {
        b.iter(|| {
            let mut sum = 0usize;
            for _ in 0..iterations {
                for &k in &keys {
                    let key = vec![CompactString::new(k)];
                    if let Some(&idx) = compact_map.get(&key) {
                        sum += idx;
                    }
                }
            }
            black_box(sum)
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark: size_of and memory footprint
// ─────────────────────────────────────────────────────────────────────────────

fn bench_memory_layout(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_layout");

    // Verify size_of for documentation purposes. These run in ~0 time
    // but produce the assertion output.
    group.bench_function("size_check", |b| {
        b.iter(|| {
            let string_size = std::mem::size_of::<String>();
            let box_str_size = std::mem::size_of::<Box<str>>();
            let compact_size = std::mem::size_of::<CompactString>();
            black_box((string_size, box_str_size, compact_size))
        })
    });

    // Measure aggregate allocation for a realistic binding-track scenario:
    // 1000 entities, each with 1 string binding, 4 distinct values.
    let binding_values = ["premium", "basic", "enterprise", "free_trial"];
    let entities = 1000;
    let values_per_entity = 4;

    group.bench_function("aggregate_Box<str>", |b| {
        b.iter(|| {
            let mut all: Vec<Vec<Box<str>>> = Vec::with_capacity(entities);
            for _ in 0..entities {
                let mut entity_bindings = Vec::with_capacity(values_per_entity);
                for &v in &binding_values {
                    entity_bindings.push(Box::<str>::from(v));
                }
                all.push(entity_bindings);
            }
            black_box(all)
        })
    });

    group.bench_function("aggregate_CompactString", |b| {
        b.iter(|| {
            let mut all: Vec<Vec<CompactString>> = Vec::with_capacity(entities);
            for _ in 0..entities {
                let mut entity_bindings = Vec::with_capacity(values_per_entity);
                for &v in &binding_values {
                    entity_bindings.push(CompactString::new(v));
                }
                all.push(entity_bindings);
            }
            black_box(all)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_event_type_comparison,
    bench_extraction,
    bench_clone,
    bench_hashmap_lookup,
    bench_memory_layout,
);
criterion_main!(benches);
