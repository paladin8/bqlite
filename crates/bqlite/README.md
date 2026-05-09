# bqlite

The top-level entry-point crate for the bqlite behavioral query engine.

`bqlite` is a thin re-export crate — its only job is to surface the public API of
the internal implementation crates under a single, stable dependency. Add `bqlite`
to your `Cargo.toml`; the internal crate split is an implementation detail.

## Re-exported surface

| Re-export | Source crate | Purpose |
|-----------|-------------|---------|
| `bqlite::ast` | `bqlite-ast` | AST types for programmatic query construction |
| `bqlite::types` | `bqlite-core` | Core types: `Event`, `Schema`, `Timestamp`, `PropertyValue` |
| `bqlite::engine` | `bqlite-engine` | `Engine` and `Database` — the query execution surface |
| `bqlite::parser` | `bqlite-parser` | BQL text → AST parser |
| `bqlite::BqliteError` | `bqlite-core` | Unified error type for all bqlite operations |
| `bqlite::Result<T>` | `bqlite-core` | Convenience alias: `Result<T, BqliteError>` |

## Error handling

All fallible bqlite APIs return `bqlite::Result<T>`, which is
`std::result::Result<T, BqliteError>`.

```rust
use bqlite::BqliteError;

// Construct and inspect a parse error.
let err = BqliteError::Parse("unexpected token: EOF".to_string());
assert_eq!(err.to_string(), "Parse error: unexpected token: EOF");
```

The error variants cover every layer — I/O, Arrow, schema, parse, plan,
execution, memory budget, cancellation, and worker panics:

```rust
use bqlite::BqliteError;

let err = BqliteError::MemoryBudgetExceeded { used: 500_000_000, budget: 256_000_000 };
assert!(err.to_string().contains("budget exceeded"));
```

## Database usage

```rust,ignore
use bqlite::engine::Engine;

let engine = Engine::open("./my_database/")?;
let result = engine.query("SELECT * FROM events LIMIT 10")?;
```
