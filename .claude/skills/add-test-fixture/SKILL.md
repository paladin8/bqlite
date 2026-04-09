# Add an Integration Test Fixture

## Fixture Format

Each test is a directory under `tests/suite/<category>/`:

```
tests/suite/<category>/<test_name>/
  ├── input.json      # Events to ingest
  ├── query.bql       # BQL query to execute
  └── expected.json   # Expected query output
```

## input.json

Array of event objects:
```json
[
  {"entity_id": "user1", "timestamp": "2024-01-01T00:00:00Z", "event_type": "signup", "properties": {}},
  {"entity_id": "user1", "timestamp": "2024-01-01T01:00:00Z", "event_type": "purchase", "properties": {"amount": 42.0}}
]
```

## query.bql

A single BQL query:
```sql
match(signup -> purchase) within 7d by entity_id
```

## expected.json

The expected output as an array of result records.

## Edge Cases to Cover

- Empty input (no events)
- Single-event entities
- Entity event limit enforcement (skip-and-flag)
- Events spanning segment boundaries
- Patterns with no matches
- Time window boundary (exactly at limit)

## Steps

1. Create the test directory
2. Write `input.json` with representative events
3. Write `query.bql` with the query under test
4. Write `expected.json` with expected output
5. Run `cargo test` to verify

No Rust code needed — just data files.
