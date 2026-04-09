# Aggregation Test Fixtures

Integration test fixtures for aggregation operators: count, sum, avg, min, max, percentiles, group-by, and count_distinct.

## Fixture Format

Each test is a subdirectory containing:
- `input.json` — events to ingest
- `query.bql` — BQL query to execute
- `expected.json` — expected query output
