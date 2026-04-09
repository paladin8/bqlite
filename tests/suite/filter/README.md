# Filter Operator Test Fixtures

Integration test fixtures for predicate evaluation: property comparisons, regex matching, null handling, and compound predicates.

## Fixture Format

Each test is a subdirectory containing:
- `input.json` — events to ingest
- `query.bql` — BQL query to execute
- `expected.json` — expected query output
