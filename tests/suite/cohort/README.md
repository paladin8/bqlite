# Cohort Test Fixtures

Integration test fixtures for query-computed cohorts: entity set materialization, cohort-query joins, cohort reuse across queries, and cohort-level property propagation.

## Fixture Format

Each test is a subdirectory containing:
- `input.json` — events to ingest
- `query.bql` — BQL query to execute
- `expected.json` — expected query output
