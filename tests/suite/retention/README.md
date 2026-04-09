# Retention Analysis Test Fixtures

Integration test fixtures for retention cohort analysis: entry/returning event pairs, standard and custom interval brackets, and retention matrix computation.

## Fixture Format

Each test is a subdirectory containing:
- `input.json` — events to ingest
- `query.bql` — BQL query to execute
- `expected.json` — expected query output
