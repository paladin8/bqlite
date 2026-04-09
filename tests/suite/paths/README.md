# Path Analysis Test Fixtures

Integration test fixtures for Sankey-style path/flow aggregation: event sequence enumeration, path counting, and branching factor analysis.

## Fixture Format

Each test is a subdirectory containing:
- `input.json` — events to ingest
- `query.bql` — BQL query to execute
- `expected.json` — expected query output
