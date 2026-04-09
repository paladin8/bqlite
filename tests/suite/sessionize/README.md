# Session Segmentation Test Fixtures

Integration test fixtures for session boundary detection: inactivity gap-based sessions, explicit end events, and session-scoped downstream queries.

## Fixture Format

Each test is a subdirectory containing:
- `input.json` — events to ingest
- `query.bql` — BQL query to execute
- `expected.json` — expected query output
