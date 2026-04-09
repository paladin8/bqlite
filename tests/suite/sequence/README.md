# Sequence Pattern Matching Test Fixtures

Integration test fixtures for temporal sequence/pattern matching: ordered event patterns with time windows, negation, repetition, and held properties.

## Fixture Format

Each test is a subdirectory containing:
- `input.json` — events to ingest
- `query.bql` — BQL query to execute
- `expected.json` — expected query output
