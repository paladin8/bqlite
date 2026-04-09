# Funnel Analysis Test Fixtures

Integration test fixtures for multi-step conversion funnels: stepwise conversion rates, held properties across steps, and time-windowed funnel completion.

## Fixture Format

Each test is a subdirectory containing:
- `input.json` — events to ingest
- `query.bql` — BQL query to execute
- `expected.json` — expected query output
