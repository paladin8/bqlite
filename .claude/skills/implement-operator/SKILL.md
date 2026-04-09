# Implement a New Physical Operator

## Steps

1. **Create the operator file**: `crates/bqlite-operators/src/<operator_name>.rs`
2. **Implement the PhysicalOperator trait** (defined in `bqlite-planner`)
   - Define input schema requirements
   - Define output schema
   - Implement the execution logic (pull-based, entity-at-a-time for stateful ops)
3. **Register the operator** in the physical planner's operator registry
4. **Add the module** to `crates/bqlite-operators/src/lib.rs`
5. **Create test fixtures** in `tests/suite/<operator_name>/`
   - Each test: `input.json`, `query.bql`, `expected.json`
   - Cover edge cases: empty inputs, single-event entities, entity event limits
6. **Add a benchmark** in `benches/<operator_name>.rs`
7. **Update** `docs/quality-score.md`

## Verification

```bash
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo bench -- <operator_name>
```

## Checklist

- [ ] Operator file created and module registered
- [ ] PhysicalOperator trait implemented
- [ ] Output schema declared and validated
- [ ] Memory budget respected
- [ ] Test fixtures created with edge cases
- [ ] Benchmark added
- [ ] Documentation updated
