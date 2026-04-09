# Add New BQL Syntax

## Steps

1. **Add AST node types** in `crates/bqlite-ast/src/lib.rs`
   - Define the new node struct/enum variant
   - Derive necessary traits (Debug, Clone, PartialEq, Serialize)
2. **Add grammar rule** in `crates/bqlite-parser/src/`
   - Add the parsing logic for the new syntax
   - Handle error cases with clear messages
3. **Add logical plan mapping** in `crates/bqlite-planner/src/`
   - Map the AST node to a logical plan node
   - Define the output schema for the new operator
4. **Write tests** covering the new syntax
   - Valid syntax cases
   - Error cases (malformed input, type mismatches)
   - Edge cases (empty patterns, boundary values)
5. **Update** `docs/design/query-language.md` with the new syntax

## Verification

```bash
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

## Checklist

- [ ] AST node defined
- [ ] Parser rule implemented
- [ ] Logical plan mapping added
- [ ] Output schema declared
- [ ] Tests for valid and invalid syntax
- [ ] Query language design doc updated
