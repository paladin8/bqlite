# Fix CI Failures

## Diagnosis

Check which step failed in the CI pipeline:

1. **Format** (`cargo fmt --all --check`)
2. **Clippy** (`cargo clippy --all-targets --all-features -- -D warnings`)
3. **Build** (`cargo build --all-targets`)
4. **Test** (`cargo test --all-targets`)

## Common Fixes

### Formatting
```bash
cargo fmt --all
```
Then commit the changes.

### Clippy Lints
- Read the lint message carefully — clippy usually suggests the exact fix
- Common lints: unused imports, dead code, redundant clones, needless borrows
- Apply the suggested fix, don't suppress with `#[allow(...)]` unless justified

### Test Failures
```bash
cargo test <test_name> -- --nocapture
```
- Check test output for assertion details
- Verify test fixtures are correct
- Check for non-deterministic behavior (ordering, timing)

### Dependency Direction Violations
- Check the dependency rules in CLAUDE.md
- A crate may only depend on crates above it in the ordering
- Fix by moving shared types to a lower crate or restructuring the dependency

## Verification

After fixing, run the full CI suite locally:
```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo build --all-targets
cargo test --all-targets
```
