#!/usr/bin/env bash
# Mirrors .github/workflows/ci.yml so agents can reproduce CI locally before
# committing. Keep these steps in sync with the workflow.
set -euo pipefail

cd "$(dirname "$0")/.."

run() {
  echo ""
  echo "==> $*"
  "$@"
}

run cargo fmt --all --check
run scripts/check-dep-direction.sh
run cargo clippy --all-targets --all-features -- -D warnings
run cargo build --all-targets
run env RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
run cargo test --all-targets
run python3 -m unittest discover -s scripts/agents -p 'test_*.py'

echo ""
echo "local-ci: all checks passed"
