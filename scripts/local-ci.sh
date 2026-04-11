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
run cargo test --all-targets
run python3 -m unittest discover -s scripts -p 'test_task_tool*.py'

echo ""
echo "local-ci: all checks passed"
