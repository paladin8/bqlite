#!/usr/bin/env bash
#
# check-dep-direction.sh — verify internal crate dependencies match the
# dependency graph declared in docs/architecture.md.
#
# The graph is layered: crates may only depend on crates below them. This
# script walks each crate's Cargo.toml, extracts its internal `bqlite-*`
# dependencies (under `[dependencies]`, `[dev-dependencies]`, and
# `[build-dependencies]`), and fails with a clear error on any violation.
#
# When adding a new crate: update the `allowed_deps` table below AND the
# graph in docs/architecture.md and CLAUDE.md in the same commit.
#
# Assumptions about the crate manifests this script can parse:
#   - Internal deps are declared as `bqlite-xxx = { workspace = true, ... }`
#     or `bqlite-xxx.workspace = true` (inline key form, not `[dependencies.x]`
#     subtable form).
#   - No `target.'cfg(...)'.dependencies` tables — all deps are unconditional.
# If either assumption changes, this script must be updated.

set -euo pipefail

# Allowed internal dependencies for each crate. Whitespace-separated.
# Must mirror docs/architecture.md §"Dependency Direction".
allowed_deps() {
    case "$1" in
        bqlite-core)      echo "" ;;
        bqlite-ast)       echo "bqlite-core" ;;
        bqlite-storage)   echo "bqlite-core" ;;
        bqlite-parser)    echo "bqlite-ast" ;;
        bqlite-planner)   echo "bqlite-ast bqlite-core" ;;
        bqlite-operators) echo "bqlite-core bqlite-storage bqlite-planner" ;;
        bqlite-engine)    echo "bqlite-core bqlite-storage bqlite-planner bqlite-operators" ;;
        bqlite-cli)       echo "bqlite-engine" ;;
        bqlite-ffi)       echo "bqlite-engine" ;;
        bqlite)           echo "bqlite-core bqlite-ast bqlite-parser bqlite-engine" ;;
        *) return 2 ;;
    esac
}

# All crates known to the graph — used to detect crates that exist in the
# table but are missing from crates/.
KNOWN_CRATES="bqlite bqlite-core bqlite-ast bqlite-storage bqlite-parser bqlite-planner bqlite-operators bqlite-engine bqlite-cli bqlite-ffi"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CRATES_DIR="$REPO_ROOT/crates"

if [[ ! -d "$CRATES_DIR" ]]; then
    printf 'error: crates directory not found at %s\n' "$CRATES_DIR" >&2
    exit 2
fi

errors=0
seen_crates=""

# Walk each crate directory in sorted order for deterministic output.
for crate_dir in "$CRATES_DIR"/*/; do
    crate="$(basename "$crate_dir")"
    manifest="$crate_dir/Cargo.toml"
    seen_crates="$seen_crates $crate"

    if [[ ! -f "$manifest" ]]; then
        printf 'error: %s is missing Cargo.toml\n' "$crate" >&2
        errors=$((errors + 1))
        continue
    fi

    if ! allowed="$(allowed_deps "$crate")"; then
        printf "error: unknown crate '%s' — add it to scripts/check-dep-direction.sh and docs/architecture.md\n" "$crate" >&2
        errors=$((errors + 1))
        continue
    fi

    # Extract internal (bqlite-*) dependency names from any [*dependencies*]
    # section. Matches both `name = { ... }` and `name.workspace = true`
    # inline-key forms. Deduplicated and sorted so the comparison loop is
    # stable across awk implementations.
    actual="$(awk '
        /^[[:space:]]*\[.*dependencies.*\][[:space:]]*$/ { in_deps = 1; next }
        /^[[:space:]]*\[/                                { in_deps = 0; next }
        in_deps && /^[[:space:]]*bqlite[-a-zA-Z0-9_]*[[:space:].=]/ {
            name = $0
            sub(/^[[:space:]]*/, "", name)
            sub(/[[:space:].=].*$/, "", name)
            if (name != "") print name
        }
    ' "$manifest" | sort -u)"

    for dep in $actual; do
        if [[ "$dep" == "$crate" ]]; then
            printf 'error: %s depends on itself\n' "$crate" >&2
            errors=$((errors + 1))
            continue
        fi
        ok=0
        for a in $allowed; do
            if [[ "$dep" == "$a" ]]; then
                ok=1
                break
            fi
        done
        if [[ $ok -eq 0 ]]; then
            printf 'error: %s depends on %s (allowed: %s)\n' \
                "$crate" "$dep" "${allowed:-<none>}" >&2
            errors=$((errors + 1))
        fi
    done
done

# Detect crates declared in the allowed-deps table but missing from crates/.
for known in $KNOWN_CRATES; do
    case " $seen_crates " in
        *" $known "*) ;;
        *)
            printf "error: crate '%s' is in the dependency graph but missing from crates/\n" "$known" >&2
            errors=$((errors + 1))
            ;;
    esac
done

if [[ $errors -gt 0 ]]; then
    printf '\ndependency direction check failed with %d error(s)\n' "$errors" >&2
    printf 'see docs/architecture.md §"Dependency Direction" for the canonical graph\n' >&2
    exit 1
fi

echo "dependency direction check: OK"
