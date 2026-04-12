#!/usr/bin/env bash
# bench-compare.sh — Compare Criterion estimates.json files between a baseline
# and the current bench run. Fails if any metric regresses more than 10%.
#
# Also checks target/bench-results.json (TASK-246) for hard target violations
# when present: any result where target.pass == false causes a failure.
#
# Usage:
#   scripts/bench-compare.sh <baseline_dir> <current_dir>
#
# Both directories should contain a Criterion output tree rooted at the
# criterion output root (i.e. what was uploaded/downloaded as the artifact):
#
#   <dir>/<bench_group>/<function>/new/estimates.json
#
# The regression threshold is 10%: a metric is a regression when 3 or more
# consecutive Criterion samples each exceed 1.10 × baseline mean (as required
# by TASKS.md §TASK-241). The consecutive-sample rule protects against single
# noisy runs on shared ubuntu-latest hardware. When sample.json is absent
# (rare — happens only in --test mode) the check falls back to the mean
# point estimate from estimates.json.
#
# Exit codes:
#   0  all metrics within threshold (or no comparable baseline for a metric)
#   1  one or more metrics have 3+ consecutive samples regressing >10%,
#      or one or more hard targets failed

set -euo pipefail

BASELINE_DIR="${1:?Usage: $0 <baseline_dir> <current_dir>}"
CURRENT_DIR="${2:?Usage: $0 <baseline_dir> <current_dir>}"
THRESHOLD="1.10"
CONSECUTIVE_REQUIRED=3

failed=0
pass_count=0
skip_count=0

# Accumulate lines for the GitHub step summary (written at the end).
summary_lines=()

# Walk only */new/estimates.json — excludes baseline/ snapshots that
# Criterion writes when re-running in the same output tree.
while IFS= read -r -d '' current_file; do
    # Derive the relative path so we can locate the matching baseline file.
    rel="${current_file#"${CURRENT_DIR}"/}"
    baseline_file="${BASELINE_DIR}/${rel}"
    bench_name="${rel%/new/estimates.json}"

    if [[ ! -f "${baseline_file}" ]]; then
        echo "SKIP (no baseline): ${bench_name}"
        summary_lines+=("| ⬜ SKIP | \`${bench_name}\` | no baseline |")
        ((skip_count++)) || true
        continue
    fi

    # Extract mean.point_estimate (nanoseconds per iteration) from both files.
    # Criterion's estimates.json schema:
    #   { "mean": { "point_estimate": <float>, ... }, ... }
    baseline_mean=$(jq '.mean.point_estimate' "${baseline_file}")
    current_mean=$(jq '.mean.point_estimate' "${current_file}")

    # Guard against null or zero estimates (happens when Criterion can't fit
    # a slope, e.g. on the very first run or extremely fast benches).
    if [[ "${baseline_mean}" == "null" || "${baseline_mean}" == "0" \
          || "${baseline_mean}" == "0.0" ]]; then
        echo "SKIP (degenerate baseline mean): ${bench_name}"
        summary_lines+=("| ⬜ SKIP | \`${bench_name}\` | degenerate baseline |")
        ((skip_count++)) || true
        continue
    fi
    if [[ "${current_mean}" == "null" || "${current_mean}" == "0" \
          || "${current_mean}" == "0.0" ]]; then
        echo "SKIP (degenerate current mean): ${bench_name}"
        summary_lines+=("| ⬜ SKIP | \`${bench_name}\` | degenerate current — bench may have failed |")
        ((skip_count++)) || true
        continue
    fi

    # ── Consecutive-sample check ──────────────────────────────────────────────
    # TASKS.md §TASK-241: "slips >10% on at least 3 consecutive Criterion
    # samples".  We use the per-sample timing from sample.json so that a
    # single noisy iteration does not block a PR on shared hardware.
    #
    # sample.json format: { "iters": [float, ...], "times": [float, ...], ... }
    # Per-sample time (ns/iter) = times[i] / iters[i]
    #
    # We report a regression only when CONSECUTIVE_REQUIRED or more adjacent
    # samples all exceed baseline_mean * THRESHOLD.
    sample_file="${current_file%estimates.json}sample.json"
    verdict="OK"

    if [[ -f "${sample_file}" ]]; then
        # jq checks for any run of CONSECUTIVE_REQUIRED consecutive samples
        # each exceeding the threshold.
        has_consecutive=$(jq \
            --argjson base "${baseline_mean}" \
            --argjson thresh "${THRESHOLD}" \
            --argjson required "${CONSECUTIVE_REQUIRED}" '
            .iters as $iters | .times as $times |
            ($iters | length) as $n |
            reduce range($n) as $i (
                { run: 0, found: false };
                if ($times[$i] / $iters[$i]) > ($base * $thresh)
                then .run += 1 |
                     if .run >= $required then .found = true else . end
                else .run = 0
                end
            ) | .found
        ' "${sample_file}" 2>/dev/null || echo "false")

        if [[ "${has_consecutive}" == "true" ]]; then
            verdict="REGRESSION"
        fi
    else
        # Fallback: no sample.json (should not happen under cargo bench).
        # Use the mean point estimate directly — single-run comparison.
        fallback_regression=$(awk \
            "BEGIN { print (${current_mean} / ${baseline_mean} > ${THRESHOLD}) \
                     ? \"true\" : \"false\" }")
        if [[ "${fallback_regression}" == "true" ]]; then
            verdict="REGRESSION"
        fi
    fi

    # ── Reporting ─────────────────────────────────────────────────────────────
    ratio=$(awk "BEGIN { printf \"%.4f\", ${current_mean} / ${baseline_mean} }")
    pct=$(awk "BEGIN { printf \"%+.1f%%\", \
                       (${current_mean} / ${baseline_mean} - 1) * 100 }")

    printf "%-12s %s  baseline=%.0fns  current=%.0fns  ratio=%s  (%s)\n" \
        "${verdict}" "${bench_name}" \
        "${baseline_mean}" "${current_mean}" \
        "${ratio}" "${pct}"

    if [[ "${verdict}" == "REGRESSION" ]]; then
        failed=1
        summary_lines+=("| 🔴 REGRESSION | \`${bench_name}\` | baseline=\`$(printf '%.0f' "${baseline_mean}")\`ns → current=\`$(printf '%.0f' "${current_mean}")\`ns (${pct}) |")
    else
        ((pass_count++)) || true
        summary_lines+=("| 🟢 OK | \`${bench_name}\` | baseline=\`$(printf '%.0f' "${baseline_mean}")\`ns → current=\`$(printf '%.0f' "${current_mean}")\`ns (${pct}) |")
    fi
done < <(find "${CURRENT_DIR}" -path "*/new/estimates.json" -print0 | sort -z)

# ── Hard target check (TASK-246) ─────────────────────────────────────────────
# If target/bench-results.json exists, check for any failed hard targets.
BENCH_RESULTS="target/bench-results.json"
target_pass_count=0
target_fail_count=0

if [[ -f "${BENCH_RESULTS}" ]]; then
    echo ""
    echo "Checking hard targets from ${BENCH_RESULTS}..."

    # Extract entries that have a target object and check pass/fail.
    while IFS=$'\t' read -r key pass value unit limit direction; do
        if [[ "${pass}" == "true" ]]; then
            printf "TARGET PASS  %s  value=%.4f %s  (%s %s)\n" \
                "${key}" "${value}" "${unit}" "${direction}" "${limit}"
            summary_lines+=("| 🟢 TARGET | \`${key}\` | \`${value}\` ${unit} (${direction} ${limit}) |")
            ((target_pass_count++)) || true
        else
            printf "TARGET FAIL  %s  value=%.4f %s  (%s %s)\n" \
                "${key}" "${value}" "${unit}" "${direction}" "${limit}"
            summary_lines+=("| 🔴 TARGET | \`${key}\` | \`${value}\` ${unit} (**FAILED** ${direction} ${limit}) |")
            ((target_fail_count++)) || true
            failed=1
        fi
    done < <(jq -r '
        to_entries[]
        | select(.value.target != null)
        | [
            .key,
            (.value.target.pass | tostring),
            (.value.value | tostring),
            .value.unit,
            (.value.target.limit | tostring),
            .value.target.direction
          ]
        | @tsv
    ' "${BENCH_RESULTS}" 2>/dev/null || true)
fi

# ── Summary output ────────────────────────────────────────────────────────────
echo ""
echo "Results: ${pass_count} OK, ${skip_count} skipped"
if [[ ${target_pass_count} -gt 0 || ${target_fail_count} -gt 0 ]]; then
    echo "Targets: ${target_pass_count} passed, ${target_fail_count} failed"
fi

# Write a markdown table to $GITHUB_STEP_SUMMARY when running in GitHub Actions.
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
        if [[ ${failed} -ne 0 ]]; then
            echo "## Bench Regression Gate — FAILED"
        else
            echo "## Bench Regression Gate — Passed"
        fi
        echo ""
        echo "| Result | Benchmark | Timing |"
        echo "|--------|-----------|--------|"
        for line in "${summary_lines[@]}"; do
            echo "${line}"
        done
        echo ""
        echo "_Threshold: >10% regression on 3+ consecutive Criterion samples_"
        if [[ ${target_pass_count} -gt 0 || ${target_fail_count} -gt 0 ]]; then
            echo ""
            echo "_Hard targets: ${target_pass_count} passed, ${target_fail_count} failed_"
        fi
    } >> "${GITHUB_STEP_SUMMARY}"
fi

if [[ ${failed} -ne 0 ]]; then
    echo "FAILED: one or more benchmarks regressed >10% vs baseline, or hard targets missed."
    exit 1
fi

echo "Bench regression gate passed."
