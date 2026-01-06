#!/usr/bin/env bash
# CLI Test Runner Entrypoint
# Runs cellarctl integration tests

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}[INFO]${NC} $*"; }
log_pass() { echo -e "${GREEN}[PASS]${NC} $*"; }
log_fail() { echo -e "${RED}[FAIL]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }

SCENARIOS_DIR="/tests/scenarios"
RESULTS_DIR="${RESULTS_DIR:-/results}"
TEST_SCENARIOS="${TEST_SCENARIOS:-all}"

# Wait for server to be ready
log_info "Waiting for server at ${CELLAR_SERVER_URL}..."
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if curl -sf "${CELLAR_SERVER_URL}/v1/health" > /dev/null 2>&1; then
        log_info "Server is ready"
        break
    fi
    attempt=$((attempt + 1))
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    log_fail "Server did not become ready in time"
    exit 1
fi

# Get list of scenarios to run
if [ "$TEST_SCENARIOS" = "all" ]; then
    scenarios=$(ls -1 "$SCENARIOS_DIR"/*.sh 2>/dev/null | sort)
else
    # Allow comma-separated or space-separated list
    scenarios=""
    for s in $(echo "$TEST_SCENARIOS" | tr ',' ' '); do
        if [ -f "$SCENARIOS_DIR/$s.sh" ]; then
            scenarios="$scenarios $SCENARIOS_DIR/$s.sh"
        elif [ -f "$SCENARIOS_DIR/$s" ]; then
            scenarios="$scenarios $SCENARIOS_DIR/$s"
        else
            log_warn "Scenario not found: $s"
        fi
    done
fi

if [ -z "$scenarios" ]; then
    log_fail "No test scenarios found"
    exit 1
fi

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  CLI Integration Tests"
echo "═══════════════════════════════════════════════════════════"
echo ""

passed=0
failed=0
failed_tests=""

for scenario in $scenarios; do
    name=$(basename "$scenario" .sh)

    echo -n "▶ Running: $name "

    # Run scenario and capture output
    log_file="$RESULTS_DIR/${name}.log"

    if bash "$scenario" > "$log_file" 2>&1; then
        echo ""
        log_pass "PASSED: $name"
        passed=$((passed + 1))
    else
        exit_code=$?
        echo ""
        log_fail "FAILED: $name (exit code: $exit_code)"
        failed=$((failed + 1))
        failed_tests="$failed_tests $name"

        # Show last 20 lines of log on failure
        echo "--- Last 20 lines of log ---"
        tail -20 "$log_file" || true
        echo "--- End of log ---"
    fi
    echo ""
done

echo "═══════════════════════════════════════════════════════════"
echo "  CLI Test Results"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "  Passed: $passed"
echo "  Failed: $failed"
if [ -n "$failed_tests" ]; then
    echo "  Failed tests:$failed_tests"
fi
echo ""

# Write results JSON (use jq for proper array formatting)
failed_tests_json="[]"
if [ -n "$failed_tests" ]; then
    # Convert space-separated list to JSON array
    failed_tests_json=$(echo "$failed_tests" | tr -s ' ' '\n' | sed '/^$/d' | jq -R . | jq -s .)
fi

jq -n \
    --argjson passed "$passed" \
    --argjson failed "$failed" \
    --argjson total "$((passed + failed))" \
    --argjson failed_tests "$failed_tests_json" \
    '{passed: $passed, failed: $failed, total: $total, failed_tests: $failed_tests}' \
    > "$RESULTS_DIR/cli-results.json"

if [ $failed -gt 0 ]; then
    log_fail "CLI tests failed"
    exit 1
fi

log_pass "All CLI tests passed"
exit 0
