#!/bin/bash
# Test orchestrator entrypoint
# Coordinates running all test scenarios and collecting results

set -euo pipefail

# Source test utilities
source /usr/local/lib/test-utils.sh

# Configuration
CELLAR_SERVER_URL="${CELLAR_SERVER_URL:-http://server:8080}"
CELLAR_CONFIG="${CELLAR_CONFIG:-unknown}"
TEST_SCENARIOS="${TEST_SCENARIOS:-all}"
RESULTS_DIR="${RESULTS_DIR:-/results}"
TESTS_DIR="/tests/scenarios"

# Test state
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0
declare -a FAILED_TESTS=()

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_header() {
    echo -e "\n${BLUE}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}\n"
}

log_test_start() {
    echo -e "${YELLOW}▶ Running: $1${NC}"
}

log_test_pass() {
    echo -e "${GREEN}✓ PASSED: $1${NC}"
}

log_test_fail() {
    echo -e "${RED}✗ FAILED: $1${NC}"
}

log_test_skip() {
    echo -e "${YELLOW}○ SKIPPED: $1${NC}"
}

# Wait for server to be ready
wait_for_server() {
    log_header "Waiting for Cellar server"

    local max_attempts=60
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if curl -sf "${CELLAR_SERVER_URL}/v1/health" > /dev/null 2>&1; then
            echo -e "${GREEN}Server is ready!${NC}"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: Server not ready, waiting..."
        sleep 2
        attempt=$((attempt + 1))
    done

    echo -e "${RED}Server failed to become ready${NC}"
    return 1
}

# Initialize test environment
initialize() {
    log_header "Initializing Test Environment"

    echo "Configuration: $CELLAR_CONFIG"
    echo "Server URL: $CELLAR_SERVER_URL"
    echo "Test scenarios: $TEST_SCENARIOS"
    echo "Results directory: $RESULTS_DIR"

    # Create results directory
    mkdir -p "$RESULTS_DIR"

    # Initialize results file
    cat > "$RESULTS_DIR/results.json" <<EOF
{
    "config": "$CELLAR_CONFIG",
    "server_url": "$CELLAR_SERVER_URL",
    "started_at": "$(date -Iseconds)",
    "tests": []
}
EOF

    # Create admin token for testing
    echo "Creating test token..."
    export CELLAR_TEST_TOKEN=$(create_test_token)
    echo "Test token created: ${CELLAR_TEST_TOKEN:0:20}..."
}

# Get the pre-configured test token
create_test_token() {
    # Use the admin token configured in server.toml
    # Token value matches the hash in [admin].token_hash
    echo "cellar-test-token-12345"
}

# Run a single test scenario
run_test() {
    local test_script="$1"
    local test_name=$(basename "$test_script" .sh)
    local test_log="$RESULTS_DIR/${test_name}.log"
    local start_time=$(date +%s)

    log_test_start "$test_name"

    # Check if test should be skipped for this config
    if should_skip_test "$test_name"; then
        log_test_skip "$test_name"
        TESTS_SKIPPED=$((TESTS_SKIPPED + 1))
        record_test_result "$test_name" "skipped" 0 ""
        return 0
    fi

    # Run the test
    local exit_code=0
    if bash "$test_script" > "$test_log" 2>&1; then
        exit_code=0
    else
        exit_code=$?
    fi

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    if [ $exit_code -eq 0 ]; then
        log_test_pass "$test_name"
        TESTS_PASSED=$((TESTS_PASSED + 1))
        record_test_result "$test_name" "passed" $duration ""
    else
        log_test_fail "$test_name"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        FAILED_TESTS+=("$test_name")
        record_test_result "$test_name" "failed" $duration "$(tail -20 "$test_log")"

        # Show last few lines of log for failed tests
        echo "--- Last 10 lines of log ---"
        tail -10 "$test_log"
        echo "--- End of log ---"
    fi
}

# Check if a test should be skipped for current config
should_skip_test() {
    local test_name="$1"

    # Skip S3-specific tests when not using S3
    if [[ "$test_name" == *"s3"* ]] && [[ "$CELLAR_CONFIG" != *"s3"* ]]; then
        return 0
    fi

    # Skip Postgres-specific tests when not using Postgres
    if [[ "$test_name" == *"postgres"* ]] && [[ "$CELLAR_CONFIG" != *"postgres"* ]]; then
        return 0
    fi

    return 1
}

# Record test result to JSON
record_test_result() {
    local name="$1"
    local status="$2"
    local duration="$3"
    local error="$4"

    local temp_file=$(mktemp)
    jq --arg name "$name" \
       --arg status "$status" \
       --arg duration "$duration" \
       --arg error "$error" \
       '.tests += [{
           "name": $name,
           "status": $status,
           "duration_secs": ($duration | tonumber),
           "error": (if $error == "" then null else $error end)
       }]' "$RESULTS_DIR/results.json" > "$temp_file"
    mv "$temp_file" "$RESULTS_DIR/results.json"
}

# Get list of test scenarios to run
get_test_scenarios() {
    if [ "$TEST_SCENARIOS" = "all" ]; then
        find "$TESTS_DIR" -name "*.sh" -type f | sort
    else
        # Run specific scenarios
        for scenario in ${TEST_SCENARIOS//,/ }; do
            local script="$TESTS_DIR/${scenario}.sh"
            if [ -f "$script" ]; then
                echo "$script"
            else
                echo "Warning: Test scenario not found: $scenario" >&2
            fi
        done
    fi
}

# Print final summary
print_summary() {
    log_header "Test Summary"

    local total=$((TESTS_PASSED + TESTS_FAILED + TESTS_SKIPPED))

    echo "Configuration: $CELLAR_CONFIG"
    echo "Total tests: $total"
    echo -e "  ${GREEN}Passed: $TESTS_PASSED${NC}"
    echo -e "  ${RED}Failed: $TESTS_FAILED${NC}"
    echo -e "  ${YELLOW}Skipped: $TESTS_SKIPPED${NC}"

    if [ ${#FAILED_TESTS[@]} -gt 0 ]; then
        echo -e "\n${RED}Failed tests:${NC}"
        for test in "${FAILED_TESTS[@]}"; do
            echo "  - $test"
        done
    fi

    # Update results file
    local temp_file=$(mktemp)
    jq --arg passed "$TESTS_PASSED" \
       --arg failed "$TESTS_FAILED" \
       --arg skipped "$TESTS_SKIPPED" \
       '. + {
           "finished_at": (now | strftime("%Y-%m-%dT%H:%M:%S%z")),
           "summary": {
               "passed": ($passed | tonumber),
               "failed": ($failed | tonumber),
               "skipped": ($skipped | tonumber),
               "total": (($passed | tonumber) + ($failed | tonumber) + ($skipped | tonumber))
           }
       }' "$RESULTS_DIR/results.json" > "$temp_file"
    mv "$temp_file" "$RESULTS_DIR/results.json"

    echo -e "\nResults saved to: $RESULTS_DIR/results.json"
}

# Main execution
main() {
    log_header "Cellar Integration Test Suite"
    echo "Starting test run at $(date)"

    # Wait for server
    if ! wait_for_server; then
        echo "ERROR: Server not available"
        exit 1
    fi

    # Initialize
    initialize

    # Run tests
    log_header "Running Test Scenarios"

    while IFS= read -r test_script; do
        if [ -n "$test_script" ]; then
            run_test "$test_script"
        fi
    done < <(get_test_scenarios)

    # Print summary
    print_summary

    # Exit with failure if any tests failed
    if [ $TESTS_FAILED -gt 0 ]; then
        exit 1
    fi

    exit 0
}

# Run main
main "$@"
