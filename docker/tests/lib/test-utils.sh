#!/bin/bash
# Test utilities library for Cellar integration tests
# Source this file in test scenarios: source /usr/local/lib/test-utils.sh

set -euo pipefail

# Configuration from environment
CELLAR_SERVER_URL="${CELLAR_SERVER_URL:-http://server:8080}"
CELLAR_TEST_TOKEN="${CELLAR_TEST_TOKEN:-}"

# Assertion functions

# Assert that a condition is true
assert_true() {
    local condition="$1"
    local message="${2:-Assertion failed}"

    if ! eval "$condition"; then
        echo "ASSERTION FAILED: $message"
        echo "  Condition: $condition"
        return 1
    fi
}

# Assert that two values are equal
assert_eq() {
    local expected="$1"
    local actual="$2"
    local message="${3:-Values not equal}"

    if [ "$expected" != "$actual" ]; then
        echo "ASSERTION FAILED: $message"
        echo "  Expected: $expected"
        echo "  Actual: $actual"
        return 1
    fi
}

# Assert that a value is not empty
assert_not_empty() {
    local value="$1"
    local message="${2:-Value is empty}"

    if [ -z "$value" ]; then
        echo "ASSERTION FAILED: $message"
        return 1
    fi
}

# Assert that a command succeeds
assert_success() {
    local message="${1:-Command failed}"
    shift

    if ! "$@"; then
        echo "ASSERTION FAILED: $message"
        echo "  Command: $*"
        return 1
    fi
}

# Assert that a command fails
assert_failure() {
    local message="${1:-Command succeeded unexpectedly}"
    shift

    if "$@"; then
        echo "ASSERTION FAILED: $message"
        echo "  Command: $*"
        return 1
    fi
}

# Assert HTTP response status code
assert_http_status() {
    local expected="$1"
    local actual="$2"
    local message="${3:-HTTP status mismatch}"

    if [ "$expected" != "$actual" ]; then
        echo "ASSERTION FAILED: $message"
        echo "  Expected status: $expected"
        echo "  Actual status: $actual"
        return 1
    fi
}

# Assert JSON field exists and has expected value
assert_json_field() {
    local json="$1"
    local field="$2"
    local expected="$3"
    local message="${4:-JSON field mismatch}"

    local actual=$(echo "$json" | jq -r "$field")

    if [ "$expected" != "$actual" ]; then
        echo "ASSERTION FAILED: $message"
        echo "  Field: $field"
        echo "  Expected: $expected"
        echo "  Actual: $actual"
        return 1
    fi
}

# HTTP helper functions

# Make a GET request
http_get() {
    local path="$1"
    local token="${2:-$CELLAR_TEST_TOKEN}"

    local headers=()
    if [ -n "$token" ]; then
        headers+=(-H "Authorization: Bearer $token")
    fi

    curl -sf "${headers[@]}" "${CELLAR_SERVER_URL}${path}"
}

# Make a GET request and return status code
http_get_status() {
    local path="$1"
    local token="${2:-$CELLAR_TEST_TOKEN}"

    local headers=()
    if [ -n "$token" ]; then
        headers+=(-H "Authorization: Bearer $token")
    fi

    curl -s -o /dev/null -w "%{http_code}" "${headers[@]}" "${CELLAR_SERVER_URL}${path}"
}

# Make a POST request with JSON body
http_post_json() {
    local path="$1"
    local body="$2"
    local token="${3:-$CELLAR_TEST_TOKEN}"

    local headers=(-H "Content-Type: application/json")
    if [ -n "$token" ]; then
        headers+=(-H "Authorization: Bearer $token")
    fi

    curl -sf "${headers[@]}" -X POST -d "$body" "${CELLAR_SERVER_URL}${path}"
}

# Make a POST request and return status code
http_post_status() {
    local path="$1"
    local body="$2"
    local token="${3:-$CELLAR_TEST_TOKEN}"

    local headers=(-H "Content-Type: application/json")
    if [ -n "$token" ]; then
        headers+=(-H "Authorization: Bearer $token")
    fi

    curl -s -o /dev/null -w "%{http_code}" "${headers[@]}" -X POST -d "$body" "${CELLAR_SERVER_URL}${path}"
}

# Make a PUT request with binary data
http_put_binary() {
    local path="$1"
    local data="$2"
    local token="${3:-$CELLAR_TEST_TOKEN}"

    local headers=(-H "Content-Type: application/octet-stream")
    if [ -n "$token" ]; then
        headers+=(-H "Authorization: Bearer $token")
    fi

    echo -n "$data" | curl -sf "${headers[@]}" -X PUT --data-binary @- "${CELLAR_SERVER_URL}${path}"
}

# Make a DELETE request
http_delete() {
    local path="$1"
    local token="${2:-$CELLAR_TEST_TOKEN}"

    local headers=()
    if [ -n "$token" ]; then
        headers+=(-H "Authorization: Bearer $token")
    fi

    curl -sf "${headers[@]}" -X DELETE "${CELLAR_SERVER_URL}${path}"
}

# Test data generation

# Generate random string
random_string() {
    local length="${1:-16}"
    openssl rand -hex "$length" | head -c "$length"
}

# Generate a fake store path
generate_store_path() {
    local name="${1:-test-package}"
    local hash=$(random_string 32)
    echo "/nix/store/${hash}-${name}"
}

# Generate a fake NAR hash
generate_nar_hash() {
    echo "sha256:$(random_string 64)"
}

# Generate test binary data
generate_binary_data() {
    local size="${1:-1024}"
    openssl rand "$size"
}

# Logging helpers

log_info() {
    echo "[INFO] $*"
}

log_warn() {
    echo "[WARN] $*" >&2
}

log_error() {
    echo "[ERROR] $*" >&2
}

log_debug() {
    if [ "${DEBUG:-}" = "1" ]; then
        echo "[DEBUG] $*"
    fi
}

# Test lifecycle helpers

# Setup function to run before each test
test_setup() {
    log_info "Setting up test environment..."
    # Create temp directory for test artifacts
    TEST_TEMP_DIR=$(mktemp -d)
    export TEST_TEMP_DIR
}

# Teardown function to run after each test
test_teardown() {
    log_info "Cleaning up test environment..."
    if [ -n "${TEST_TEMP_DIR:-}" ] && [ -d "$TEST_TEMP_DIR" ]; then
        rm -rf "$TEST_TEMP_DIR"
    fi
}

# Run a test function with setup/teardown
run_test() {
    local test_func="$1"
    local test_name="${2:-$test_func}"

    log_info "Running test: $test_name"

    test_setup

    local result=0
    if ! "$test_func"; then
        result=1
    fi

    test_teardown

    return $result
}

# Wait for condition with timeout
wait_for() {
    local condition="$1"
    local timeout="${2:-30}"
    local interval="${3:-1}"

    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        if eval "$condition"; then
            return 0
        fi
        sleep $interval
        elapsed=$((elapsed + interval))
    done

    return 1
}

# Retry a command with backoff
retry() {
    local max_attempts="${1:-3}"
    local delay="${2:-1}"
    shift 2

    local attempt=1
    while [ $attempt -le $max_attempts ]; do
        if "$@"; then
            return 0
        fi

        if [ $attempt -lt $max_attempts ]; then
            log_info "Attempt $attempt failed, retrying in ${delay}s..."
            sleep $delay
            delay=$((delay * 2))
        fi

        attempt=$((attempt + 1))
    done

    return 1
}

# Export functions for use in subshells
export -f assert_true assert_eq assert_not_empty assert_success assert_failure
export -f assert_http_status assert_json_field
export -f http_get http_get_status http_post_json http_post_status http_put_binary http_delete
export -f random_string generate_store_path generate_nar_hash generate_binary_data
export -f log_info log_warn log_error log_debug
export -f test_setup test_teardown run_test wait_for retry
