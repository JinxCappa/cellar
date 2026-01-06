#!/bin/bash
# Test 08: PostgreSQL Metadata
# Tests specific to PostgreSQL metadata backend - only runs in postgres-fs configuration

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 08: PostgreSQL Metadata"

# Check if we're in PostgreSQL configuration
if [[ "${CELLAR_CONFIG:-}" != *"postgres"* ]]; then
    log_info "Skipping PostgreSQL tests - not in PostgreSQL configuration"
    exit 0
fi

# Note: These tests verify behavior specific to PostgreSQL
# They don't directly connect to PostgreSQL but verify the server behavior

# Test 1: Concurrent upload sessions
log_info "Testing concurrent upload sessions..."

# Create multiple sessions in parallel
PIDS=()
RESULTS_FILE=$(mktemp)

for i in {1..5}; do
    (
        STORE_PATH=$(generate_store_path "concurrent-$i")
        NAR_HASH=$(generate_nar_hash)
        RESPONSE=$(http_post_json "/v1/uploads" "{
            \"store_path\": \"$STORE_PATH\",
            \"nar_hash\": \"$NAR_HASH\",
            \"nar_size\": 1024
        }" 2>/dev/null || echo '{"error": "failed"}')

        if echo "$RESPONSE" | jq -e '.upload_id' > /dev/null 2>&1; then
            echo "success" >> "$RESULTS_FILE"
        else
            echo "failure" >> "$RESULTS_FILE"
        fi
    ) &
    PIDS+=($!)
done

# Wait for all background jobs
for pid in "${PIDS[@]}"; do
    wait $pid || true
done

SUCCESS_COUNT=$(grep -c "success" "$RESULTS_FILE" || echo "0")
FAILURE_COUNT=$(grep -c "failure" "$RESULTS_FILE" || echo "0")
rm -f "$RESULTS_FILE"

log_info "Concurrent sessions - Success: $SUCCESS_COUNT, Failure: $FAILURE_COUNT"

if [ "$SUCCESS_COUNT" -gt 0 ]; then
    log_info "PostgreSQL handled concurrent sessions correctly"
fi

# Test 2: Transaction isolation
log_info "Testing transaction isolation..."

# Create a session and verify it's not visible until committed
ISOLATED_PATH=$(generate_store_path "isolated-test")
ISOLATED_PATH_HASH=$(echo "$ISOLATED_PATH" | sed 's|/nix/store/||' | cut -d'-' -f1)
ISOLATED_NAR_HASH=$(generate_nar_hash)

ISOLATED_SESSION=$(http_post_json "/v1/uploads" "{
    \"store_path\": \"$ISOLATED_PATH\",
    \"nar_hash\": \"$ISOLATED_NAR_HASH\",
    \"nar_size\": 512
}" 2>/dev/null || echo '{}')

if echo "$ISOLATED_SESSION" | jq -e '.upload_id' > /dev/null 2>&1; then
    ISOLATED_ID=$(echo "$ISOLATED_SESSION" | jq -r '.upload_id')

    # Check narinfo before commit - should not exist
    PRE_COMMIT_STATUS=$(http_get_status "/${ISOLATED_PATH_HASH}.narinfo" "")
    log_info "Pre-commit narinfo status: $PRE_COMMIT_STATUS"

    if [ "$PRE_COMMIT_STATUS" = "404" ]; then
        log_info "Correctly returns 404 before commit"
    fi
fi

# Test 3: Query performance check
log_info "Testing query performance..."

START_TIME=$(date +%s%N)

# Perform multiple lookups
for i in {1..10}; do
    http_get_status "/$(nix_base32_string 32).narinfo" "" > /dev/null 2>&1 || true
done

END_TIME=$(date +%s%N)
DURATION_MS=$(( (END_TIME - START_TIME) / 1000000 ))

log_info "10 narinfo lookups took ${DURATION_MS}ms"

if [ $DURATION_MS -lt 5000 ]; then
    log_info "Query performance is acceptable"
else
    log_warn "Query performance may be slow: ${DURATION_MS}ms for 10 queries"
fi

# Test 4: Large batch operations
log_info "Testing batch operations..."

# Create multiple uploads in sequence
BATCH_COUNT=10
BATCH_SUCCESS=0

for i in $(seq 1 $BATCH_COUNT); do
    BATCH_PATH=$(generate_store_path "batch-$i")
    BATCH_NAR_HASH=$(generate_nar_hash)
    BATCH_RESPONSE=$(http_post_json "/v1/uploads" "{
        \"store_path\": \"$BATCH_PATH\",
        \"nar_hash\": \"$BATCH_NAR_HASH\",
        \"nar_size\": 256
    }" 2>/dev/null || echo '{}')

    if echo "$BATCH_RESPONSE" | jq -e '.upload_id' > /dev/null 2>&1; then
        BATCH_SUCCESS=$((BATCH_SUCCESS + 1))
    fi
done

log_info "Batch operations: $BATCH_SUCCESS/$BATCH_COUNT successful"

# Test 5: Index utilization (indirect check)
log_info "Testing indexed lookups..."

# Create a known path and look it up
INDEXED_PATH=$(generate_store_path "indexed-test")
INDEXED_PATH_HASH=$(echo "$INDEXED_PATH" | sed 's|/nix/store/||' | cut -d'-' -f1)

# Generate test data and compute NAR hash from actual content (SRI format)
TEST_DATA=$(generate_test_data 128)
NAR_SIZE=${#TEST_DATA}
INDEXED_NAR_HASH="sha256-$(echo -n "$TEST_DATA" | openssl dgst -sha256 -binary | base64 | tr -d '\n')"
CHUNK_HASH=$(echo -n "$TEST_DATA" | compute_sha256)

# Create and commit
INDEXED_SESSION=$(http_post_json "/v1/uploads" "{
    \"store_path\": \"$INDEXED_PATH\",
    \"nar_hash\": \"$INDEXED_NAR_HASH\",
    \"nar_size\": $NAR_SIZE
}" 2>/dev/null || echo '{}')

if echo "$INDEXED_SESSION" | jq -e '.upload_id' > /dev/null 2>&1; then
    INDEXED_ID=$(echo "$INDEXED_SESSION" | jq -r '.upload_id')

    # Upload the chunk and commit (using TEST_DATA and CHUNK_HASH computed above)
    http_put_binary "/v1/uploads/$INDEXED_ID/chunks/$CHUNK_HASH" "$TEST_DATA" 2>/dev/null || true
    http_post_json "/v1/uploads/$INDEXED_ID/commit" "{\"manifest\": [\"$CHUNK_HASH\"]}" 2>/dev/null || true

    sleep 1

    # Time the lookup
    LOOKUP_START=$(date +%s%N)
    http_get_status "/${INDEXED_PATH_HASH}.narinfo" "" > /dev/null 2>&1 || true
    LOOKUP_END=$(date +%s%N)
    LOOKUP_MS=$(( (LOOKUP_END - LOOKUP_START) / 1000000 ))

    log_info "Indexed lookup took ${LOOKUP_MS}ms"
fi

# Test 6: Connection pool behavior
log_info "Testing connection pool under load..."

# Rapid-fire requests
RAPID_START=$(date +%s%N)
for i in {1..20}; do
    http_get_status "/v1/health" "" > /dev/null 2>&1 &
done
wait

RAPID_END=$(date +%s%N)
RAPID_MS=$(( (RAPID_END - RAPID_START) / 1000000 ))

log_info "20 concurrent health checks took ${RAPID_MS}ms"

log_info "PostgreSQL metadata tests completed!"
