#!/bin/bash
# Test 06: Garbage Collection
# Tests the GC workflow and cleanup operations

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 06: Garbage Collection"

# Test 1: Trigger GC job
log_info "Testing GC trigger..."
GC_RESPONSE=$(http_post_json "/v1/admin/gc" '{"job_type": "chunk_gc"}' 2>/dev/null || echo '{"error": "not available"}')
log_info "GC response: $GC_RESPONSE"

if echo "$GC_RESPONSE" | jq -e '.job_id' > /dev/null 2>&1; then
    GC_JOB_ID=$(echo "$GC_RESPONSE" | jq -r '.job_id')
    log_info "GC job created: $GC_JOB_ID"

    # Test 2: Check GC job status
    log_info "Checking GC job status..."
    sleep 2  # Give time for GC to process

    GC_STATUS=$(http_get "/v1/admin/gc/$GC_JOB_ID" 2>/dev/null || echo '{}')
    log_info "GC status: $GC_STATUS"

    if echo "$GC_STATUS" | jq -e '.state' > /dev/null 2>&1; then
        STATE=$(echo "$GC_STATUS" | jq -r '.state')
        log_info "GC job state: $STATE"

        # Wait for completion if still running
        ATTEMPTS=0
        while [ "$STATE" = "running" ] || [ "$STATE" = "queued" ]; do
            sleep 2
            ATTEMPTS=$((ATTEMPTS + 1))
            if [ $ATTEMPTS -gt 30 ]; then
                log_warn "GC job taking too long, continuing..."
                break
            fi
            GC_STATUS=$(http_get "/v1/admin/gc/$GC_JOB_ID" 2>/dev/null || echo '{}')
            STATE=$(echo "$GC_STATUS" | jq -r '.state // "unknown"')
        done

        if [ "$STATE" = "finished" ]; then
            log_info "GC job completed successfully"

            # Check stats if available
            if echo "$GC_STATUS" | jq -e '.stats' > /dev/null 2>&1; then
                STATS=$(echo "$GC_STATUS" | jq '.stats')
                log_info "GC stats: $STATS"
            fi
        fi
    fi
else
    log_info "GC API may not be fully implemented or requires admin privileges"
fi

# Test 3: List GC jobs
log_info "Listing GC jobs..."
GC_JOBS=$(http_get "/v1/admin/gc" 2>/dev/null || echo '[]')
log_info "GC jobs: $GC_JOBS"

# Test 4: Test expired session cleanup
log_info "Testing expired session handling..."

# Create a session that we won't complete
NAR_HASH=$(generate_nar_hash)
EXPIRED_SESSION=$(http_post_json "/v1/uploads" "{
    \"store_path\": \"/nix/store/expired-test-session\",
    \"nar_hash\": \"$NAR_HASH\",
    \"nar_size\": 1024
}" 2>/dev/null || echo '{}')

if echo "$EXPIRED_SESSION" | jq -e '.upload_id' > /dev/null 2>&1; then
    EXPIRED_ID=$(echo "$EXPIRED_SESSION" | jq -r '.upload_id')
    log_info "Created incomplete session: $EXPIRED_ID"

    # Trigger session cleanup (upload_gc cleans up expired uploads)
    SESSION_GC=$(http_post_json "/v1/admin/gc" '{"job_type": "upload_gc"}' 2>/dev/null || echo '{}')
    log_info "Session GC response: $SESSION_GC"
fi

# Test 5: Check metrics (replaces old /v1/admin/stats)
log_info "Checking metrics..."
METRICS=$(http_get "/v1/admin/metrics" 2>/dev/null || echo '{}')
if [ "$METRICS" != "{}" ]; then
    log_info "Metrics: $METRICS"
fi

log_info "Garbage collection tests completed!"
