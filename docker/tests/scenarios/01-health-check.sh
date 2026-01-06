#!/bin/bash
# Test 01: Health Check
# Verifies that the server is running and responding to health checks

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 01: Health Check"

# Test 1: Basic health endpoint
log_info "Testing /v1/health endpoint..."
HEALTH_RESPONSE=$(http_get "/v1/health" "")
assert_not_empty "$HEALTH_RESPONSE" "Health response should not be empty"
log_info "Health response: $HEALTH_RESPONSE"

# Test 2: Health endpoint returns 200
log_info "Testing /v1/health returns 200..."
HEALTH_STATUS=$(http_get_status "/v1/health" "")
assert_http_status "200" "$HEALTH_STATUS" "Health endpoint should return 200"

# Test 3: Capabilities endpoint (if available)
log_info "Testing /v1/capabilities endpoint..."
CAPABILITIES_STATUS=$(http_get_status "/v1/capabilities" "")
if [ "$CAPABILITIES_STATUS" = "200" ]; then
    CAPABILITIES=$(http_get "/v1/capabilities" "")
    log_info "Capabilities: $CAPABILITIES"
else
    log_info "Capabilities endpoint returned $CAPABILITIES_STATUS (may not be implemented)"
fi

# Test 4: Nix cache info endpoint
log_info "Testing /nix-cache-info endpoint..."
CACHE_INFO_STATUS=$(http_get_status "/nix-cache-info" "")
if [ "$CACHE_INFO_STATUS" = "200" ]; then
    CACHE_INFO=$(http_get "/nix-cache-info" "")
    log_info "Cache info: $CACHE_INFO"

    # Verify required fields
    assert_true "echo '$CACHE_INFO' | grep -q 'StoreDir:'" "Cache info should contain StoreDir"
else
    log_info "Nix cache info endpoint returned $CACHE_INFO_STATUS"
fi

# Test 5: Metrics endpoint (if enabled)
log_info "Testing /metrics endpoint..."
METRICS_STATUS=$(http_get_status "/metrics" "")
if [ "$METRICS_STATUS" = "200" ]; then
    METRICS=$(http_get "/metrics" "")
    # Check for Prometheus format
    if echo "$METRICS" | grep -q "^# HELP\|^# TYPE"; then
        log_info "Metrics endpoint returns Prometheus format"
    fi
else
    log_info "Metrics endpoint returned $METRICS_STATUS (may not be enabled)"
fi

log_info "Health check tests passed!"
