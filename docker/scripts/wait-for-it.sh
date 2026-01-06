#!/bin/bash
# wait-for-it.sh - Wait for a service to become available
# Usage: wait-for-it.sh host:port [-t timeout] [-- command args]

set -e

TIMEOUT=30
QUIET=0
HOST=""
PORT=""
COMMAND=""

usage() {
    echo "Usage: $0 host:port [-t timeout] [-q] [-- command args]"
    echo ""
    echo "  -t TIMEOUT    Timeout in seconds (default: 30)"
    echo "  -q            Quiet mode"
    echo "  -- COMMAND    Execute command after service is available"
    exit 1
}

wait_for_service() {
    local host="$1"
    local port="$2"
    local timeout="$3"

    local start_time=$(date +%s)
    local end_time=$((start_time + timeout))

    if [ $QUIET -eq 0 ]; then
        echo "Waiting for $host:$port..."
    fi

    while true; do
        if nc -z "$host" "$port" 2>/dev/null; then
            if [ $QUIET -eq 0 ]; then
                echo "$host:$port is available"
            fi
            return 0
        fi

        local current_time=$(date +%s)
        if [ $current_time -ge $end_time ]; then
            echo "Timeout waiting for $host:$port after ${timeout}s"
            return 1
        fi

        sleep 1
    done
}

wait_for_http() {
    local url="$1"
    local timeout="$2"

    local start_time=$(date +%s)
    local end_time=$((start_time + timeout))

    if [ $QUIET -eq 0 ]; then
        echo "Waiting for HTTP endpoint $url..."
    fi

    while true; do
        if curl -sf "$url" > /dev/null 2>&1; then
            if [ $QUIET -eq 0 ]; then
                echo "$url is available"
            fi
            return 0
        fi

        local current_time=$(date +%s)
        if [ $current_time -ge $end_time ]; then
            echo "Timeout waiting for $url after ${timeout}s"
            return 1
        fi

        sleep 1
    done
}

# Parse arguments
while [ $# -gt 0 ]; do
    case "$1" in
        -t)
            TIMEOUT="$2"
            shift 2
            ;;
        -q)
            QUIET=1
            shift
            ;;
        --)
            shift
            COMMAND="$*"
            break
            ;;
        *:*)
            HOST=$(echo "$1" | cut -d: -f1)
            PORT=$(echo "$1" | cut -d: -f2)
            shift
            ;;
        http://*)
            # HTTP URL mode
            if wait_for_http "$1" "$TIMEOUT"; then
                if [ -n "$COMMAND" ]; then
                    exec $COMMAND
                fi
                exit 0
            fi
            exit 1
            ;;
        *)
            usage
            ;;
    esac
done

if [ -z "$HOST" ] || [ -z "$PORT" ]; then
    usage
fi

if wait_for_service "$HOST" "$PORT" "$TIMEOUT"; then
    if [ -n "$COMMAND" ]; then
        exec $COMMAND
    fi
    exit 0
fi

exit 1
