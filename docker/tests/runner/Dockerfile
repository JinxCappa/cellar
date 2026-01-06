# Test orchestrator Dockerfile
# Runs integration tests against the Cellar server

FROM alpine:3.20

# Install dependencies
RUN apk add --no-cache \
    bash \
    curl \
    jq \
    openssl \
    coreutils \
    grep \
    sed \
    aws-cli \
    && rm -rf /var/cache/apk/*

# Create test user
RUN adduser -D -u 1000 tester

# Create directories
RUN mkdir -p /tests /results /workspace \
    && chown -R tester:tester /tests /results /workspace

# Copy test utilities
COPY docker/tests/lib/test-utils.sh /usr/local/lib/
COPY docker/scripts/wait-for-it.sh /usr/local/bin/
COPY docker/tests/runner/entrypoint.sh /usr/local/bin/

RUN chmod +x /usr/local/bin/wait-for-it.sh /usr/local/bin/entrypoint.sh

# Copy test scenarios
COPY docker/tests/scenarios/ /tests/scenarios/

RUN chmod +x /tests/scenarios/*.sh 2>/dev/null || true

USER tester
WORKDIR /workspace

# Environment variables
ENV CELLAR_SERVER_URL=http://server:8080
ENV CELLAR_CONFIG=unknown
ENV TEST_SCENARIOS=all
ENV RESULTS_DIR=/results

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
