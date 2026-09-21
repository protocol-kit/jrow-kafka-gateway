#!/usr/bin/env bash
#
# End-to-end test for jrow-kafka-gateway.
#
# Brings up docker-compose.yml (a single-node Kafka broker + the gateway
# configured via config.e2e.toml), produces a uniquely-tagged JSON message
# onto the Kafka "orders" topic, and verifies it round-trips:
#
#   Kafka "orders" --(source)--> jrow "kafka.orders" --(sink)--> Kafka "roundtrip.kafka.orders"
#
# This exercises both bridge directions (Kafka -> jrow and jrow -> Kafka)
# plus the embedded jrow-server in a single pass, and cross-checks the
# gateway's /health stats endpoint.
#
# Usage:
#   ./e2e-test.sh            # build, run, test, tear down
#   KEEP=1 ./e2e-test.sh      # leave the docker-compose stack running after the test
#
# Requires: docker (with the `compose` plugin), curl, jq

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE=(docker compose)
BROKER_SERVICE="kafka"
GATEWAY_SERVICE="jrow-kafka-gateway"
SOURCE_TOPIC="orders"
SINK_TOPIC="roundtrip.kafka.orders"
HEALTH_URL="http://localhost:8090/health"
TEST_ID="e2e-$(date +%s)-$$"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

FAILED=0

pass() { echo -e "  ${GREEN}✓${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; FAILED=1; }
info() { echo -e "${YELLOW}→${NC} $1"; }
step() { echo ""; echo "=== $1 ==="; }

cleanup() {
    if [ "${KEEP:-0}" = "1" ]; then
        echo ""
        echo "KEEP=1 set - leaving the docker-compose stack running."
        echo "Tear down manually with: (cd \"$SCRIPT_DIR\" && ${COMPOSE[*]} down -v)"
    else
        step "Tearing down"
        "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

require() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "Missing required tool: $1" >&2
        exit 1
    }
}
require docker
require curl
require jq

echo "=========================================="
echo "  jrow-kafka-gateway E2E Test"
echo "=========================================="

step "Building and starting docker-compose stack"
if ! "${COMPOSE[@]}" up -d --build; then
    echo "Failed to start docker-compose stack" >&2
    exit 1
fi

step "Waiting for Kafka broker to become healthy"
KAFKA_READY=0
for _ in $(seq 1 60); do
    CID=$("${COMPOSE[@]}" ps -q "$BROKER_SERVICE" 2>/dev/null)
    if [ -n "$CID" ]; then
        STATUS=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$CID" 2>/dev/null || echo "unknown")
        if [ "$STATUS" = "healthy" ]; then
            KAFKA_READY=1
            break
        fi
    fi
    sleep 2
done

if [ "$KAFKA_READY" = "1" ]; then
    pass "Kafka broker is healthy"
else
    fail "Kafka broker did not become healthy in time"
    "${COMPOSE[@]}" logs "$BROKER_SERVICE" | tail -n 50
    exit 1
fi

step "Waiting for jrow-kafka-gateway health endpoint"
GATEWAY_READY=0
for _ in $(seq 1 60); do
    if curl -sf "$HEALTH_URL" >/dev/null 2>&1; then
        GATEWAY_READY=1
        break
    fi
    sleep 2
done

if [ "$GATEWAY_READY" = "1" ]; then
    pass "jrow-kafka-gateway health endpoint is up"
else
    fail "jrow-kafka-gateway health endpoint did not come up in time"
    "${COMPOSE[@]}" logs "$GATEWAY_SERVICE" | tail -n 80
    exit 1
fi

step "Ensuring Kafka topics exist"
"${COMPOSE[@]}" exec -T "$BROKER_SERVICE" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists --topic "$SOURCE_TOPIC" >/dev/null
"${COMPOSE[@]}" exec -T "$BROKER_SERVICE" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists --topic "$SINK_TOPIC" >/dev/null
pass "Kafka topics ready ($SOURCE_TOPIC, $SINK_TOPIC)"

step "Producing test message onto Kafka topic '$SOURCE_TOPIC'"
TEST_PAYLOAD=$(jq -nc --arg id "$TEST_ID" '{test_id: $id, message: "hello from e2e test"}')
if echo "$TEST_PAYLOAD" | "${COMPOSE[@]}" exec -T "$BROKER_SERVICE" /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic "$SOURCE_TOPIC" >/dev/null 2>&1; then
    pass "Produced test message (test_id=$TEST_ID)"
else
    fail "Failed to produce test message"
fi

step "Waiting for the message to round-trip back onto '$SINK_TOPIC'"
CONSUMED=""
for _ in $(seq 1 6); do
    CONSUMED=$("${COMPOSE[@]}" exec -T "$BROKER_SERVICE" /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 --topic "$SINK_TOPIC" --from-beginning \
        --max-messages 50 --timeout-ms 15000 2>/dev/null | grep -F "$TEST_ID" | head -n 1 || true)
    if [ -n "$CONSUMED" ]; then
        break
    fi
    sleep 2
done

if [ -z "$CONSUMED" ]; then
    fail "Did not observe round-tripped message on '$SINK_TOPIC' containing test_id=$TEST_ID"
    "${COMPOSE[@]}" logs "$GATEWAY_SERVICE" | tail -n 100
else
    pass "Observed round-tripped message on '$SINK_TOPIC'"

    ROUNDTRIP_TEST_ID=$(echo "$CONSUMED" | jq -r '.value.test_id // empty' 2>/dev/null)
    ROUNDTRIP_KAFKA_TOPIC=$(echo "$CONSUMED" | jq -r '.kafka_topic // empty' 2>/dev/null)

    if [ "$ROUNDTRIP_TEST_ID" = "$TEST_ID" ]; then
        pass "Round-tripped payload's value.test_id matches the original message"
    else
        fail "Round-tripped value.test_id mismatch (expected '$TEST_ID', got '$ROUNDTRIP_TEST_ID')"
    fi

    if [ "$ROUNDTRIP_KAFKA_TOPIC" = "$SOURCE_TOPIC" ]; then
        pass "Envelope's kafka_topic field correctly records the original topic ('$SOURCE_TOPIC')"
    else
        fail "Envelope's kafka_topic field mismatch (expected '$SOURCE_TOPIC', got '$ROUNDTRIP_KAFKA_TOPIC')"
    fi
fi

step "Checking gateway /health stats"
HEALTH_JSON=$(curl -sf "$HEALTH_URL" || echo "{}")
echo "$HEALTH_JSON" | jq . 2>/dev/null || echo "$HEALTH_JSON"

SOURCE_PUBLISHED=$(echo "$HEALTH_JSON" | jq -r '.source.messages_published // 0')
SINK_PRODUCED=$(echo "$HEALTH_JSON" | jq -r '.sink.messages_produced // 0')
SOURCE_ERRORS=$(echo "$HEALTH_JSON" | jq -r '.source.publish_errors // 0')
SINK_ERRORS=$(echo "$HEALTH_JSON" | jq -r '.sink.produce_errors // 0')

if [ "${SOURCE_PUBLISHED:-0}" -ge 1 ] 2>/dev/null; then
    pass "Source stats: messages_published=$SOURCE_PUBLISHED"
else
    fail "Source stats: expected messages_published >= 1, got '$SOURCE_PUBLISHED'"
fi

if [ "${SINK_PRODUCED:-0}" -ge 1 ] 2>/dev/null; then
    pass "Sink stats: messages_produced=$SINK_PRODUCED"
else
    fail "Sink stats: expected messages_produced >= 1, got '$SINK_PRODUCED'"
fi

if [ "${SOURCE_ERRORS:-0}" -eq 0 ] 2>/dev/null && [ "${SINK_ERRORS:-0}" -eq 0 ] 2>/dev/null; then
    pass "No publish/produce errors reported"
else
    fail "Errors reported: source.publish_errors=$SOURCE_ERRORS sink.produce_errors=$SINK_ERRORS"
fi

echo ""
echo "=========================================="
if [ "$FAILED" -eq 0 ]; then
    echo -e "  ${GREEN}E2E TEST PASSED${NC}"
    echo "=========================================="
    exit 0
else
    echo -e "  ${RED}E2E TEST FAILED${NC}"
    echo "=========================================="
    exit 1
fi
