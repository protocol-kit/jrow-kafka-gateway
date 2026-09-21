# jrow-kafka-gateway

A bidirectional gateway bridging [Kafka](https://kafka.apache.org/) topics and [jrow](../jrow) persistent pub/sub.

## Overview

`jrow-kafka-gateway` is a standalone Rust binary that bridges Kafka and jrow in both directions:

- **Kafka -> jrow** ("source" side) - Consumes configured Kafka topics and publishes them into an embedded, persistence-backed `jrow-server`.
- **jrow -> Kafka** ("sink" side) - Subscribes to a jrow persistent subscription (exact topic or wildcard pattern) and produces messages onto Kafka topics, using an exactly-once ack-after-write pattern.

Both directions are independently toggleable, so the binary can run as a Kafka-only source, a Kafka-only sink, or fully bidirectional in a single process.

## Architecture

```
┌──────────────┐          ┌───────────────────────────────────────────┐          ┌──────────────┐
│ Kafka topics │  consume │            jrow-kafka-gateway              │ produce  │ Kafka topics │
│    (in)      │─────────▶│                                             │─────────▶│    (out)     │
└──────────────┘          │  ┌────────────┐        ┌──────────────┐    │          └──────────────┘
                           │  │ KafkaSource│──────▶│ Embedded     │    │
                           │  │ (consumer) │publish│ JrowServer   │    │
                           │  └────────────┘ persist│ (sled)      │    │
                           │                        └──────┬───────┘    │
                           │                               │ ws         │
                           │                        ┌──────▼───────┐    │
                           │                        │ JrowClient   │    │
                           │                        │ (persistent  │    │
                           │                        │ subscription)│    │
                           │                        └──────┬───────┘    │
                           │                               │            │
                           │                        ┌──────▼───────┐    │
                           │                        │ KafkaSink    │    │
                           │                        │ (producer)   │    │
                           │                        └──────────────┘    │
                           └───────────────────────────────────────────┘
```

### Data Flow

**Kafka -> jrow**

1. `KafkaSource` consumes messages from the configured Kafka topics.
2. Each message is wrapped into a JSON envelope (`kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `key`, `value`) — `value` is parsed as JSON when possible, otherwise kept as a UTF-8 string.
3. The Kafka topic name is rendered through `source.jrow_topic_template` to produce the jrow topic.
4. The envelope is published via `JrowServer::publish_persistent`.
5. Only after a successful publish is the Kafka offset committed — a crash before that point results in redelivery, not data loss.

**jrow -> Kafka**

1. `KafkaSink` connects a `JrowClient` and calls `subscribe_persistent` on `sink.jrow_topic` (exact topic or `*`/`>` wildcard pattern).
2. Each delivered message's `topic` field is rendered through `sink.kafka_topic_template` to produce the Kafka topic; `data` is serialized as the Kafka payload.
3. The message is produced to Kafka via `rdkafka`'s `FutureProducer`.
4. Only after Kafka confirms the produce is the message acknowledged back to jrow (`client.ack_persistent`) — on failure, the message is not acknowledged and will be redelivered.

## Installation

### From Source

```bash
cd jrow-kafka-gateway
cargo build --release

# Binary will be at target/release/jrow-kafka-gateway
```

> **Note**: this crate depends on [`rdkafka`](https://github.com/fede1024/rust-rdkafka) with the `cmake-build` feature, which vendors and statically builds `librdkafka`. You'll need `cmake`, a C toolchain (`build-essential`/`gcc`, `make`), `pkg-config`, and `libssl-dev` installed to build from source.

### Using Docker

```bash
docker build -t jrow-kafka-gateway -f jrow-kafka-gateway/Dockerfile .
```

The `Dockerfile`'s builder stage installs `cmake`, `build-essential`, `pkg-config`, and `libssl-dev` before compiling.

## Configuration

Create a `config.toml` file (see [`config.example.toml`](config.example.toml)):

```toml
[jrow]
bind_address = "127.0.0.1:9950"
persistent_storage_path = "./data/kafka-gateway.db"
# client_url = "ws://127.0.0.1:9950"   # defaults to bind_address

[kafka]
bootstrap_servers = "localhost:9092"

[kafka.options]
# "security.protocol" = "SASL_SSL"

[source]
enabled = true
group_id = "jrow-kafka-gateway"
topics = ["orders", "payments"]
jrow_topic_template = "kafka.{topic}"
auto_offset_reset = "earliest"

[sink]
enabled = true
subscription_id = "kafka-gateway-sink"
jrow_topic = "events.>"
kafka_topic_template = "jrow.{topic}"

[health]
enabled = true
bind_address = "127.0.0.1:8090"
```

Set `source.enabled = false` or `sink.enabled = false` to run the gateway in a single direction only.

### Environment Variables

Both `[kafka.options]` values and any other string field support `${VAR_NAME}` substitution:

```bash
export KAFKA_USERNAME="my-user"
export KAFKA_PASSWORD="my-secret"
```

```toml
[kafka.options]
"sasl.username" = "${KAFKA_USERNAME}"
"sasl.password" = "${KAFKA_PASSWORD}"
```

### Splitting Source and Sink Across Instances

By default the sink's `JrowClient` connects to this same process's embedded `jrow-server` (`ws://{jrow.bind_address}`). Set `jrow.client_url` to point at a different jrow-server to run the Kafka->jrow and jrow->Kafka halves as separate gateway instances, each in its own container.

## Usage

```bash
# Run the gateway
./jrow-kafka-gateway --config config.toml

# With verbose logging
./jrow-kafka-gateway --config config.toml --verbose

# Test configuration and connectivity without running
./jrow-kafka-gateway --config config.toml --test
```

### Command-Line Options

```
Options:
  -c, --config <FILE>    Path to configuration file (TOML)
  -v, --verbose           Enable verbose logging (DEBUG level)
  -t, --trace             Enable trace logging (TRACE level)
  -T, --test              Test configuration and connectivity, then exit
  -h, --help               Print help
  -V, --version            Print version
```

`--test` builds the embedded jrow-server, checks Kafka broker connectivity for the source (via a metadata fetch), and — if the sink is enabled — connects a real `JrowClient` and Kafka producer to verify end-to-end connectivity (without registering the persistent subscription).

### Health Endpoint

When `[health].enabled = true`, `GET /health` returns combined stats for both directions:

```json
{
  "status": "healthy",
  "source": {
    "messages_consumed": 5420,
    "messages_published": 5420,
    "publish_errors": 0
  },
  "sink": {
    "messages_received": 1830,
    "messages_produced": 1830,
    "messages_acknowledged": 1830,
    "produce_errors": 0
  }
}
```

## Exactly-/At-Least-Once Delivery

- **Kafka -> jrow**: Kafka offsets are committed only after a successful `publish_persistent` call, so a crash between consuming and publishing results in redelivery from Kafka (at-least-once into jrow).
- **jrow -> Kafka**: jrow messages are acknowledged only after Kafka confirms the produce, so a crash between subscribing and producing results in redelivery from jrow's persistent storage (at-least-once into Kafka) — no ack until the produce succeeds.

Because both hops are at-least-once, downstream consumers should treat processing as idempotent.

## Development

### Running Unit Tests

```bash
cargo test
```

Unit tests cover configuration parsing/validation and topic template rendering.

### End-to-End Test (Docker Compose)

[`docker-compose.yml`](docker-compose.yml) brings up a single-node Kafka broker (KRaft mode, `apache/kafka`, no Zookeeper) plus the gateway itself, configured via [`config.e2e.toml`](config.e2e.toml). That config deliberately chains the sink's input pattern onto the source's output topic so a single gateway instance round-trips a message through both bridge directions:

```
Kafka "orders" --(source)--> jrow "kafka.orders" --(sink)--> Kafka "roundtrip.kafka.orders"
```

[`e2e-test.sh`](e2e-test.sh) automates this: it builds and starts the stack, waits for Kafka and the gateway's health endpoint to come up, produces a uniquely-tagged JSON message onto `orders`, waits for the round-tripped message to appear on `roundtrip.kafka.orders`, verifies the payload and metadata survived the round trip, and cross-checks the `/health` stats endpoint.

```bash
# Requires: docker (with the `compose` plugin), curl, jq
./e2e-test.sh

# Leave the stack running after the test (skip teardown) for manual inspection
KEEP=1 ./e2e-test.sh
```

On success you'll see a `E2E TEST PASSED` summary; on failure the script prints the relevant `docker compose logs` output and exits non-zero (useful in CI).

To interact with the stack manually:

```bash
docker compose up -d --build
docker compose logs -f jrow-kafka-gateway
curl -s http://localhost:8090/health | jq .
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose down -v
```

## License

This project is MIT-0 licensed for code and CC0-1.0 licensed for non-code content - See LICENSE file for details

## Related

- [jrow](../jrow) - JSON-RPC over WebSocket toolkit with persistent pub/sub
- [Apache Kafka](https://kafka.apache.org/)
- [rust-rdkafka](https://github.com/fede1024/rust-rdkafka)
