# Multi-stage build for jrow-kafka-gateway

# Build stage
FROM rustlang/rust:nightly as builder

# rdkafka's `cmake-build` feature vendors and statically builds librdkafka,
# which requires a C toolchain, cmake, and libssl-dev (for SASL_SSL/SSL support).
RUN apt-get update && \
    apt-get install -y cmake build-essential pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# jrow-client/jrow-core/jrow-server are fetched straight from
# github.com/protocol-kit/jrow (see Cargo.toml), so only this crate's own
# sources are needed here - no sibling workspace to copy in.
COPY . .

RUN cargo build --release

# Runtime stage
# NOTE: must match the builder's glibc ABI. `rustlang/rust:nightly` is based on
# Debian trixie (13), so the runtime image needs to be trixie too (bookworm's
# older glibc is not forward-compatible with binaries built against trixie).
FROM debian:trixie-slim

# Install CA certificates for HTTPS/TLS Kafka connections, and curl for the
# container HEALTHCHECK below
RUN apt-get update && \
    apt-get install -y ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Copy the binary from builder
COPY --from=builder /app/target/release/jrow-kafka-gateway /usr/local/bin/

# Create a non-root user
RUN useradd -m -u 1000 jrow && \
    mkdir -p /data && \
    chown -R jrow:jrow /data

USER jrow
WORKDIR /home/jrow

# Assumes the default [health] config (enabled = true, port 8090); override
# or remove this if your config disables the health endpoint or changes its port.
HEALTHCHECK --interval=5s --timeout=3s --start-period=15s --retries=10 \
    CMD curl -f http://localhost:8090/health || exit 1

# Default command
ENTRYPOINT ["jrow-kafka-gateway"]
CMD ["--help"]
