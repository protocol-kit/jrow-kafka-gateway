//! jrow -> Kafka bridge: consumes a jrow persistent subscription and
//! produces messages onto Kafka topics.

use crate::config::{KafkaConfig, SinkConfig};
use crate::error::{GatewayError, Result};
use crate::topic_map::TopicTemplate;
use jrow_client::JrowClient;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const PRODUCE_TIMEOUT: Duration = Duration::from_secs(10);

/// Snapshot of jrow -> Kafka bridge statistics
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct SinkStats {
    pub messages_received: u64,
    pub messages_produced: u64,
    pub messages_acknowledged: u64,
    pub produce_errors: u64,
}

/// Bridges a jrow persistent subscription into Kafka topics
pub struct KafkaSink {
    client: JrowClient,
    producer: Arc<FutureProducer>,
    subscription_id: String,
    jrow_topic: String,
    kafka_topic_template: TopicTemplate,
    stats: Arc<SinkStatsInner>,
}

#[derive(Default)]
struct SinkStatsInner {
    messages_received: AtomicU64,
    messages_produced: AtomicU64,
    messages_acknowledged: AtomicU64,
    produce_errors: AtomicU64,
}

impl KafkaSink {
    /// Connect a jrow client and Kafka producer, ready to `run()`
    pub async fn new(jrow_client_url: &str, kafka: &KafkaConfig, sink: &SinkConfig) -> Result<Self> {
        let kafka_topic_template = TopicTemplate::new(sink.kafka_topic_template.clone())?;

        let client = JrowClient::connect(jrow_client_url)
            .await
            .map_err(|e| GatewayError::Jrow(e.to_string()))?;

        tracing::info!(url = %jrow_client_url, "Kafka sink connected to jrow");

        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", &kafka.bootstrap_servers);
        for (key, value) in &kafka.options {
            client_config.set(key, value);
        }
        let producer: Arc<FutureProducer> = Arc::new(client_config.create()?);

        Ok(Self {
            client,
            producer,
            subscription_id: sink.subscription_id.clone(),
            jrow_topic: sink.jrow_topic.clone(),
            kafka_topic_template,
            stats: Arc::new(SinkStatsInner::default()),
        })
    }

    /// Start the persistent subscription and bridge messages to Kafka.
    ///
    /// `subscribe_persistent` registers the notification handler and returns
    /// once the subscription is acknowledged by the server; delivery then
    /// continues in the background for the lifetime of the connection.
    pub async fn run(&self) -> Result<()> {
        let producer = Arc::clone(&self.producer);
        let kafka_topic_template = self.kafka_topic_template.clone();
        let stats = Arc::clone(&self.stats);
        let subscription_id = self.subscription_id.clone();
        let fallback_topic = self.jrow_topic.clone();
        let client = self.client.clone();

        let resumed_seq = self
            .client
            .subscribe_persistent(subscription_id.clone(), self.jrow_topic.clone(), move |msg| {
                let producer = Arc::clone(&producer);
                let kafka_topic_template = kafka_topic_template.clone();
                let stats = Arc::clone(&stats);
                let subscription_id = subscription_id.clone();
                let fallback_topic = fallback_topic.clone();
                let client = client.clone();

                async move {
                    stats.messages_received.fetch_add(1, Ordering::Relaxed);

                    let sequence_id = msg.get("sequence_id").and_then(|v| v.as_u64()).unwrap_or(0);
                    let jrow_topic = msg
                        .get("topic")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .unwrap_or(fallback_topic);
                    let data = msg.get("data").cloned().unwrap_or(serde_json::Value::Null);

                    let kafka_topic = kafka_topic_template.render(&jrow_topic);

                    match serde_json::to_vec(&data) {
                        Ok(payload) => {
                            let record: FutureRecord<'_, str, [u8]> =
                                FutureRecord::to(&kafka_topic).payload(&payload);

                            match producer.send(record, Timeout::After(PRODUCE_TIMEOUT)).await {
                                Ok(_) => {
                                    stats.messages_produced.fetch_add(1, Ordering::Relaxed);
                                    // Only acknowledge back to jrow once Kafka has
                                    // confirmed the produce, guaranteeing at-least-once
                                    // delivery into Kafka.
                                    client.ack_persistent(&subscription_id, sequence_id);
                                    stats.messages_acknowledged.fetch_add(1, Ordering::Relaxed);
                                }
                                Err((e, _)) => {
                                    stats.produce_errors.fetch_add(1, Ordering::Relaxed);
                                    tracing::error!(
                                        error = %e,
                                        kafka_topic = %kafka_topic,
                                        jrow_topic = %jrow_topic,
                                        sequence_id = sequence_id,
                                        "Failed to produce message to Kafka; message will be redelivered"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            stats.produce_errors.fetch_add(1, Ordering::Relaxed);
                            tracing::error!(
                                error = %e,
                                sequence_id = sequence_id,
                                "Failed to serialize jrow message for Kafka"
                            );
                        }
                    }
                }
            })
            .await
            .map_err(|e| GatewayError::Jrow(e.to_string()))?;

        tracing::info!(
            subscription_id = %self.subscription_id,
            jrow_topic = %self.jrow_topic,
            resumed_from_seq = resumed_seq,
            "Kafka sink subscribed"
        );

        Ok(())
    }

    /// Current statistics snapshot
    pub fn stats(&self) -> SinkStats {
        SinkStats {
            messages_received: self.stats.messages_received.load(Ordering::Relaxed),
            messages_produced: self.stats.messages_produced.load(Ordering::Relaxed),
            messages_acknowledged: self.stats.messages_acknowledged.load(Ordering::Relaxed),
            produce_errors: self.stats.produce_errors.load(Ordering::Relaxed),
        }
    }

    /// Verify Kafka broker connectivity by fetching cluster metadata
    pub fn test_connectivity(&self) -> Result<()> {
        self.producer
            .client()
            .fetch_metadata(None, std::time::Duration::from_secs(5))?;
        Ok(())
    }

    /// Flush any buffered Kafka produce requests (best-effort, blocking)
    pub async fn flush(&self) {
        let producer = Arc::clone(&self.producer);
        let result = tokio::task::spawn_blocking(move || {
            producer.flush(Timeout::After(Duration::from_secs(10)))
        })
        .await;

        match result {
            Ok(Ok(())) => tracing::info!("Kafka sink producer flushed"),
            Ok(Err(e)) => tracing::warn!(error = %e, "Failed to flush Kafka sink producer"),
            Err(e) => tracing::warn!(error = %e, "Flush task panicked"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_default() {
        let stats = SinkStats::default();
        assert_eq!(stats.messages_received, 0);
        assert_eq!(stats.produce_errors, 0);
    }
}
