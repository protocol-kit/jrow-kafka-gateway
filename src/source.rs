//! Kafka -> jrow bridge: consumes Kafka topics and publishes into jrow's
//! embedded, persistent-storage-backed `JrowServer`.

use crate::config::{KafkaConfig, SourceConfig};
use crate::error::{GatewayError, Result};
use crate::topic_map::TopicTemplate;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;

/// Snapshot of Kafka -> jrow bridge statistics
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct SourceStats {
    pub messages_consumed: u64,
    pub messages_published: u64,
    pub publish_errors: u64,
}

/// Bridges Kafka topics into jrow persistent topics
pub struct KafkaSource {
    consumer: StreamConsumer,
    server: Arc<jrow_server::JrowServer>,
    jrow_topic_template: TopicTemplate,
    messages_consumed: Arc<AtomicU64>,
    messages_published: Arc<AtomicU64>,
    publish_errors: Arc<AtomicU64>,
}

impl KafkaSource {
    /// Build a new Kafka source from configuration and an already-running
    /// embedded jrow server
    pub fn new(
        kafka: &KafkaConfig,
        source: &SourceConfig,
        server: Arc<jrow_server::JrowServer>,
    ) -> Result<Self> {
        let jrow_topic_template = TopicTemplate::new(source.jrow_topic_template.clone())?;

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &kafka.bootstrap_servers)
            .set("group.id", &source.group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", &source.auto_offset_reset);

        for (key, value) in &kafka.options {
            client_config.set(key, value);
        }

        let consumer: StreamConsumer = client_config.create()?;

        let topics: Vec<&str> = source.topics.iter().map(String::as_str).collect();
        consumer.subscribe(&topics)?;

        tracing::info!(
            topics = ?source.topics,
            group_id = %source.group_id,
            jrow_topic_template = %jrow_topic_template,
            "Kafka source subscribed"
        );

        Ok(Self {
            consumer,
            server,
            jrow_topic_template,
            messages_consumed: Arc::new(AtomicU64::new(0)),
            messages_published: Arc::new(AtomicU64::new(0)),
            publish_errors: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Run the consume -> publish loop until a shutdown signal is received
    pub async fn run(&self, mut shutdown: broadcast::Receiver<()>) -> Result<()> {
        loop {
            tokio::select! {
                result = self.consumer.recv() => {
                    match result {
                        Ok(msg) => {
                            self.messages_consumed.fetch_add(1, Ordering::Relaxed);
                            if let Err(e) = self.handle_message(&msg).await {
                                self.publish_errors.fetch_add(1, Ordering::Relaxed);
                                tracing::error!(
                                    error = %e,
                                    kafka_topic = msg.topic(),
                                    partition = msg.partition(),
                                    offset = msg.offset(),
                                    "Failed to publish Kafka message to jrow; offset will not be committed"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "Kafka consumer receive error");
                        }
                    }
                }
                _ = shutdown.recv() => {
                    tracing::info!("Kafka source shutting down");
                    break;
                }
            }
        }
        Ok(())
    }

    async fn handle_message(&self, msg: &BorrowedMessage<'_>) -> Result<()> {
        let jrow_topic = self.jrow_topic_template.render(msg.topic());
        let data = message_to_json(msg);

        self.server
            .publish_persistent(jrow_topic.clone(), data)
            .await
            .map_err(|e| GatewayError::Jrow(e.to_string()))?;

        self.messages_published.fetch_add(1, Ordering::Relaxed);

        // Only commit the Kafka offset once the message has been durably
        // published into jrow, so a crash before that point results in
        // redelivery instead of data loss.
        if let Err(e) = self.consumer.commit_message(msg, CommitMode::Async) {
            tracing::warn!(error = %e, "Failed to commit Kafka offset");
        }

        tracing::debug!(
            kafka_topic = msg.topic(),
            jrow_topic = %jrow_topic,
            partition = msg.partition(),
            offset = msg.offset(),
            "Bridged Kafka message to jrow"
        );

        Ok(())
    }

    /// Current statistics snapshot
    pub fn stats(&self) -> SourceStats {
        SourceStats {
            messages_consumed: self.messages_consumed.load(Ordering::Relaxed),
            messages_published: self.messages_published.load(Ordering::Relaxed),
            publish_errors: self.publish_errors.load(Ordering::Relaxed),
        }
    }

    /// Verify Kafka broker connectivity by fetching cluster metadata
    pub fn test_connectivity(&self) -> Result<()> {
        self.consumer
            .client()
            .fetch_metadata(None, std::time::Duration::from_secs(5))?;
        Ok(())
    }
}

/// Convert a Kafka message into a jrow-friendly JSON envelope
fn message_to_json(msg: &BorrowedMessage<'_>) -> serde_json::Value {
    let key = msg
        .key()
        .map(|k| String::from_utf8_lossy(k).to_string());

    let value = match msg.payload() {
        Some(bytes) => match serde_json::from_slice::<serde_json::Value>(bytes) {
            Ok(v) => v,
            Err(_) => serde_json::Value::String(String::from_utf8_lossy(bytes).to_string()),
        },
        None => serde_json::Value::Null,
    };

    serde_json::json!({
        "kafka_topic": msg.topic(),
        "kafka_partition": msg.partition(),
        "kafka_offset": msg.offset(),
        "kafka_timestamp": msg.timestamp().to_millis(),
        "key": key,
        "value": value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_default() {
        let stats = SourceStats::default();
        assert_eq!(stats.messages_consumed, 0);
        assert_eq!(stats.messages_published, 0);
        assert_eq!(stats.publish_errors, 0);
    }
}
