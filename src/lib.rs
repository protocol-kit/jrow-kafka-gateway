//! jrow-kafka-gateway: bidirectional bridge between Kafka and jrow
//!
//! This crate provides a standalone binary that bridges Kafka topics and jrow's
//! persistent pub/sub in both directions:
//!
//! - **Kafka -> jrow**: consumes configured Kafka topics and publishes them into
//!   an embedded, persistence-backed [`jrow_server::JrowServer`].
//! - **jrow -> Kafka**: subscribes to a jrow persistent subscription (exact topic
//!   or wildcard pattern) via [`jrow_client::JrowClient`] and produces messages
//!   onto Kafka topics.
//!
//! Both directions are independently toggleable via configuration, so the
//! gateway can run as a Kafka source, a Kafka sink, or fully bidirectional.

pub mod config;
pub mod error;
pub mod gateway;
pub mod health;
pub mod sink;
pub mod source;
pub mod topic_map;

pub use config::Config;
pub use error::{GatewayError, Result};
pub use gateway::{Gateway, GatewayStats};
pub use sink::{KafkaSink, SinkStats};
pub use source::{KafkaSource, SourceStats};
pub use topic_map::TopicTemplate;
