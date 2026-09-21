//! Error types for jrow-kafka-gateway

use thiserror::Error;

/// Result type alias for jrow-kafka-gateway operations
pub type Result<T> = std::result::Result<T, GatewayError>;

/// Main error type for the Kafka gateway
#[derive(Error, Debug)]
pub enum GatewayError {
    /// Configuration error (missing/invalid fields)
    #[error("configuration error: {0}")]
    Configuration(String),

    /// Error connecting to or communicating with a jrow server/client
    #[error("jrow error: {0}")]
    Jrow(String),

    /// Error from the Kafka client (librdkafka)
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    /// Serialization/deserialization error
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// TOML parsing error
    #[error("TOML parsing error: {0}")]
    TomlParse(#[from] toml::de::Error),

    /// IO error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// jrow-core error (bubbled up from JrowServer/JrowClient)
    #[error("jrow-core error: {0}")]
    JrowCore(#[from] jrow_core::Error),

    /// Topic mapping/template error
    #[error("topic mapping error: {0}")]
    TopicMapping(String),

    /// Shutdown signal received
    #[error("shutdown signal received")]
    Shutdown,
}

impl GatewayError {
    /// Check if this error represents a transient condition worth retrying/redelivering
    pub fn is_retryable(&self) -> bool {
        match self {
            GatewayError::Kafka(_) => true,
            GatewayError::Jrow(_) => true,
            GatewayError::JrowCore(_) => true,
            GatewayError::Io(_) => true,
            GatewayError::Configuration(_) => false,
            GatewayError::Serialization(_) => false,
            GatewayError::TomlParse(_) => false,
            GatewayError::TopicMapping(_) => false,
            GatewayError::Shutdown => false,
        }
    }
}
