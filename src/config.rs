//! Configuration management for jrow-kafka-gateway

use crate::error::{GatewayError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Main configuration structure
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub jrow: JrowConfig,
    pub kafka: KafkaConfig,
    #[serde(default)]
    pub source: SourceConfig,
    #[serde(default)]
    pub sink: SinkConfig,
    #[serde(default)]
    pub health: HealthConfig,
}

/// jrow embedded server / client configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JrowConfig {
    /// Address the embedded jrow-server binds to (e.g. "127.0.0.1:9950")
    pub bind_address: String,

    /// Path to the sled database used for persistent subscriptions/messages
    pub persistent_storage_path: String,

    /// WebSocket URL the sink side connects to. Defaults to `ws://{bind_address}`
    /// (the gateway's own embedded server) if not set. Override this to point the
    /// sink at a different/external jrow-server (e.g. when splitting source and
    /// sink across two gateway instances).
    #[serde(default)]
    pub client_url: Option<String>,
}

impl JrowConfig {
    /// Resolve the URL the sink's JrowClient should connect to
    pub fn resolved_client_url(&self) -> String {
        self.client_url
            .clone()
            .unwrap_or_else(|| format!("ws://{}", self.bind_address))
    }
}

/// Kafka connection configuration shared by the source and sink
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KafkaConfig {
    /// Comma-separated list of Kafka bootstrap brokers (e.g. "localhost:9092")
    pub bootstrap_servers: String,

    /// Additional passthrough librdkafka client options (e.g. "security.protocol")
    #[serde(default)]
    pub options: HashMap<String, String>,
}

/// Kafka -> jrow configuration ("source" direction)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceConfig {
    /// Enable the Kafka -> jrow direction
    #[serde(default)]
    pub enabled: bool,

    /// Kafka consumer group id
    #[serde(default = "default_group_id")]
    pub group_id: String,

    /// Kafka topics to consume from
    #[serde(default)]
    pub topics: Vec<String>,

    /// Template used to derive the jrow topic from a Kafka topic name.
    /// The literal placeholder `{topic}` is replaced with the Kafka topic name.
    #[serde(default = "default_jrow_topic_template")]
    pub jrow_topic_template: String,

    /// Kafka `auto.offset.reset` policy ("earliest" or "latest")
    #[serde(default = "default_auto_offset_reset")]
    pub auto_offset_reset: String,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            group_id: default_group_id(),
            topics: Vec::new(),
            jrow_topic_template: default_jrow_topic_template(),
            auto_offset_reset: default_auto_offset_reset(),
        }
    }
}

fn default_group_id() -> String {
    "jrow-kafka-gateway".to_string()
}

fn default_jrow_topic_template() -> String {
    "kafka.{topic}".to_string()
}

fn default_auto_offset_reset() -> String {
    "earliest".to_string()
}

/// jrow -> Kafka configuration ("sink" direction)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SinkConfig {
    /// Enable the jrow -> Kafka direction
    #[serde(default)]
    pub enabled: bool,

    /// Unique persistent subscription id for this sink
    #[serde(default = "default_subscription_id")]
    pub subscription_id: String,

    /// jrow topic or pattern to subscribe to (e.g. "events.>" )
    #[serde(default)]
    pub jrow_topic: String,

    /// Template used to derive the Kafka topic from the delivered jrow topic.
    /// The literal placeholder `{topic}` is replaced with the jrow topic name.
    #[serde(default = "default_kafka_topic_template")]
    pub kafka_topic_template: String,
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            subscription_id: default_subscription_id(),
            jrow_topic: String::new(),
            kafka_topic_template: default_kafka_topic_template(),
        }
    }
}

fn default_subscription_id() -> String {
    "jrow-kafka-gateway-sink".to_string()
}

fn default_kafka_topic_template() -> String {
    "jrow.{topic}".to_string()
}

/// Health/stats HTTP endpoint configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HealthConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_health_bind_address")]
    pub bind_address: String,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: default_health_bind_address(),
        }
    }
}

fn default_health_bind_address() -> String {
    "127.0.0.1:8090".to_string()
}

impl Config {
    /// Load configuration from a TOML file
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let content = std::fs::read_to_string(path.as_ref())?;
        Self::parse_toml(&content)
    }

    /// Parse configuration from a TOML string
    pub fn parse_toml(content: &str) -> Result<Self> {
        // Substitute environment variables first (e.g. "${KAFKA_BOOTSTRAP_SERVERS}")
        let content = substitute_env_vars(content);

        let config: Config = toml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        if self.jrow.bind_address.is_empty() {
            return Err(GatewayError::Configuration(
                "jrow.bind_address cannot be empty".to_string(),
            ));
        }
        if self.jrow.persistent_storage_path.is_empty() {
            return Err(GatewayError::Configuration(
                "jrow.persistent_storage_path cannot be empty".to_string(),
            ));
        }
        if self.kafka.bootstrap_servers.is_empty() {
            return Err(GatewayError::Configuration(
                "kafka.bootstrap_servers cannot be empty".to_string(),
            ));
        }

        if !self.source.enabled && !self.sink.enabled {
            return Err(GatewayError::Configuration(
                "at least one of source.enabled or sink.enabled must be true".to_string(),
            ));
        }

        if self.source.enabled {
            if self.source.group_id.is_empty() {
                return Err(GatewayError::Configuration(
                    "source.group_id cannot be empty when source.enabled is true".to_string(),
                ));
            }
            if self.source.topics.is_empty() {
                return Err(GatewayError::Configuration(
                    "source.topics cannot be empty when source.enabled is true".to_string(),
                ));
            }
            if self.source.jrow_topic_template.is_empty() {
                return Err(GatewayError::Configuration(
                    "source.jrow_topic_template cannot be empty when source.enabled is true"
                        .to_string(),
                ));
            }
            if !self.source.jrow_topic_template.contains("{topic}") {
                return Err(GatewayError::Configuration(
                    "source.jrow_topic_template must contain the '{topic}' placeholder"
                        .to_string(),
                ));
            }
        }

        if self.sink.enabled {
            if self.sink.subscription_id.is_empty() {
                return Err(GatewayError::Configuration(
                    "sink.subscription_id cannot be empty when sink.enabled is true".to_string(),
                ));
            }
            if self.sink.jrow_topic.is_empty() {
                return Err(GatewayError::Configuration(
                    "sink.jrow_topic cannot be empty when sink.enabled is true".to_string(),
                ));
            }
            if self.sink.kafka_topic_template.is_empty() {
                return Err(GatewayError::Configuration(
                    "sink.kafka_topic_template cannot be empty when sink.enabled is true"
                        .to_string(),
                ));
            }
            if !self.sink.kafka_topic_template.contains("{topic}") {
                return Err(GatewayError::Configuration(
                    "sink.kafka_topic_template must contain the '{topic}' placeholder"
                        .to_string(),
                ));
            }
        }

        if self.health.enabled && self.health.bind_address.is_empty() {
            return Err(GatewayError::Configuration(
                "health.bind_address cannot be empty when health.enabled is true".to_string(),
            ));
        }

        Ok(())
    }

    /// Create a default configuration (useful for testing)
    pub fn default_config() -> Self {
        Config {
            jrow: JrowConfig {
                bind_address: "127.0.0.1:9950".to_string(),
                persistent_storage_path: "./data/kafka-gateway.db".to_string(),
                client_url: None,
            },
            kafka: KafkaConfig {
                bootstrap_servers: "localhost:9092".to_string(),
                options: HashMap::new(),
            },
            source: SourceConfig {
                enabled: true,
                group_id: default_group_id(),
                topics: vec!["orders".to_string()],
                jrow_topic_template: default_jrow_topic_template(),
                auto_offset_reset: default_auto_offset_reset(),
            },
            sink: SinkConfig {
                enabled: true,
                subscription_id: default_subscription_id(),
                jrow_topic: "events.>".to_string(),
                kafka_topic_template: default_kafka_topic_template(),
            },
            health: HealthConfig::default(),
        }
    }
}

/// Substitute environment variables in the form ${VAR_NAME}
fn substitute_env_vars(content: &str) -> String {
    let mut result = content.to_string();

    let re = regex::Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").unwrap();

    for cap in re.captures_iter(content) {
        let full_match = &cap[0];
        let var_name = &cap[1];

        if let Ok(value) = std::env::var(var_name) {
            result = result.replace(full_match, &value);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_var_substitution() {
        std::env::set_var("JKG_TEST_VAR", "test_value");

        let input = "key = \"${JKG_TEST_VAR}\"";
        let output = substitute_env_vars(input);

        assert_eq!(output, "key = \"test_value\"");
    }

    #[test]
    fn test_default_config_is_valid() {
        let config = Config::default_config();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_requires_at_least_one_direction() {
        let mut config = Config::default_config();
        config.source.enabled = false;
        config.sink.enabled = false;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_source_requires_topics() {
        let mut config = Config::default_config();
        config.source.topics.clear();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_sink_requires_jrow_topic() {
        let mut config = Config::default_config();
        config.sink.jrow_topic.clear();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_templates_require_placeholder() {
        let mut config = Config::default_config();
        config.source.jrow_topic_template = "kafka.static".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_resolved_client_url_defaults_to_bind_address() {
        let config = Config::default_config();
        assert_eq!(
            config.jrow.resolved_client_url(),
            format!("ws://{}", config.jrow.bind_address)
        );
    }

    #[test]
    fn test_resolved_client_url_override() {
        let mut config = Config::default_config();
        config.jrow.client_url = Some("ws://external-jrow:9004".to_string());
        assert_eq!(config.jrow.resolved_client_url(), "ws://external-jrow:9004");
    }

    #[test]
    fn test_parse_from_toml() {
        let toml_str = r#"
[jrow]
bind_address = "127.0.0.1:9950"
persistent_storage_path = "./data/gw.db"

[kafka]
bootstrap_servers = "localhost:9092"

[source]
enabled = true
group_id = "gw"
topics = ["orders"]

[sink]
enabled = true
jrow_topic = "events.>"
"#;
        let config = Config::parse_toml(toml_str).unwrap();
        assert_eq!(config.jrow.bind_address, "127.0.0.1:9950");
        assert!(config.source.enabled);
        assert!(config.sink.enabled);
        assert_eq!(config.sink.jrow_topic, "events.>");
    }
}
