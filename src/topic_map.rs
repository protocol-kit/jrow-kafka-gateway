//! Kafka <-> jrow topic name mapping via simple `{topic}` templates

use crate::error::{GatewayError, Result};

/// A topic name template containing a literal `{topic}` placeholder.
///
/// Used in both directions:
/// - Kafka -> jrow: `{topic}` is replaced with the consumed Kafka topic name.
/// - jrow -> Kafka: `{topic}` is replaced with the jrow topic delivered in the
///   persistent message envelope.
#[derive(Debug, Clone)]
pub struct TopicTemplate(String);

const PLACEHOLDER: &str = "{topic}";

impl TopicTemplate {
    /// Create a new template, validating that it contains the `{topic}` placeholder
    pub fn new(template: impl Into<String>) -> Result<Self> {
        let template = template.into();
        if !template.contains(PLACEHOLDER) {
            return Err(GatewayError::TopicMapping(format!(
                "template '{}' must contain the '{}' placeholder",
                template, PLACEHOLDER
            )));
        }
        Ok(Self(template))
    }

    /// Render the template by substituting the placeholder with `topic`
    pub fn render(&self, topic: &str) -> String {
        self.0.replace(PLACEHOLDER, topic)
    }

    /// The raw template string
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TopicTemplate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_source_direction() {
        let tmpl = TopicTemplate::new("kafka.{topic}").unwrap();
        assert_eq!(tmpl.render("orders"), "kafka.orders");
    }

    #[test]
    fn test_render_sink_direction() {
        let tmpl = TopicTemplate::new("jrow.{topic}").unwrap();
        assert_eq!(tmpl.render("events.user.login"), "jrow.events.user.login");
    }

    #[test]
    fn test_render_static_prefix_suffix() {
        let tmpl = TopicTemplate::new("prefix-{topic}-suffix").unwrap();
        assert_eq!(tmpl.render("x"), "prefix-x-suffix");
    }

    #[test]
    fn test_rejects_missing_placeholder() {
        assert!(TopicTemplate::new("no-placeholder").is_err());
    }

    #[test]
    fn test_multiple_placeholders_all_replaced() {
        let tmpl = TopicTemplate::new("{topic}/{topic}").unwrap();
        assert_eq!(tmpl.render("a"), "a/a");
    }
}
