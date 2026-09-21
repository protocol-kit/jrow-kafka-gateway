//! Minimal HTTP health/stats endpoint for the Kafka gateway

use crate::error::Result;
use crate::sink::{KafkaSink, SinkStats};
use crate::source::{KafkaSource, SourceStats};
use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use std::sync::Arc;

/// Shared state exposed by the health endpoint
#[derive(Clone)]
pub struct HealthState {
    source: Option<Arc<KafkaSource>>,
    sink: Option<Arc<KafkaSink>>,
}

impl HealthState {
    pub fn new(source: Option<Arc<KafkaSource>>, sink: Option<Arc<KafkaSink>>) -> Self {
        Self { source, sink }
    }
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    source: Option<SourceStats>,
    sink: Option<SinkStats>,
}

async fn health_handler(State(state): State<HealthState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy",
        source: state.source.as_ref().map(|s| s.stats()),
        sink: state.sink.as_ref().map(|s| s.stats()),
    })
}

/// Serve the `/health` endpoint on `bind_address` until the process exits
pub async fn serve(bind_address: &str, state: HealthState) -> Result<()> {
    let app = Router::new()
        .route("/health", get(health_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind_address).await?;
    tracing::info!(bind_address = %bind_address, "Health server listening");

    axum::serve(listener, app)
        .await
        .map_err(crate::error::GatewayError::Io)?;

    Ok(())
}
