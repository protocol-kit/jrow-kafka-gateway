//! Orchestrates the embedded jrow-server together with the optional
//! Kafka -> jrow source and jrow -> Kafka sink bridges.

use crate::config::Config;
use crate::error::{GatewayError, Result};
use crate::health::{self, HealthState};
use crate::sink::{KafkaSink, SinkStats};
use crate::source::{KafkaSource, SourceStats};
use jrow_server::JrowServer;
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

/// Combined statistics for both bridge directions
#[derive(Debug, Clone, Serialize)]
pub struct GatewayStats {
    pub source: Option<SourceStats>,
    pub sink: Option<SinkStats>,
}

/// Top-level gateway: owns the embedded jrow-server and the enabled bridges
pub struct Gateway {
    config: Config,
    server: Arc<JrowServer>,
    source: Option<Arc<KafkaSource>>,
    sink: Option<Arc<KafkaSink>>,
}

impl Gateway {
    /// Build the embedded jrow-server and the Kafka -> jrow source (if enabled).
    ///
    /// The jrow -> Kafka sink is created lazily in [`Gateway::run`] because it
    /// needs a live `JrowClient` connection, which by default targets this same
    /// embedded server.
    pub async fn new(config: Config) -> Result<Self> {
        let addr: SocketAddr = config.jrow.bind_address.parse().map_err(|e| {
            GatewayError::Configuration(format!("invalid jrow.bind_address: {}", e))
        })?;

        let server = JrowServer::builder()
            .bind(addr)
            .with_persistent_storage(&config.jrow.persistent_storage_path)
            .subscription_timeout(Duration::from_secs(3600))
            .build()
            .await?;
        let server = Arc::new(server);

        tracing::info!(
            bind_address = %config.jrow.bind_address,
            storage_path = %config.jrow.persistent_storage_path,
            "Embedded jrow-server initialized"
        );

        let source = if config.source.enabled {
            Some(Arc::new(KafkaSource::new(
                &config.kafka,
                &config.source,
                Arc::clone(&server),
            )?))
        } else {
            None
        };

        Ok(Self {
            config,
            server,
            source,
            sink: None,
        })
    }

    /// Current statistics snapshot for both directions
    pub fn stats(&self) -> GatewayStats {
        GatewayStats {
            source: self.source.as_ref().map(|s| s.stats()),
            sink: self.sink.as_ref().map(|s| s.stats()),
        }
    }

    /// Validate configuration and exercise external connectivity (Kafka
    /// brokers, and jrow for the sink side) without starting the long-running
    /// service. Used by the CLI's `--test` flag.
    pub async fn test(&self) -> Result<()> {
        if let Some(source) = &self.source {
            tracing::info!("Testing Kafka broker connectivity (source)...");
            source.test_connectivity()?;
            tracing::info!("Kafka source connectivity OK");
        }

        if self.config.sink.enabled {
            tracing::info!("Testing jrow + Kafka connectivity (sink)...");

            // Temporarily accept jrow connections so a JrowClient pointed at
            // our own embedded server can complete its WebSocket handshake.
            let server_clone = Arc::clone(&self.server);
            let server_handle = tokio::spawn(async move {
                let _ = server_clone.run().await;
            });

            let client_url = self.config.jrow.resolved_client_url();
            let sink_result =
                KafkaSink::new(&client_url, &self.config.kafka, &self.config.sink).await;

            server_handle.abort();

            let sink = sink_result?;
            sink.test_connectivity()?;
            tracing::info!("Kafka sink connectivity OK");
        }

        Ok(())
    }

    /// Run the gateway until a shutdown signal (Ctrl+C) is received
    pub async fn run(mut self) -> Result<()> {
        let (shutdown_tx, _) = broadcast::channel::<()>(4);

        // Start accepting jrow WebSocket connections. The TCP listener was
        // already bound during `JrowServer::builder().build()`, so it is safe
        // to connect a client to it even before this task starts its accept
        // loop (the connection will simply queue until accepted).
        let server_clone = Arc::clone(&self.server);
        let server_handle: JoinHandle<()> = tokio::spawn(async move {
            if let Err(e) = server_clone.run().await {
                tracing::error!(error = %e, "jrow-server error");
            }
        });

        let mut task_handles: Vec<JoinHandle<()>> = Vec::new();

        if let Some(source) = self.source.clone() {
            let rx = shutdown_tx.subscribe();
            task_handles.push(tokio::spawn(async move {
                if let Err(e) = source.run(rx).await {
                    tracing::error!(error = %e, "Kafka source error");
                }
            }));
        }

        if self.config.sink.enabled {
            let client_url = self.config.jrow.resolved_client_url();
            let sink = KafkaSink::new(&client_url, &self.config.kafka, &self.config.sink).await?;
            let sink = Arc::new(sink);
            self.sink = Some(Arc::clone(&sink));

            task_handles.push(tokio::spawn(async move {
                if let Err(e) = sink.run().await {
                    tracing::error!(error = %e, "Kafka sink error");
                }
            }));
        }

        let health_handle = if self.config.health.enabled {
            let state = HealthState::new(self.source.clone(), self.sink.clone());
            let bind_address = self.config.health.bind_address.clone();
            Some(tokio::spawn(async move {
                if let Err(e) = health::serve(&bind_address, state).await {
                    tracing::error!(error = %e, "Health server error");
                }
            }))
        } else {
            None
        };

        tracing::info!("jrow-kafka-gateway running. Press Ctrl+C to shut down.");

        tokio::signal::ctrl_c().await?;
        tracing::info!("Received shutdown signal (Ctrl+C), shutting down gracefully");

        // Stop the Kafka -> jrow consume loop first so no new messages are pulled
        let _ = shutdown_tx.send(());

        // Flush any in-flight Kafka produces from the jrow -> Kafka side
        if let Some(sink) = &self.sink {
            sink.flush().await;
        }

        for handle in task_handles {
            handle.abort();
        }
        if let Some(handle) = health_handle {
            handle.abort();
        }
        server_handle.abort();

        tracing::info!("Shutdown complete");
        Ok(())
    }
}
