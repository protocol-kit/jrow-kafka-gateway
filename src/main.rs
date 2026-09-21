//! jrow-kafka-gateway: bidirectional bridge between Kafka and jrow
//!
//! Consumes configured Kafka topics and publishes them into an embedded jrow
//! server, and/or subscribes to a jrow persistent subscription and produces
//! messages onto Kafka topics.

use clap::Parser;
use jrow_kafka_gateway::{Config, Gateway};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

/// Command-line arguments
#[derive(Parser, Debug)]
#[command(
    name = "jrow-kafka-gateway",
    version,
    about = "Bidirectional gateway bridging Kafka topics and jrow persistent pub/sub",
    long_about = "Bridges Kafka and jrow in both directions: consumes Kafka topics and \
                  publishes them into an embedded, persistence-backed jrow-server \
                  (Kafka -> jrow), and subscribes to a jrow persistent subscription to \
                  produce messages onto Kafka topics (jrow -> Kafka). Both directions are \
                  independently toggleable via configuration."
)]
struct Args {
    /// Path to configuration file (TOML)
    #[arg(short, long, value_name = "FILE")]
    config: PathBuf,

    /// Enable verbose logging (DEBUG level)
    #[arg(short, long)]
    verbose: bool,

    /// Enable trace logging (TRACE level)
    #[arg(short, long)]
    trace: bool,

    /// Test configuration and connectivity, then exit
    #[arg(short = 'T', long)]
    test: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command-line arguments
    let args = Args::parse();

    // Initialize logging
    let log_level = if args.trace {
        "trace"
    } else if args.verbose {
        "debug"
    } else {
        "info"
    };

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new(format!(
                "jrow_kafka_gateway={},jrow_server=info,jrow_client=info",
                log_level
            ))
        }))
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    // Print banner
    print_banner();

    // Load configuration
    tracing::info!(config_file = %args.config.display(), "Loading configuration");

    let config = Config::from_file(&args.config)
        .map_err(|e| format!("Failed to load configuration: {}", e))?;

    tracing::info!("Configuration loaded successfully");

    // Test mode - validate config and test connectivity, then exit
    if args.test {
        tracing::info!("Running in test mode");
        print_config_summary(&config);

        let gateway = Gateway::new(config).await?;

        match gateway.test().await {
            Ok(()) => {
                println!("\n✓ Configuration is valid");
                println!("✓ jrow-server initialized successfully");
                println!("✓ Kafka connectivity verified");
                println!("\nTest passed! Ready to run.");
                return Ok(());
            }
            Err(e) => {
                eprintln!("\n✗ Test failed: {}", e);
                std::process::exit(1);
            }
        }
    }

    // Build and run the gateway
    let gateway = Gateway::new(config).await?;
    gateway.run().await?;

    Ok(())
}

fn print_banner() {
    println!(
        r#"
     _                                _         __ _              
    (_)                              | |       / _| |             
     _ _ __ _____      __   ___  ___ | |_ ___ | |_| | __ ___      __
    | | '__/ _ \ \ /\ / /  / _ \/ _ \| __/ _ \|  _| |/ _` \ \ /\ / /
    | | | | (_) \ V  V /  |  __/ (_) | ||  __/| | | | (_| |\ V  V / 
    | |_|  \___/ \_/\_/    \___|\___/ \__\___||_| |_|\__,_| \_/\_/  
   _/ |         __/ | __ _  __ _| |_ _____      ____ _ _   _
  |__/         / _` |/ _` |/ _` | __/ _ \ \ /\ / / _` | | | |
              | (_| | (_| | (_| | ||  __/\ V  V / (_| | |_| |
               \__, |\__,_|\__,_|\__\___| \_/\_/ \__,_|\__, |
                __/ |                                   __/ |
               |___/                                   |___/
    "#
    );
    println!("  Bidirectional Kafka <-> jrow Gateway");
    println!("  Version {}\n", env!("CARGO_PKG_VERSION"));
}

fn print_config_summary(config: &Config) {
    println!("\n📋 Configuration Summary:");
    println!("  ├─ jrow Bind Address:  {}", config.jrow.bind_address);
    println!("  ├─ jrow Storage Path:  {}", config.jrow.persistent_storage_path);
    println!("  ├─ jrow Client URL:    {}", config.jrow.resolved_client_url());
    println!("  ├─ Kafka Brokers:      {}", config.kafka.bootstrap_servers);
    if config.source.enabled {
        println!("  ├─ Source (Kafka->jrow): enabled");
        println!("  │  ├─ Topics:          {}", config.source.topics.join(", "));
        println!("  │  ├─ Group ID:        {}", config.source.group_id);
        println!("  │  └─ jrow Template:   {}", config.source.jrow_topic_template);
    } else {
        println!("  ├─ Source (Kafka->jrow): disabled");
    }
    if config.sink.enabled {
        println!("  ├─ Sink (jrow->Kafka):   enabled");
        println!("  │  ├─ Subscription ID: {}", config.sink.subscription_id);
        println!("  │  ├─ jrow Topic:      {}", config.sink.jrow_topic);
        println!("  │  └─ Kafka Template:  {}", config.sink.kafka_topic_template);
    } else {
        println!("  ├─ Sink (jrow->Kafka):   disabled");
    }
    if config.health.enabled {
        println!("  └─ Health Endpoint:    {}", config.health.bind_address);
    } else {
        println!("  └─ Health Endpoint:    disabled");
    }
}
