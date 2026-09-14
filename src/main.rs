use anyhow::Result;
use clap::Parser;
use extractor::discogs::rules;
use extractor::discogs::rules::RulesConfig;
use extractor::{config::ExtractorConfig, discogs, health::HealthServer, runtime, telemetry};
use std::sync::Arc;
use tokio::signal;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tracing::{error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// GrooveMap catalog ingestion for Discogs.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Force reprocess all files
    #[clap(short, long, env = "FORCE_REPROCESS", value_parser = clap::builder::BoolishValueParser::new(), default_value_t = false)]
    force_reprocess: bool,

    /// Path to data quality rules YAML file
    #[clap(long, env = "DATA_QUALITY_RULES")]
    data_quality_rules: Option<std::path::PathBuf>,

    /// Process a versioned local smoke manifest once, without public dump acquisition
    #[clap(long, env = "LOCAL_MANIFEST")]
    local_manifest: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize tracing with LOG_LEVEL environment variable
    // Supports: DEBUG, INFO, WARNING, ERROR, CRITICAL (maps to Rust's trace, debug, info, warn, error)
    let log_level = std::env::var("LOG_LEVEL").unwrap_or_else(|_| "INFO".to_string());
    let filter = build_tracing_filter(&log_level);

    // The traces bootstrap has to run BEFORE the subscriber is installed, because the
    // OpenTelemetry layer it produces is part of that subscriber. It logs nothing that
    // survives this ordering, so the resolved state is reported below instead. Returns None
    // (and never fails startup) when traces are disabled or no endpoint is configured.
    let trace_provider = telemetry::init_traces(telemetry::DEFAULT_SERVICE_NAME);

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(filter))
        .with(tracing_subscriber::fmt::layer().with_target(false).with_thread_ids(false).with_line_number(true).json())
        // `Option<Layer>` is itself a layer: `None` costs nothing per span, so a disabled
        // trace pipeline leaves the logging path exactly as it was.
        .with(trace_provider.as_ref().map(telemetry::trace_layer))
        .init();

    if trace_provider.is_some() {
        info!("🧵 OpenTelemetry traces exporting over OTLP/HTTP-protobuf");
    } else {
        info!("📉 OpenTelemetry traces disabled (no OTLP endpoint, or OTEL_TRACES_EXPORTER=none)");
    }

    // Telemetry bootstrap. Must precede any instrumented code path: the instruments bind
    // once to whatever MeterProvider is global when they are first touched. Returns None
    // (and logs once) when no OTLP endpoint is configured — never fails startup.
    let meter_provider = telemetry::init_metrics(telemetry::DEFAULT_SERVICE_NAME);

    // Display ASCII art
    print_ascii_art();

    // Load configuration from environment (drop-in replacement for extractor)
    let mut config = match ExtractorConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            error!("❌ Configuration error: {}", e);
            std::process::exit(1);
        }
    };

    // CLI arg takes precedence over env var for rules path
    if args.data_quality_rules.is_some() {
        config.data_quality_rules = args.data_quality_rules;
    }

    info!("{}", startup_banner_message());

    // Every metric carries `source`; this service extracts exactly one provider, so it is
    // set once here rather than threaded through every call site.
    telemetry::set_source(telemetry::SOURCE_DISCOGS);

    // Load and compile data quality rules if configured
    let compiled_rules = if let Some(ref rules_path) = config.data_quality_rules {
        info!("📋 Loading data quality rules from {:?}", rules_path);
        match RulesConfig::load(rules_path) {
            // nosemgrep: rust.actix.path-traversal.tainted-path.tainted-path
            Ok(rules_config) => match rules::CompiledRulesConfig::compile(rules_config) {
                Ok(compiled) => {
                    info!("✅ Data quality rules loaded and compiled successfully");
                    Some(Arc::new(compiled))
                }
                Err(e) => {
                    error!("❌ Failed to compile data quality rules: {}", e);
                    std::process::exit(1);
                }
            },
            Err(e) => {
                error!("❌ Failed to load data quality rules: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    let config = Arc::new(config);

    // Initialize shared state
    let state = Arc::new(RwLock::new(runtime::ExtractorState::default()));
    let trigger = Arc::new(Mutex::new(None::<bool>));

    // Start health server
    let health_server = HealthServer::new(config.health_port, state.clone(), trigger.clone());
    let health_handle = tokio::spawn(async move {
        if let Err(e) = health_server.run().await {
            error!("❌ Health server error: {}", e);
        }
    });

    // Set up signal handlers
    let shutdown = setup_shutdown_handler();

    // Create factory for message queue connections
    let mq_factory: Arc<dyn runtime::MessageQueueFactory> = Arc::new(runtime::DefaultMessageQueueFactory);

    let extraction_result = discogs::run_extraction_loop(
        config.clone(),
        state.clone(),
        shutdown.clone(),
        args.force_reprocess,
        mq_factory,
        trigger.clone(),
        compiled_rules,
        args.local_manifest,
    )
    .await;

    // Cleanup
    info!("🛑 Shutting down discogs-ingestion...");
    health_handle.abort();

    // Flush the final metric export and the last spans before the process goes away, on
    // BOTH exit paths — the failure path below ends in process::exit, which runs no
    // destructors.
    telemetry::shutdown_metrics(meter_provider).await;
    telemetry::shutdown_traces(trace_provider).await;

    match extraction_result {
        Ok(_) => {
            info!("✅ discogs-ingestion service shutdown complete");
            Ok(())
        }
        Err(e) => {
            error!("❌ discogs-ingestion failed: {}", e);
            // Sleep before exiting so docker-compose's `restart: on-failure`
            // policy can't flap us through a rate-limit window. The polite
            // client already absorbs single Retry-After cooldowns up to 2h;
            // this cooldown is a backstop for the residual case where the
            // failure cause is something the client can't retry past
            // (cap exceeded, network error, etc.).
            apply_failure_cooldown(std::env::var("FAILURE_COOLDOWN_SECS").ok().as_deref()).await;
            std::process::exit(1);
        }
    }
}

/// Default cooldown applied before the extractor exits with a non-zero status.
const DEFAULT_FAILURE_COOLDOWN_SECS: u64 = 600;

/// Parse the `FAILURE_COOLDOWN_SECS` env-var value into a number of seconds.
/// Garbage / missing values fall back to [`DEFAULT_FAILURE_COOLDOWN_SECS`].
/// Pure function — extracted so the env→duration mapping is unit-testable
/// without invoking `process::exit`.
fn parse_failure_cooldown(env_value: Option<&str>) -> u64 {
    env_value.and_then(|s| s.parse::<u64>().ok()).unwrap_or(DEFAULT_FAILURE_COOLDOWN_SECS)
}

/// Sleep the configured failure cooldown, if non-zero, before the caller exits.
async fn apply_failure_cooldown(env_value: Option<&str>) {
    let cooldown = parse_failure_cooldown(env_value);
    if cooldown > 0 {
        error!("😴 Sleeping {}s before exiting to avoid restart-loop flapping (override with FAILURE_COOLDOWN_SECS=0)", cooldown);
        tokio::time::sleep(std::time::Duration::from_secs(cooldown)).await;
    }
}

/// Startup banner text reflecting the actual resolved `--source` mode, instead of a
/// hardcoded "Discogs" claim regardless of which extractor container is running.
fn startup_banner_message() -> &'static str {
    "🚀 Starting GrooveMap discogs-ingestion"
}

fn print_ascii_art() {
    println!("{}", ascii_art());
}

fn ascii_art() -> &'static str {
    r#"
         _        _               _                  _   _
 __ __ _| |_ __ _| |___  __ _ ___(_)_ _  __ _ ___ __| |_(_)___ _ _
/ _/ _` |  _/ _` | / _ \/ _` |___| | ' \/ _` / -_|_-<  _| / _ \ ' \
\__\__,_|\__\__,_|_\___/\__, |   |_|_||_\__, \___/__/\__|_\___/_||_|
                        |___/           |___/
                          discogs-ingestion
"#
}

fn setup_shutdown_handler() -> Arc<tokio::sync::Notify> {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_clone = shutdown.clone();

    tokio::spawn(async move {
        #[cfg(unix)]
        {
            let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate()).expect("failed to install SIGTERM handler");
            tokio::select! {
                _ = signal::ctrl_c() => {
                    info!("🛑 Received SIGINT (Ctrl+C)");
                }
                _ = sigterm.recv() => {
                    info!("🛑 Received SIGTERM");
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = signal::ctrl_c().await;
            info!("🛑 Received shutdown signal");
        }
        shutdown_clone.notify_waiters();
    });

    shutdown
}

/// Build tracing filter string from Python-style log level
fn build_tracing_filter(log_level: &str) -> String {
    let rust_level = match log_level.to_uppercase().as_str() {
        "DEBUG" => "debug",
        "INFO" => "info",
        "WARNING" | "WARN" => "warn",
        "ERROR" => "error",
        "CRITICAL" => "error",
        _ => "info",
    };
    let lapin_level = if rust_level == "debug" { "info" } else { "warn" };
    format!("extractor={},lapin={}", rust_level, lapin_level)
}

#[cfg(test)]
#[path = "tests/main_tests.rs"]
mod tests;
