#![forbid(clippy::unwrap_used)]

mod server;

use std::{env, net::SocketAddr, sync::Arc};

use axum::{Router, routing::get};
use color_eyre::eyre::Context;
use rustls::crypto::{CryptoProvider, ring};

fn setup_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            env::var("RUST_LOG").unwrap_or_else(|_| {
                format!(
                    "{}=debug,tower_http=debug,rosu_v2=trace,axum::rejection=trace",
                    env!("CARGO_CRATE_NAME")
                )
            }),
        ))
        .with_line_number(true)
        .with_file(true)
        .init();
}

fn prometheus() -> color_eyre::Result<()> {
    let ip: SocketAddr = ([0, 0, 0, 0], 10101).into();
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new().with_http_listener(ip);

    builder.install().wrap_err("Failed to install Prometheus")?;
    tracing::info!(ip = ?ip, "Installing Prometheus");
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(error) = CryptoProvider::install_default(ring::default_provider()) {
        eprintln!("Failed to install rustls crypto provider: {error:?}");
        return;
    }

    setup_logging();

    if let Err(error) = prometheus() {
        tracing::error!(%error, "Prometheus installation failed");
        return;
    }

    let _ = dotenvy::dotenv();

    tracing::info!(
        crate_name = env!("CARGO_CRATE_NAME"),
        "Starting application"
    );

    let server = match server::Server::init().await {
        Ok(server) => Arc::new(server),
        Err(error) => {
            tracing::error!(%error, "Server initialization failed");
            return;
        }
    };

    let addr: SocketAddr = match env::var("APP_ADDR") {
        Ok(addr) => match addr.parse() {
            Ok(addr) => addr,
            Err(error) => {
                tracing::error!(%error, "Invalid APP_ADDR");
                return;
            }
        },
        Err(_) => ([0, 0, 0, 0], 6727).into(),
    };

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::error!(%addr, %error, "Failed to bind HTTP listener");
            return;
        }
    };
    tracing::info!(%addr, "Listening for HTTP requests");

    let app = Router::new()
        .route("/", get(server::handle_root))
        .with_state(Arc::clone(&server));

    if let Err(error) = axum::serve(listener, app.into_make_service()).await {
        tracing::error!(%error, "HTTP server failed");
    }
}
