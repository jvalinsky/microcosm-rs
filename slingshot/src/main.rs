// use foyer::HybridCache;
// use foyer::{Engine, DirectFsDeviceOptions, HybridCacheBuilder};
use metrics_exporter_prometheus::PrometheusBuilder;
use slingshot::{consume, error::MainTaskError, firehose_cache, serve};

use clap::Parser;
use tokio_util::sync::CancellationToken;

/// Slingshot record edge cache
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    /// Jetstream server to connect to (exclusive with --fixture). Provide either a wss:// URL, or a shorhand value:
    /// 'us-east-1', 'us-east-2', 'us-west-1', or 'us-west-2'
    #[arg(long)]
    jetstream: String,
    /// don't request zstd-compressed jetstream events
    ///
    /// reduces CPU at the expense of more ingress bandwidth
    #[arg(long, action)]
    jetstream_no_zstd: bool,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::init();

    let shutdown = CancellationToken::new();

    let ctrlc_shutdown = shutdown.clone();
    ctrlc::set_handler(move || ctrlc_shutdown.cancel()).expect("failed to set ctrl-c handler");

    let args = Args::parse();

    if let Err(e) = install_metrics_server() {
        log::error!("failed to install metrics server: {e:?}");
    } else {
        log::info!("metrics listening at http://0.0.0.0:8765");
    }

    log::info!("setting up firehose cache...");
    let cache = firehose_cache("./foyer").await?;
    log::info!("firehose cache ready.");

    let mut tasks: tokio::task::JoinSet<Result<(), MainTaskError>> = tokio::task::JoinSet::new();

    let server_shutdown = shutdown.clone();
    let server_cache_handle = cache.clone();
    tasks.spawn(async move {
        serve(server_cache_handle, server_shutdown).await?;
        Ok(())
    });

    let consumer_shutdown = shutdown.clone();
    tasks.spawn(async move {
        consume(
            args.jetstream,
            None,
            args.jetstream_no_zstd,
            consumer_shutdown,
            cache,
        )
        .await?;
        Ok(())
    });

    tokio::select! {
        _ = shutdown.cancelled() => log::warn!("shutdown requested"),
        Some(r) = tasks.join_next() => {
            log::warn!("a task exited, shutting down: {r:?}");
            shutdown.cancel();
        }
    }

    tokio::select! {
        _ = async {
            while let Some(completed) = tasks.join_next().await {
                log::info!("shutdown: task completed: {completed:?}");
            }
        } => {},
        _ = tokio::time::sleep(std::time::Duration::from_secs(3)) => {
            log::info!("shutdown: not all tasks completed on time. aborting...");
            tasks.shutdown().await;
        },
    }

    log::info!("bye!");

    Ok(())
}

fn install_metrics_server() -> Result<(), metrics_exporter_prometheus::BuildError> {
    log::info!("installing metrics server...");
    let host = [0, 0, 0, 0];
    let port = 8765;
    PrometheusBuilder::new()
        .set_quantiles(&[0.5, 0.9, 0.99, 1.0])?
        .set_bucket_duration(std::time::Duration::from_secs(300))?
        .set_bucket_count(std::num::NonZero::new(12).unwrap()) // count * duration = 60 mins. stuff doesn't happen that fast here.
        .set_enable_unit_suffix(false) // this seemed buggy for constellation (sometimes wouldn't engage)
        .with_http_listener((host, port))
        .install()?;
    log::info!(
        "metrics server installed! listening on http://{}.{}.{}.{}:{port}",
        host[0],
        host[1],
        host[2],
        host[3]
    );
    Ok(())
}
