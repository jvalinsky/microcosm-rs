use spacedust::consumer;
use spacedust::server;

use clap::Parser;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// Aggregate links in the at-mosphere
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

    // tokio broadcast keeps a single main output queue for all subscribers.
    // each subscriber clones off a copy of an individual value for each recv.
    // since there's no large per-client buffer, we can make this one kind of
    // big and accommodate more slow/bursty clients.
    //
    // in fact, we *could* even keep lagging clients alive, inserting lag-
    // indicating messages to their output.... but for now we'll drop them to
    // avoid accumulating zombies.
    //
    // events on the channel are individual links as they are discovered. a link
    // contains a source and a target. the target is an at-uri, so it's up to
    // ~1KB max; source is a collection + link path, which can be more but in
    // practice the whole link rarely approaches 1KB total.
    //
    // TODO: determine if a pathological case could blow this up (eg 1MB link
    // paths + slow subscriber -> 16GiB queue)
    let (b, _) = broadcast::channel(16_384);
    let consumer_sender = b.clone();

    let shutdown = CancellationToken::new();

    let ctrlc_shutdown = shutdown.clone();
    ctrlc::set_handler(move || ctrlc_shutdown.cancel()).expect("failed to set ctrl-c handler");

    let args = Args::parse();

    let server_shutdown = shutdown.clone();
    let serving = tokio::spawn(async move {
        server::serve(b, server_shutdown).await
    });

    let consumer_shutdown = shutdown.clone();
    let consuming = tokio::spawn(async move {
        consumer::consume(consumer_sender, args.jetstream, None, args.jetstream_no_zstd, consumer_shutdown).await
    });

    let (served, consumed) = tokio::join!(serving, consuming);
    log::info!("serving ended: {served:?}");
    log::info!("consuming ended: {consumed:?}");

    log::info!("bye!");

    Ok(())
}
