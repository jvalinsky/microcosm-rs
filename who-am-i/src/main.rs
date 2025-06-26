use clap::Parser;
use tokio_util::sync::CancellationToken;
use who_am_i::serve;

/// Aggregate links in the at-mosphere
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    /// secret key from which the cookie-signing key is derived
    ///
    /// must have at least 512 bits (64 bytes) of randomness
    ///
    /// eg: `cat /dev/urandom | head -c 64 | base64`
    #[arg(long)]
    app_secret: String,
    /// Enable dev mode
    ///
    /// enables automatic template reloading
    #[arg(long, action)]
    dev: bool,
}

#[tokio::main]
async fn main() {
    let shutdown = CancellationToken::new();

    let ctrlc_shutdown = shutdown.clone();
    ctrlc::set_handler(move || ctrlc_shutdown.cancel()).expect("failed to set ctrl-c handler");

    let args = Args::parse();

    serve(shutdown, args.app_secret, args.dev).await;
}
