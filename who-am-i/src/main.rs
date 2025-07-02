use clap::{ArgAction, Parser};
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
    #[arg(long, env)]
    app_secret: String,
    /// Enable dev mode
    ///
    /// enables automatic template reloading
    #[arg(long, action)]
    dev: bool,
    /// Hosts who are allowed to one-click auth
    ///
    /// Pass this argument multiple times to allow multiple hosts
    #[arg(long = "allow_host", short = 'a', action = ArgAction::Append)]
    allowed_hosts: Vec<String>,
}

#[tokio::main]
async fn main() {
    let shutdown = CancellationToken::new();

    let ctrlc_shutdown = shutdown.clone();
    ctrlc::set_handler(move || ctrlc_shutdown.cancel()).expect("failed to set ctrl-c handler");

    let args = Args::parse();

    if args.allowed_hosts.is_empty() {
        panic!("at least one --one-click host must be set");
    }

    println!("starting with allowed_hosts hosts:");
    for host in &args.allowed_hosts {
        println!(" - {host}");
    }

    serve(shutdown, args.app_secret, args.allowed_hosts, args.dev).await;
}
