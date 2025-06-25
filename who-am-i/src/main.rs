use tokio_util::sync::CancellationToken;
use who_am_i::serve;

#[tokio::main]
async fn main() {
    env_logger::init();

    let server_shutdown = CancellationToken::new();
    serve(server_shutdown).await.unwrap();
}
