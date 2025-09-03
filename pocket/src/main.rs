use pocket::serve;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    println!("Hello, world!");
    serve("mac.cinnebar-tet.ts.net").await
}

