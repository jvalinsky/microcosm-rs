use clap::Parser;
use poem::{
    EndpointExt, Route, Server, get, handler,
    listener::TcpListener,
    middleware::{AddData, Tracing},
    web::{Data, Json, TypedHeader, headers::Host},
};
use serde::Serialize;

#[handler]
fn hello() -> String {
    "ɹoʇɔǝʅⅎǝɹ".to_string()
}

#[derive(Debug, Serialize)]
struct DidDoc {
    id: String,
    service: [DidService; 1],
}

#[derive(Debug, Clone, Serialize)]
struct DidService {
    id: String,
    r#type: String,
    service_endpoint: String,
}

#[handler]
fn did_doc(TypedHeader(host): TypedHeader<Host>, service: Data<&DidService>) -> Json<DidDoc> {
    Json(DidDoc {
        id: format!("did:web:{}", host.hostname()),
        service: [service.clone()],
    })
}

/// Slingshot record edge cache
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    /// The DID document service ID to serve
    ///
    /// must start with a '#', like `#bsky_appview'
    #[arg(long)]
    id: String,
    /// Service type
    ///
    /// Not sure exactly what its requirements are. 'BlueskyAppview' for example
    #[arg(long)]
    r#type: String,
    /// The HTTPS endpoint for the service
    #[arg(long)]
    service_endpoint: String,
}

impl From<Args> for DidService {
    fn from(a: Args) -> Self {
        Self {
            id: a.id,
            r#type: a.r#type,
            service_endpoint: a.service_endpoint,
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt::init();
    log::info!("ɹoʇɔǝʅⅎǝɹ");

    let args = Args::parse();
    let service: DidService = args.into();

    Server::new(TcpListener::bind("0.0.0.0:3001"))
        .run(
            Route::new()
                .at("/", get(hello))
                .at("/.well-known/did.json", get(did_doc))
                .with(AddData::new(service))
                .with(Tracing),
        )
        .await
        .unwrap()
}
