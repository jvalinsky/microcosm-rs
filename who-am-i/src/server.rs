use atrium_api::agent::SessionManager;
use atrium_oauth::CallbackParams;
use axum::{
    Router,
    extract::{Query, State},
    response::{Html, Redirect},
    routing::get,
};

use serde::Deserialize;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::{Client, authorize, client};

const INDEX_HTML: &str = include_str!("../static/index.html");
const FAVICON: &[u8] = include_bytes!("../static/favicon.ico");

pub async fn serve(shutdown: CancellationToken) {
    let state = AppState {
        client: Arc::new(client()),
    };

    let app = Router::new()
        .route("/", get(|| async { Html(INDEX_HTML) }))
        .route("/favicon.ico", get(|| async { FAVICON })) // todo MIME
        .route("/auth", get(start_oauth))
        .route("/authorized", get(complete_oauth))
        .with_state(state);

    let listener = TcpListener::bind("0.0.0.0:9997")
        .await
        .expect("listener binding to work");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
        .unwrap();
}

#[derive(Clone)]
struct AppState {
    pub client: Arc<Client>,
}

#[derive(Debug, Deserialize)]
struct BeginOauthParams {
    handle: String,
}
async fn start_oauth(
    State(state): State<AppState>,
    Query(params): Query<BeginOauthParams>,
) -> Redirect {
    let AppState { client } = state;
    let BeginOauthParams { handle } = params;
    let auth_url = authorize(&client, &handle).await;
    Redirect::to(&auth_url)
}

async fn complete_oauth(
    State(state): State<AppState>,
    Query(params): Query<CallbackParams>,
) -> Html<String> {
    let AppState { client } = state;
    let Ok((oauth_session, _)) = client.callback(params).await else {
        panic!("failed to do client callback");
    };
    let did = oauth_session.did().await.expect("a did to be present");
    Html(format!("sup: {did:?}"))
}
