use atrium_api::agent::SessionManager;
use atrium_oauth::CallbackParams;
use axum::{
    Router,
    extract::{FromRef, Query, State},
    response::{Html, IntoResponse, Redirect},
    routing::get,
};
use axum_extra::extract::cookie::{Cookie, Key, SameSite, SignedCookieJar};

use serde::Deserialize;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::{Client, authorize, client};

const FAVICON: &[u8] = include_bytes!("../static/favicon.ico");
const INDEX_HTML: &str = include_str!("../static/index.html");
const LOGIN_HTML: &str = include_str!("../static/login.html");

pub async fn serve(shutdown: CancellationToken) {
    let state = AppState {
        key: Key::generate(), // TODO: via config
        client: Arc::new(client()),
    };

    let app = Router::new()
        .route("/", get(|| async { Html(INDEX_HTML) }))
        .route("/favicon.ico", get(|| async { FAVICON })) // todo MIME
        .route("/prompt", get(prompt))
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
    pub key: Key,
    pub client: Arc<Client>,
}

impl FromRef<AppState> for Key {
    fn from_ref(state: &AppState) -> Self {
        state.key.clone()
    }
}

async fn prompt(jar: SignedCookieJar) -> impl IntoResponse {
    let m = if let Some(did) = jar.get("did") {
        format!("oh i know you: {did}")
    } else {
        LOGIN_HTML.into()
    };
    (jar, Html(m))
}

#[derive(Debug, Deserialize)]
struct BeginOauthParams {
    handle: String,
}
async fn start_oauth(
    State(state): State<AppState>,
    Query(params): Query<BeginOauthParams>,
    jar: SignedCookieJar,
) -> (SignedCookieJar, Redirect) {
    // if any existing session was active, clear it first
    let jar = jar.remove("did");

    let auth_url = authorize(&state.client, &params.handle).await;
    (jar, Redirect::to(&auth_url))
}

async fn complete_oauth(
    State(state): State<AppState>,
    Query(params): Query<CallbackParams>,
    jar: SignedCookieJar,
) -> (SignedCookieJar, Html<String>) {
    let Ok((oauth_session, _)) = state.client.callback(params).await else {
        panic!("failed to do client callback");
    };
    let did = oauth_session.did().await.expect("a did to be present");
    let cookie = Cookie::build(("did", did.to_string()))
        .http_only(true)
        .secure(true)
        .same_site(SameSite::None)
        .max_age(std::time::Duration::from_secs(86_400).try_into().unwrap());
    let jar = jar.add(cookie);
    (jar, Html(format!("sup: {did:?}")))
}
