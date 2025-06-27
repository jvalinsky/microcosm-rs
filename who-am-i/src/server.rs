use atrium_api::agent::SessionManager;
use atrium_oauth::CallbackParams;
use axum::{
    Router,
    extract::{FromRef, Query, State},
    http::header::{HeaderMap, REFERER},
    response::{Html, IntoResponse, Json, Redirect},
    routing::get,
};
use axum_extra::extract::cookie::{Cookie, Key, SameSite, SignedCookieJar};
use axum_template::{RenderHtml, engine::Engine};
use handlebars::{Handlebars, handlebars_helper};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{Client, ExpiringTaskMap, authorize, client, resolve_identity};

const FAVICON: &[u8] = include_bytes!("../static/favicon.ico");
const INDEX_HTML: &str = include_str!("../static/index.html");
const LOGIN_HTML: &str = include_str!("../static/login.html");

const DID_COOKIE_KEY: &str = "did";

type AppEngine = Engine<Handlebars<'static>>;

#[derive(Clone)]
struct AppState {
    pub key: Key,
    pub engine: AppEngine,
    pub client: Arc<Client>,
    pub resolving: ExpiringTaskMap<Option<String>>,
    pub shutdown: CancellationToken,
}

impl FromRef<AppState> for Key {
    fn from_ref(state: &AppState) -> Self {
        state.key.clone()
    }
}

pub async fn serve(shutdown: CancellationToken, app_secret: String, dev: bool) {
    let mut hbs = Handlebars::new();
    hbs.set_dev_mode(dev);
    hbs.register_templates_directory("templates", Default::default())
        .unwrap();

    handlebars_helper!(json: |v: Value| serde_json::to_string(&v).unwrap());
    hbs.register_helper("json", Box::new(json));

    // clients have to pick up their identity-resolving tasks within this period
    let task_pickup_expiration = Duration::from_secs(15);

    let state = AppState {
        engine: Engine::new(hbs),
        key: Key::from(app_secret.as_bytes()), // TODO: via config
        client: Arc::new(client()),
        resolving: ExpiringTaskMap::new(task_pickup_expiration),
        shutdown: shutdown.clone(),
    };

    let app = Router::new()
        .route("/", get(|| async { Html(INDEX_HTML) }))
        .route("/favicon.ico", get(|| async { FAVICON })) // todo MIME
        .route("/prompt", get(prompt))
        .route("/user-info", get(user_info))
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

#[derive(Debug, Serialize)]
struct Known {
    did: Value,
    fetch_key: Value,
    parent_host: String,
}
async fn prompt(
    State(AppState {
        engine,
        resolving,
        shutdown,
        ..
    }): State<AppState>,
    jar: SignedCookieJar,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(referrer) = headers.get(REFERER) else {
        return Html::<&'static str>("missing referrer, sorry").into_response();
    };
    let Ok(referrer) = referrer.to_str() else {
        return "referer contained opaque bytes".into_response();
    };
    let Ok(url) = Url::parse(referrer) else {
        return "referrer was not a url".into_response();
    };
    let Some(parent_host) = url.host_str() else {
        return "could nto get host from url".into_response();
    };
    let m = if let Some(did) = jar.get(DID_COOKIE_KEY) {
        let did = did.value_trimmed().to_string();

        let task_shutdown = shutdown.child_token();
        let fetch_key = resolving.dispatch(resolve_identity(did.clone()), task_shutdown);

        let json_did = Value::String(did);
        let json_fetch_key = Value::String(fetch_key);
        let known = Known {
            did: json_did,
            fetch_key: json_fetch_key,
            parent_host: parent_host.to_string(),
        };
        return (jar, RenderHtml("prompt-known", engine, known)).into_response();
    } else {
        LOGIN_HTML.into_response()
    };
    (jar, Html(m)).into_response()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct UserInfoParams {
    fetch_key: String,
}
async fn user_info(
    State(AppState { resolving, .. }): State<AppState>,
    Query(params): Query<UserInfoParams>,
) -> impl IntoResponse {
    let Some(task_handle) = resolving.take(&params.fetch_key) else {
        return "oops, task does not exist or is gone".into_response();
    };
    if let Some(handle) = task_handle.await.unwrap() {
        // TODO: get active state etc.
        // ...but also, that's a bsky thing?
        let Some(handle) = handle.strip_prefix("at://") else {
            return "hmm, handle did not start with at://".into_response();
        };
        Json(json!({ "handle": handle })).into_response()
    } else {
        "no handle?".into_response()
    }
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
    let jar = jar.remove(DID_COOKIE_KEY);

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
    let cookie = Cookie::build((DID_COOKIE_KEY, did.to_string()))
        .http_only(true)
        .secure(true)
        .same_site(SameSite::None)
        .max_age(std::time::Duration::from_secs(86_400).try_into().unwrap());
    let jar = jar.add(cookie);
    (jar, Html(format!("sup: {did:?}")))
}
