use atrium_api::types::string::Did;
use axum::{
    Router,
    extract::{FromRef, Query, State},
    http::{
        StatusCode,
        header::{CONTENT_TYPE, HeaderMap, REFERER},
    },
    response::{IntoResponse, Json, Redirect, Response},
    routing::get,
};
use axum_extra::extract::cookie::{Cookie, Key, SameSite, SignedCookieJar};
use axum_template::{RenderHtml, engine::Engine};
use handlebars::{Handlebars, handlebars_helper};

use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{ExpiringTaskMap, OAuth, OAuthCallbackParams, OAuthCompleteError, ResolveHandleError};

const FAVICON: &[u8] = include_bytes!("../static/favicon.ico");
const STYLE_CSS: &str = include_str!("../static/style.css");

const DID_COOKIE_KEY: &str = "did";

type AppEngine = Engine<Handlebars<'static>>;
type Rendered = RenderHtml<&'static str, AppEngine, Value>;

#[derive(Clone)]
struct AppState {
    pub key: Key,
    pub allowed_hosts: Arc<HashSet<String>>,
    pub engine: AppEngine,
    pub oauth: Arc<OAuth>,
    pub resolve_handles: ExpiringTaskMap<Result<String, ResolveHandleError>>,
    pub shutdown: CancellationToken,
}

impl FromRef<AppState> for Key {
    fn from_ref(state: &AppState) -> Self {
        state.key.clone()
    }
}

pub async fn serve(
    shutdown: CancellationToken,
    app_secret: String,
    allowed_hosts: Vec<String>,
    dev: bool,
) {
    let mut hbs = Handlebars::new();
    hbs.set_dev_mode(dev);
    hbs.register_templates_directory("templates", Default::default())
        .unwrap();

    handlebars_helper!(json: |v: Value| serde_json::to_string(&v).unwrap());
    hbs.register_helper("json", Box::new(json));

    // clients have to pick up their identity-resolving tasks within this period
    let task_pickup_expiration = Duration::from_secs(15);

    let oauth = OAuth::new().unwrap();

    let state = AppState {
        engine: Engine::new(hbs),
        key: Key::from(app_secret.as_bytes()), // TODO: via config
        allowed_hosts: Arc::new(HashSet::from_iter(allowed_hosts)),
        oauth: Arc::new(oauth),
        resolve_handles: ExpiringTaskMap::new(task_pickup_expiration),
        shutdown: shutdown.clone(),
    };

    let app = Router::new()
        .route("/", get(hello))
        .route("/favicon.ico", get(favicon)) // todo MIME
        .route("/style.css", get(css))
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

async fn hello(State(AppState { engine, .. }): State<AppState>) -> Rendered {
    RenderHtml("hello", engine, json!({}))
}

async fn css() -> impl IntoResponse {
    let headers = [
        (CONTENT_TYPE, "text/css"),
        // (CACHE_CONTROL, "") // TODO
    ];
    (headers, STYLE_CSS)
}

async fn favicon() -> impl IntoResponse {
    ([(CONTENT_TYPE, "image/x-icon")], FAVICON)
}

async fn prompt(
    State(AppState {
        allowed_hosts,
        engine,
        oauth,
        resolve_handles,
        shutdown,
        ..
    }): State<AppState>,
    jar: SignedCookieJar,
    headers: HeaderMap,
) -> impl IntoResponse {
    let err = |reason, check_frame| {
        let info = json!({
            "reason": reason,
            "check_frame": check_frame,
        });
        RenderHtml("prompt-error", engine.clone(), info).into_response()
    };

    let Some(referrer) = headers.get(REFERER) else {
        return err("Missing referer", true);
    };
    let Ok(referrer) = referrer.to_str() else {
        return err("Unreadable referer", true);
    };
    let Ok(url) = Url::parse(referrer) else {
        return err("Bad referer", true);
    };
    let Some(parent_host) = url.host_str() else {
        return err("Referer missing host", true);
    };
    if !allowed_hosts.contains(parent_host) {
        return err("Login is not allowed on this page", false);
    }
    if let Some(did) = jar.get(DID_COOKIE_KEY) {
        let Ok(did) = Did::new(did.value_trimmed().to_string()) else {
            return err("Bad cookie", false);
        };

        let fetch_key = resolve_handles.dispatch(
            {
                let oauth = oauth.clone();
                let did = did.clone();
                async move { oauth.resolve_handle(did.clone()).await }
            },
            shutdown.child_token(),
        );

        RenderHtml(
            "prompt-known",
            engine,
            json!({
                "did": did,
                "fetch_key": fetch_key,
                "parent_host": parent_host,
            }),
        )
        .into_response()
    } else {
        RenderHtml(
            "prompt-anon",
            engine,
            json!({
                "parent_host": parent_host,
            }),
        )
        .into_response()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct UserInfoParams {
    fetch_key: String,
}
async fn user_info(
    State(AppState {
        resolve_handles, ..
    }): State<AppState>,
    Query(params): Query<UserInfoParams>,
) -> impl IntoResponse {
    let Some(task_handle) = resolve_handles.take(&params.fetch_key) else {
        return "oops, task does not exist or is gone".into_response();
    };
    if let Ok(handle) = task_handle.await.unwrap() {
        Json(json!({ "handle": handle })).into_response()
    } else {
        "no handle?".into_response()
    }
}

#[derive(Debug, Deserialize)]
struct BeginOauthParams {
    handle: String,
    flow: String,
}
async fn start_oauth(
    State(AppState { oauth, .. }): State<AppState>,
    Query(params): Query<BeginOauthParams>,
    jar: SignedCookieJar,
    headers: HeaderMap,
) -> (SignedCookieJar, Redirect) {
    // if any existing session was active, clear it first
    let jar = jar.remove(DID_COOKIE_KEY);

    if let Some(referrer) = headers.get(REFERER) {
        if let Ok(referrer) = referrer.to_str() {
            println!("referrer: {referrer}");
        } else {
            eprintln!("referer contained opaque bytes");
        };
    } else {
        eprintln!("no referrer");
    };

    let auth_url = oauth.begin(&params.handle).await.unwrap();
    let flow = params.flow;
    if !flow.chars().all(|c| char::is_ascii_alphanumeric(&c)) {
        panic!("invalid flow (injection attempt?)"); // should probably just url-encode it instead..
    }
    eprintln!("auth_url {auth_url}");

    (jar, Redirect::to(&auth_url))
}

impl OAuthCompleteError {
    fn to_error_response(&self, engine: AppEngine) -> Response {
        let (level, desc) = match self {
            OAuthCompleteError::Denied { description, .. } => {
                ("warn", format!("asdf: {description:?}"))
            }
            OAuthCompleteError::Failed { .. } => (
                "error",
                "Something went wrong while requesting permission, sorry!".to_string(),
            ),
            OAuthCompleteError::CallbackFailed(_) => (
                "error",
                "Something went wrong after permission was granted, sorry!".to_string(),
            ),
            OAuthCompleteError::NoDid => (
                "error",
                "Something went wrong when trying to confirm your identity, sorry!".to_string(),
            ),
        };
        (
            if level == "warn" {
                StatusCode::FORBIDDEN
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            },
            RenderHtml(
                "auth-fail",
                engine,
                json!({
                    "reason": desc,
                }),
            ),
        )
            .into_response()
    }
}

async fn complete_oauth(
    State(AppState {
        engine,
        resolve_handles,
        oauth,
        shutdown,
        ..
    }): State<AppState>,
    Query(params): Query<OAuthCallbackParams>,
    jar: SignedCookieJar,
) -> Result<(SignedCookieJar, impl IntoResponse), Response> {
    let did = match oauth.complete(params).await {
        Ok(did) => did,
        Err(e) => return Err(e.to_error_response(engine)),
    };

    let cookie = Cookie::build((DID_COOKIE_KEY, did.to_string()))
        .http_only(true)
        .secure(true)
        .same_site(SameSite::None)
        .max_age(std::time::Duration::from_secs(86_400).try_into().unwrap());

    let jar = jar.add(cookie);

    let fetch_key = resolve_handles.dispatch(
        {
            let oauth = oauth.clone();
            let did = did.clone();
            async move { oauth.resolve_handle(did.clone()).await }
        },
        shutdown.child_token(),
    );

    Ok((
        jar,
        RenderHtml(
            "authorized",
            engine,
            json!({
                "did": did,
                "fetch_key": fetch_key,
            }),
        ),
    ))
}
