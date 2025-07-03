use atrium_api::types::string::Did;
use axum::{
    Router,
    extract::{FromRef, Query, State},
    http::{
        StatusCode,
        header::{CONTENT_SECURITY_POLICY, CONTENT_TYPE, HeaderMap, REFERER, X_FRAME_OPTIONS},
    },
    response::{IntoResponse, Json, Redirect, Response},
    routing::{get, post},
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

use crate::{
    ExpiringTaskMap, OAuth, OAuthCallbackParams, OAuthCompleteError, ResolveHandleError, Tokens,
};

const FAVICON: &[u8] = include_bytes!("../static/favicon.ico");
const STYLE_CSS: &str = include_str!("../static/style.css");

const DID_COOKIE_KEY: &str = "did";

const COOKIE_EXPIRATION: Duration = Duration::from_secs(30 * 86_400);

type AppEngine = Engine<Handlebars<'static>>;

#[derive(Clone)]
struct AppState {
    pub key: Key,
    pub allowed_hosts: Arc<HashSet<String>>,
    pub engine: AppEngine,
    pub oauth: Arc<OAuth>,
    pub resolve_handles: ExpiringTaskMap<Result<String, ResolveHandleError>>,
    pub shutdown: CancellationToken,
    pub tokens: Arc<Tokens>,
}

impl FromRef<AppState> for Key {
    fn from_ref(state: &AppState) -> Self {
        state.key.clone()
    }
}

pub async fn serve(
    shutdown: CancellationToken,
    app_secret: String,
    tokens: Tokens,
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
        tokens: Arc::new(tokens),
    };

    let app = Router::new()
        .route("/", get(hello))
        .route("/favicon.ico", get(favicon)) // todo MIME
        .route("/style.css", get(css))
        .route("/prompt", get(prompt))
        .route("/user-info", get(user_info))
        .route("/auth", get(start_oauth))
        .route("/authorized", get(complete_oauth))
        .route("/disconnect", post(disconnect))
        .with_state(state);

    let listener = TcpListener::bind("0.0.0.0:9997")
        .await
        .expect("listener binding to work");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
        .unwrap();
}

async fn hello(
    State(AppState {
        engine,
        resolve_handles,
        shutdown,
        oauth,
        ..
    }): State<AppState>,
    mut jar: SignedCookieJar,
) -> Response {
    let info = if let Some(did) = jar.get(DID_COOKIE_KEY) {
        if let Ok(did) = Did::new(did.value_trimmed().to_string()) {
            // push cookie expiry
            jar = jar.add(cookie(&did));
            let fetch_key = resolve_handles.dispatch(
                {
                    let oauth = oauth.clone();
                    let did = did.clone();
                    async move { oauth.resolve_handle(did.clone()).await }
                },
                shutdown.child_token(),
            );
            json!({
                "did": did,
                "fetch_key": fetch_key,
            })
        } else {
            jar = jar.remove(DID_COOKIE_KEY);
            json!({})
        }
    } else {
        json!({})
    };
    let frame_headers = [
        (X_FRAME_OPTIONS, "deny"),
        (CONTENT_SECURITY_POLICY, "frame-ancestors 'none'"),
    ];
    (frame_headers, jar, RenderHtml("hello", engine, info)).into_response()
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

fn cookie(did: &Did) -> Cookie<'static> {
    Cookie::build((DID_COOKIE_KEY, did.to_string()))
        .http_only(true)
        .secure(true)
        .same_site(SameSite::None)
        .max_age(COOKIE_EXPIRATION.try_into().unwrap())
        .into()
}

async fn prompt(
    State(AppState {
        allowed_hosts,
        engine,
        oauth,
        resolve_handles,
        shutdown,
        tokens,
        ..
    }): State<AppState>,
    jar: SignedCookieJar,
    headers: HeaderMap,
) -> impl IntoResponse {
    let err = |reason, check_frame| {
        metrics::counter!("whoami_auth_prompt", "ok" => "false", "reason" => reason).increment(1);
        let info = json!({ "reason": reason, "check_frame": check_frame });
        let html = RenderHtml("prompt-error", engine.clone(), info);
        (StatusCode::BAD_REQUEST, html).into_response()
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
    let parent_origin = url.origin().ascii_serialization();
    if parent_origin == "null" {
        return err("Referer origin is opaque", true);
    }

    let frame_headers = [
        (X_FRAME_OPTIONS, format!("allow-from {parent_origin}")),
        (
            CONTENT_SECURITY_POLICY,
            format!("frame-ancestors {parent_origin}"),
        ),
    ];

    if let Some(did) = jar.get(DID_COOKIE_KEY) {
        let Ok(did) = Did::new(did.value_trimmed().to_string()) else {
            return err("Bad cookie", false);
        };

        // push cookie expiry
        let jar = jar.add(cookie(&did));

        let token = match tokens.mint(&*did) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("failed to create JWT: {e:?}");
                return err("failed to create JWT", false);
            }
        };

        let fetch_key = resolve_handles.dispatch(
            {
                let oauth = oauth.clone();
                let did = did.clone();
                async move { oauth.resolve_handle(did.clone()).await }
            },
            shutdown.child_token(),
        );

        metrics::counter!("whoami_auth_prompt", "ok" => "true", "known" => "true").increment(1);
        let info = json!({
            "did": did,
            "token": token,
            "fetch_key": fetch_key,
            "parent_host": parent_host,
            "parent_origin": parent_origin,
        });
        (frame_headers, jar, RenderHtml("prompt", engine, info)).into_response()
    } else {
        metrics::counter!("whoami_auth_prompt", "ok" => "true", "known" => "false").increment(1);
        let info = json!({
            "parent_host": parent_host,
            "parent_origin": parent_origin,
        });
        (frame_headers, RenderHtml("prompt", engine, info)).into_response()
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
    let err = |status, reason: &str| {
        metrics::counter!("whoami_user_info", "found" => "false", "reason" => reason.to_string())
            .increment(1);
        (status, Json(json!({ "reason": reason }))).into_response()
    };

    let Some(task_handle) = resolve_handles.take(&params.fetch_key) else {
        return err(StatusCode::NOT_FOUND, "fetch key does not exist or expired");
    };

    match task_handle.await {
        Err(task_err) => {
            eprintln!("task join error? {task_err:?}");
            err(StatusCode::INTERNAL_SERVER_ERROR, "server errored")
        }
        Ok(Err(ResolveHandleError::ResolutionFailed(atrium_identity::Error::NotFound))) => {
            err(StatusCode::NOT_FOUND, "handle not found")
        }
        Ok(Err(ResolveHandleError::ResolutionFailed(e))) => {
            eprintln!("handle resolution failed: {e:?}");
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "handle resolution failed",
            )
        }
        Ok(Err(ResolveHandleError::NoHandle)) => err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "resolved identity but did not find a handle",
        ),
        Ok(Err(ResolveHandleError::InvalidHandle(_h, reason))) => err(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("handle appears invalid: {reason}"),
        ),
        Ok(Ok(handle)) => {
            metrics::counter!("whoami_user_info", "found" => "true").increment(1);
            Json(json!({ "handle": handle })).into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
struct BeginOauthParams {
    handle: String,
}
async fn start_oauth(
    State(AppState { oauth, engine, .. }): State<AppState>,
    Query(params): Query<BeginOauthParams>,
    jar: SignedCookieJar,
) -> Response {
    // if any existing session was active, clear it first
    // ...this might help a confusion attack w multiple sign-in flows or smth
    let jar = jar.remove(DID_COOKIE_KEY);

    use atrium_identity::Error as IdError;
    use atrium_oauth::Error as OAuthError;

    let err = |code, reason: &str| {
        metrics::counter!("whoami_auth_start", "ok" => "false", "reason" => reason.to_string())
            .increment(1);
        let info = json!({
            "result": "fail",
            "reason": reason,
        });
        (code, RenderHtml("auth-fail", engine.clone(), info)).into_response()
    };

    match oauth.begin(&params.handle).await {
        Err(OAuthError::Identity(
            IdError::NotFound | IdError::HttpStatus(StatusCode::NOT_FOUND),
        )) => err(StatusCode::NOT_FOUND, "handle not found"),
        Err(OAuthError::Identity(IdError::AtIdentifier(r))) => err(StatusCode::BAD_REQUEST, &r),
        Err(e) => {
            eprintln!("begin auth failed: {e:?}");
            err(StatusCode::INTERNAL_SERVER_ERROR, "unknown")
        }
        Ok(auth_url) => {
            metrics::counter!("whoami_auth_start", "ok" => "true").increment(1);
            (jar, Redirect::to(&auth_url)).into_response()
        }
    }
}

async fn complete_oauth(
    State(AppState {
        engine,
        resolve_handles,
        oauth,
        shutdown,
        tokens,
        ..
    }): State<AppState>,
    Query(params): Query<OAuthCallbackParams>,
    jar: SignedCookieJar,
) -> Response {
    let err = |code, result, reason: &str| {
        metrics::counter!("whoami_auth_complete", "ok" => "false", "reason" => reason.to_string())
            .increment(1);
        let info = json!({
            "result": result,
            "reason": reason,
        });
        (code, RenderHtml("auth-fail", engine.clone(), info)).into_response()
    };

    let did = match oauth.complete(params).await {
        Ok(did) => did,
        Err(e) => {
            return match e {
                OAuthCompleteError::Denied { description, .. } => {
                    let desc = description.unwrap_or("permission to share was denied".to_string());
                    err(StatusCode::FORBIDDEN, "deny", desc.as_str())
                }
                OAuthCompleteError::Failed { .. } => {
                    eprintln!("auth completion failed: {e:?}");
                    err(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "fail",
                        "failed to complete",
                    )
                }
                OAuthCompleteError::CallbackFailed(e) => {
                    eprintln!("auth callback failed: {e:?}");
                    err(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "fail",
                        "failed to complete callback",
                    )
                }
                OAuthCompleteError::NoDid => err(StatusCode::BAD_REQUEST, "fail", "no DID found"),
            };
        }
    };

    let jar = jar.add(cookie(&did));

    let token = match tokens.mint(&*did) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("failed to create JWT: {e:?}");
            return err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "fail",
                "failed to create JWT",
            );
        }
    };

    let fetch_key = resolve_handles.dispatch(
        {
            let oauth = oauth.clone();
            let did = did.clone();
            async move { oauth.resolve_handle(did.clone()).await }
        },
        shutdown.child_token(),
    );

    metrics::counter!("whoami_auth_complete", "ok" => "true").increment(1);
    let info = json!({
        "did": did,
        "token": token,
        "fetch_key": fetch_key,
    });
    (jar, RenderHtml("authorized", engine, info)).into_response()
}

async fn disconnect(jar: SignedCookieJar) -> impl IntoResponse {
    metrics::counter!("whoami_disconnect").increment(1);
    let jar = jar.remove(DID_COOKIE_KEY);
    (jar, Json(json!({ "ok": true })))
}
