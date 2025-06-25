
use atrium_api::agent::SessionManager;
use std::error::Error;
use metrics::{histogram, counter};
use std::sync::Arc;
use http::{
    header::{ORIGIN, USER_AGENT},
    Response, StatusCode,
};
use dropshot::{
    Body, HttpResponseSeeOther, http_response_see_other,
    ApiDescription, ConfigDropshot, ConfigLogging, ConfigLoggingLevel, RequestContext,
    ServerBuilder, endpoint, HttpResponse, HttpError, ServerContext, Query,
};

use atrium_oauth::CallbackParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{Client, client, authorize};

const INDEX_HTML: &str = include_str!("../static/index.html");
const FAVICON: &[u8] = include_bytes!("../static/favicon.ico");

pub async fn serve(
    shutdown: CancellationToken
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let config_logging = ConfigLogging::StderrTerminal {
        level: ConfigLoggingLevel::Info,
    };

    let log = config_logging
        .to_logger("example-basic")?;

    let mut api = ApiDescription::new();
    api.register(index).unwrap();
    api.register(favicon).unwrap();
    api.register(openapi).unwrap();
    api.register(start_oauth).unwrap();
    api.register(finish_oauth).unwrap();

    // TODO: put spec in a once cell / lazy lock thing?
    let spec = Arc::new(
        api.openapi(
            "Who-am-i",
            env!("CARGO_PKG_VERSION")
                .parse()
                .inspect_err(|e| {
                    eprintln!("failed to parse cargo package version for openapi: {e:?}")
                })
                .unwrap_or(semver::Version::new(0, 0, 1)),
        )
        .description("An atproto identity verifier that is very much not ready for real use")
        .contact_name("part of @microcosm.blue")
        .contact_url("https://microcosm.blue")
        .json()?,
    );

    let ctx = Context { spec, client: client().into() };

    let server = ServerBuilder::new(api, ctx, log)
        .config(ConfigDropshot {
            bind_address: "0.0.0.0:9997".parse().unwrap(),
            ..Default::default()
        })
        .start()?;

    tokio::select! {
        s = server.wait_for_shutdown() => {
            s?;
            log::info!("server shut down normally.");
        },
        _ = shutdown.cancelled() => {
            log::info!("shutting down: closing server");
            server.close().await?;
        },
    }
    Ok(())
}

#[derive(Clone)]
struct Context {
    pub spec: Arc<serde_json::Value>,
    pub client: Arc<Client>,
}

async fn instrument_handler<T, H, R>(ctx: &RequestContext<T>, handler: H) -> Result<R, HttpError>
where
    R: HttpResponse,
    H: Future<Output = Result<R, HttpError>>,
    T: ServerContext,
{
    let start = Instant::now();
    let result = handler.await;
    let latency = start.elapsed();
    let status_code = match &result {
        Ok(response) => response.status_code(),
        Err(e) => e.status_code.as_status(),
    }
    .as_str() // just the number (.to_string()'s Display does eg `200 OK`)
    .to_string();
    let endpoint = ctx.endpoint.operation_id.clone();
    let headers = ctx.request.headers();
    let origin = headers
        .get(ORIGIN)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let ua = headers
        .get(USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .map(|ua| {
            if ua.starts_with("Mozilla/5.0 ") {
                "browser"
            } else {
                ua
            }
        })
        .unwrap_or("")
        .to_string();
    counter!("server_requests_total",
        "endpoint" => endpoint.clone(),
        "origin" => origin,
        "ua" => ua,
        "status_code" => status_code,
    )
    .increment(1);
    histogram!("server_handler_latency", "endpoint" => endpoint).record(latency.as_micros() as f64);
    result
}

use dropshot::{HttpResponseHeaders, HttpResponseOk};

pub type OkCorsResponse<T> = Result<HttpResponseHeaders<HttpResponseOk<T>>, HttpError>;

/// Helper for constructing Ok responses: return OkCors(T).into()
/// (not happy with this yet)
pub struct OkCors<T: Serialize + JsonSchema + Send + Sync>(pub T);

impl<T> From<OkCors<T>> for OkCorsResponse<T>
where
    T: Serialize + JsonSchema + Send + Sync,
{
    fn from(ok: OkCors<T>) -> OkCorsResponse<T> {
        let mut res = HttpResponseHeaders::new_unnamed(HttpResponseOk(ok.0));
        res.headers_mut()
            .insert("access-control-allow-origin", "*".parse().unwrap());
        Ok(res)
    }
}

// TODO: cors for HttpError


/// Serve index page as html
#[endpoint {
    method = GET,
    path = "/",
    /*
     * not useful to have this in openapi
     */
    unpublished = true,
}]
async fn index(ctx: RequestContext<Context>) -> Result<Response<Body>, HttpError> {
    instrument_handler(&ctx, async {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "text/html")
            .body(INDEX_HTML.into())?)
    })
    .await
}

/// Serve index page as html
#[endpoint {
    method = GET,
    path = "/favicon.ico",
    /*
     * not useful to have this in openapi
     */
    unpublished = true,
}]
async fn favicon(ctx: RequestContext<Context>) -> Result<Response<Body>, HttpError> {
    instrument_handler(&ctx, async {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "image/x-icon")
            .body(FAVICON.to_vec().into())?)
    })
    .await
}

/// Meta: get the openapi spec for this api
#[endpoint {
    method = GET,
    path = "/openapi",
    /*
     * not useful to have this in openapi
     */
    unpublished = true,
}]
async fn openapi(ctx: RequestContext<Context>) -> OkCorsResponse<serde_json::Value> {
    instrument_handler(&ctx, async {
        let spec = (*ctx.context().spec).clone();
        OkCors(spec).into()
    })
    .await
}

#[derive(Debug, Deserialize, JsonSchema)]
struct BeginOauthQuery {
    handle: String,
}
#[endpoint {
    method = GET,
    path = "/auth",
}]
async fn start_oauth(
    ctx: RequestContext<Context>,
    query: Query<BeginOauthQuery>,
) -> Result<HttpResponseSeeOther, HttpError> {
    let BeginOauthQuery { handle } = query.into_inner();

    instrument_handler(&ctx, async {
        let Context { client, .. } = ctx.context();

        let auth_url = authorize(client, &handle).await;

        http_response_see_other(auth_url)
    })
    .await
}

#[derive(Debug, Deserialize, JsonSchema)]
struct AuthorizedCallbackQuery {
    code: String,
    state: Option<String>,
    iss: Option<String>,
}
impl From<AuthorizedCallbackQuery> for CallbackParams {
    fn from(q: AuthorizedCallbackQuery) -> Self {
        let AuthorizedCallbackQuery { code, state, iss } = q;
        Self { code, state, iss }
    }
}
#[endpoint {
    method = GET,
    path = "/authorized",
}]
async fn finish_oauth(
    ctx: RequestContext<Context>,
    query: Query<AuthorizedCallbackQuery>,
) -> Result<Response<Body>, HttpError> {
    instrument_handler(&ctx, async {
        let Context { client, .. } = ctx.context();
        let params = query.into_inner();

        let Ok((oauth_session, _)) = client.callback(params.into()).await else {
            panic!("failed to do client callback");
        };

        let did = oauth_session.did().await.expect("a did to be present");

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "text/html")
            .body(format!("sup: {did:?}").into())?)
    })
    .await
}
