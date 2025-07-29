use serde_json::value::RawValue;
use crate::CachedRecord;
use foyer::HybridCache;
use crate::error::ServerError;
use dropshot::{
    ApiDescription, Body, ConfigDropshot, ConfigLogging,
    ConfigLoggingLevel, HttpError, HttpResponse, Query, RequestContext,
    ServerBuilder, ServerContext, endpoint,
    ClientErrorStatusCode,
};
use http::{
    Response, StatusCode,
    header::{ORIGIN, USER_AGENT},
};
use metrics::{counter, histogram};
use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const INDEX_HTML: &str = include_str!("../static/index.html");
const FAVICON: &[u8] = include_bytes!("../static/favicon.ico");

pub async fn serve(
    cache: HybridCache<String, CachedRecord>,
    shutdown: CancellationToken,
) -> Result<(), ServerError> {
    let config_logging = ConfigLogging::StderrTerminal {
        level: ConfigLoggingLevel::Info,
    };

    let log = config_logging
        .to_logger("example-basic")
        .map_err(ServerError::ConfigLogError)?;

    let mut api = ApiDescription::new();
    api.register(index).unwrap();
    api.register(favicon).unwrap();
    api.register(openapi).unwrap();
    api.register(get_record).unwrap();

    // TODO: put spec in a once cell / lazy lock thing?
    let spec = Arc::new(
        api.openapi(
            "Slingshot",
            env!("CARGO_PKG_VERSION")
                .parse()
                .inspect_err(|e| {
                    eprintln!("failed to parse cargo package version for openapi: {e:?}")
                })
                .unwrap_or(semver::Version::new(0, 0, 1)),
        )
        .description("A fast edge cache for getRecord")
        .contact_name("part of @microcosm.blue")
        .contact_url("https://microcosm.blue")
        .json()
        .map_err(ServerError::OpenApiJsonFail)?,
    );

    let sub_shutdown = shutdown.clone();
    let ctx = Context {
        cache,
        spec,
        shutdown: sub_shutdown,
    };

    let server = ServerBuilder::new(api, ctx, log)
        .config(ConfigDropshot {
            bind_address: "0.0.0.0:9996".parse().unwrap(),
            ..Default::default()
        })
        .start()?;

    tokio::select! {
        s = server.wait_for_shutdown() => {
            s.map_err(ServerError::ServerExited)?;
            log::info!("server shut down normally.");
        },
        _ = shutdown.cancelled() => {
            log::info!("shutting down: closing server");
            server.close().await.map_err(ServerError::BadClose)?;
        },
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct Context {
    pub cache: HybridCache<String, CachedRecord>,
    pub spec: Arc<serde_json::Value>,
    pub shutdown: CancellationToken,
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

pub fn cors_err(e: HttpError) -> HttpError {
    e.with_header("access-control-allow-origin", "*").unwrap()
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
struct GetRecordQuery {
    /// The DID of the repo
    ///
    /// NOTE: handles should be accepted here but this is still TODO in slingshot
    pub repo: String,
    /// The NSID of the record collection
    pub collection: String,
    /// The Record key
    pub rkey: String,
    /// Optional: the CID of the version of the record.
    ///
    /// If not specified, then return the most recent version.
    ///
    /// If specified and a newer version of the record exists, returns 404 not
    /// found. That is: slingshot only retains the most recent version of a
    /// record.
    #[serde(default)]
    pub cid: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct GetRecordResponse {
    pub uri: String,
    pub cid: String,
    pub value: Box<RawValue>,
}

/// com.atproto.repo.getRecord
///
/// Get a single record from a repository. Does not require auth.
///
/// See https://docs.bsky.app/docs/api/com-atproto-repo-get-record for the
/// canonical XRPC documentation that this endpoint aims to be compatible with.
#[endpoint {
    method = GET,
    path = "/xrpc/com.atproto.repo.getRecord",
}]
async fn get_record(
    ctx: RequestContext<Context>,
    query: Query<GetRecordQuery>,
) -> OkCorsResponse<GetRecordResponse> {

    let Context { cache, .. } = ctx.context();
    let GetRecordQuery { repo, collection, rkey, cid } = query.into_inner();

    // TODO: yeah yeah
    let at_uri = format!(
        "at://{}/{}/{}",
        &*repo, &*collection, &*rkey
    );

    instrument_handler(&ctx, async {
        let entry = cache
            .fetch(at_uri.clone(), || async move {
                Err(foyer::Error::Other(Box::new(ServerError::OhNo("booo".to_string()))))
            })
            .await
            .unwrap();

        match *entry {
            CachedRecord::Found(ref raw) => {
                let (found_cid, raw_value) = raw.into();
                let found_cid = found_cid.as_ref().to_string();
                if cid.map(|c| c != found_cid).unwrap_or(false) {
                    Err(HttpError::for_not_found(None, "CID mismatch".to_string()))
                        .map_err(cors_err)?;
                }
                OkCors(GetRecordResponse {
                    uri: at_uri,
                    cid: found_cid,
                    value: raw_value,
                }).into()
            },
            CachedRecord::Deleted => {
                Err(HttpError::for_client_error_with_status(
                    Some("Gone".to_string()),
                    ClientErrorStatusCode::GONE,
                )).map_err(cors_err)
            }
        }
    })
    .await

}
