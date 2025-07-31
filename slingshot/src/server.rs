use crate::{CachedRecord, Identity, Repo, error::ServerError};
use atrium_api::types::string::{Cid, Did, Handle, Nsid, RecordKey};
use foyer::HybridCache;
use serde::Serialize;
use std::str::FromStr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use poem::{
    Endpoint, EndpointExt, Route, Server,
    endpoint::make_sync,
    http::Method,
    listener::{
        Listener, TcpListener,
        acme::{AutoCert, LETS_ENCRYPT_PRODUCTION},
    },
    middleware::{Cors, Tracing},
};
use poem_openapi::{
    ApiResponse, Object, OpenApi, OpenApiService, param::Query, payload::Json, types::Example,
};

fn example_did() -> String {
    "did:plc:hdhoaan3xa3jiuq4fg4mefid".to_string()
}
fn example_collection() -> String {
    "app.bsky.feed.like".to_string()
}
fn example_rkey() -> String {
    "3lv4ouczo2b2a".to_string()
}

#[derive(Object)]
#[oai(example = true)]
struct XrpcErrorResponseObject {
    /// Should correspond an error `name` in the lexicon errors array
    error: String,
    /// Human-readable description and possibly additonal context
    message: String,
}
impl Example for XrpcErrorResponseObject {
    fn example() -> Self {
        Self {
            error: "RecordNotFound".to_string(),
            message: "This record was deleted".to_string(),
        }
    }
}
type XrpcError = Json<XrpcErrorResponseObject>;
fn xrpc_error(error: impl AsRef<str>, message: impl AsRef<str>) -> XrpcError {
    Json(XrpcErrorResponseObject {
        error: error.as_ref().to_string(),
        message: message.as_ref().to_string(),
    })
}

fn bad_request_handler(err: poem::Error) -> GetRecordResponse {
    GetRecordResponse::BadRequest(Json(XrpcErrorResponseObject {
        error: "InvalidRequest".to_string(),
        message: format!("Bad request, here's some info that maybe should not be exposed: {err}"),
    }))
}

#[derive(Object)]
#[oai(example = true)]
struct FoundRecordResponseObject {
    /// at-uri for this record
    uri: String,
    /// CID for this exact version of the record
    ///
    /// Slingshot will always return the CID, despite it not being a required
    /// response property in the official lexicon.
    cid: Option<String>,
    /// the record itself as JSON
    value: serde_json::Value,
}
impl Example for FoundRecordResponseObject {
    fn example() -> Self {
        Self {
            uri: format!(
                "at://{}/{}/{}",
                example_did(),
                example_collection(),
                example_rkey()
            ),
            cid: Some("bafyreialv3mzvvxaoyrfrwoer3xmabbmdchvrbyhayd7bga47qjbycy74e".to_string()),
            value: serde_json::json!({
                "$type": "app.bsky.feed.like",
                "createdAt": "2025-07-29T18:02:02.327Z",
                "subject": {
                  "cid": "bafyreia2gy6eyk5qfetgahvshpq35vtbwy6negpy3gnuulcdi723mi7vxy",
                  "uri": "at://did:plc:vwzwgnygau7ed7b7wt5ux7y2/app.bsky.feed.post/3lv4lkb4vgs2k"
                }
            }),
        }
    }
}

#[derive(ApiResponse)]
#[oai(bad_request_handler = "bad_request_handler")]
enum GetRecordResponse {
    /// Record found
    #[oai(status = 200)]
    Ok(Json<FoundRecordResponseObject>),
    /// Bad request or no record to return
    ///
    /// The only error name in the repo.getRecord lexicon is `RecordNotFound`,
    /// but the [canonical api docs](https://docs.bsky.app/docs/api/com-atproto-repo-get-record)
    /// also list `InvalidRequest`, `ExpiredToken`, and `InvalidToken`. Of
    /// these, slingshot will only return `RecordNotFound` or `InvalidRequest`.
    #[oai(status = 400)]
    BadRequest(XrpcError),
    /// Just using 500 for potentially upstream errors for now
    #[oai(status = 500)]
    ServerError(XrpcError),
}

struct Xrpc {
    cache: HybridCache<String, CachedRecord>,
    identity: Identity,
    repo: Arc<Repo>,
}

#[OpenApi]
impl Xrpc {
    /// com.atproto.repo.getRecord
    ///
    /// Get a single record from a repository. Does not require auth.
    ///
    /// See https://docs.bsky.app/docs/api/com-atproto-repo-get-record for the
    /// canonical XRPC documentation that this endpoint aims to be compatible
    /// with.
    #[oai(path = "/com.atproto.repo.getRecord", method = "get")]
    async fn get_record(
        &self,
        /// The DID or handle of the repo
        #[oai(example = "example_did")]
        Query(repo): Query<String>,
        /// The NSID of the record collection
        #[oai(example = "example_collection")]
        Query(collection): Query<String>,
        /// The Record key
        #[oai(example = "example_rkey")]
        Query(rkey): Query<String>,
        /// Optional: the CID of the version of the record.
        ///
        /// If not specified, then return the most recent version.
        ///
        /// If specified and a newer version of the record exists, returns 404 not
        /// found. That is: slingshot only retains the most recent version of a
        /// record. (TODO: verify bsky behaviour for mismatched/old CID)
        Query(cid): Query<Option<String>>,
    ) -> GetRecordResponse {
        let did = match Did::new(repo.clone()) {
            Ok(did) => did,
            Err(_) => {
                let Ok(handle) = Handle::new(repo) else {
                    return GetRecordResponse::BadRequest(xrpc_error(
                        "InvalidRequest",
                        "repo was not a valid DID or handle",
                    ));
                };
                if let Ok(res) = self.identity.handle_to_did(handle).await {
                    if let Some(did) = res {
                        did
                    } else {
                        return GetRecordResponse::BadRequest(xrpc_error(
                            "InvalidRequest",
                            "Could not resolve handle repo to a DID",
                        ));
                    }
                } else {
                    return GetRecordResponse::ServerError(xrpc_error(
                        "ResolutionFailed",
                        "errored while trying to resolve handle to DID",
                    ));
                }
            }
        };

        let Ok(collection) = Nsid::new(collection) else {
            return GetRecordResponse::BadRequest(xrpc_error(
                "InvalidRequest",
                "invalid NSID for collection",
            ));
        };

        let Ok(rkey) = RecordKey::new(rkey) else {
            return GetRecordResponse::BadRequest(xrpc_error("InvalidRequest", "invalid rkey"));
        };

        let cid: Option<Cid> = if let Some(cid) = cid {
            let Ok(cid) = Cid::from_str(&cid) else {
                return GetRecordResponse::BadRequest(xrpc_error("InvalidRequest", "invalid CID"));
            };
            Some(cid)
        } else {
            None
        };

        let at_uri = format!("at://{}/{}/{}", &*did, &*collection, &*rkey);

        let entry = self
            .cache
            .fetch(at_uri.clone(), {
                let cid = cid.clone();
                let repo_api = self.repo.clone();
                || async move {
                    repo_api
                        .get_record(&did, &collection, &rkey, &cid)
                        .await
                        .map_err(|e| foyer::Error::Other(Box::new(e)))
                }
            })
            .await
            .unwrap(); // todo

        // TODO: actual 404

        match *entry {
            CachedRecord::Found(ref raw) => {
                let (found_cid, raw_value) = raw.into();
                if cid.clone().map(|c| c != found_cid).unwrap_or(false) {
                    return GetRecordResponse::BadRequest(Json(XrpcErrorResponseObject {
                        error: "RecordNotFound".to_string(),
                        message: "A record was found but its CID did not match that requested"
                            .to_string(),
                    }));
                }
                // TODO: thank u stellz: https://gist.github.com/stella3d/51e679e55b264adff89d00a1e58d0272
                let value =
                    serde_json::from_str(raw_value.get()).expect("RawValue to be valid json");
                GetRecordResponse::Ok(Json(FoundRecordResponseObject {
                    uri: at_uri,
                    cid: Some(found_cid.as_ref().to_string()),
                    value,
                }))
            }
            CachedRecord::Deleted => GetRecordResponse::BadRequest(Json(XrpcErrorResponseObject {
                error: "RecordNotFound".to_string(),
                message: "This record was deleted".to_string(),
            })),
        }
    }

    // TODO
    // #[oai(path = "/com.atproto.identity.resolveHandle", method = "get")]
    // #[oai(path = "/com.atproto.identity.resolveDid", method = "get")]
    // but these are both not specified to do bidirectional validation, which is what we want to offer
    // com.atproto.identity.resolveIdentity seems right, but requires returning the full did-doc
    // would be nice if there were two queries:
    //  did -> verified handle + pds url
    //  handle -> verified did + pds url
    //
    // we could do horrible things and implement resolveIdentity with only a stripped-down fake did doc
    // but this will *definitely* cause problems because eg. we're not currently storing pubkeys and
    // those are a little bit important
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct AppViewService {
    id: String,
    r#type: String,
    service_endpoint: String,
}
#[derive(Debug, Clone, Serialize)]
struct AppViewDoc {
    id: String,
    service: [AppViewService; 1],
}
/// Serve a did document for did:web for this to be an xrpc appview
///
/// No slingshot endpoints currently require auth, so it's not necessary to do
/// service proxying, however clients may wish to:
///
/// - PDS proxying offers a level of client IP anonymity from slingshot
/// - slingshot *may* implement more generous per-user rate-limits for proxied requests in the future
fn get_did_doc(host: &str) -> impl Endpoint + use<> {
    let doc = poem::web::Json(AppViewDoc {
        id: format!("did:web:{host}"),
        service: [AppViewService {
            id: "#slingshot".to_string(),
            r#type: "SlingshotRecordProxy".to_string(),
            service_endpoint: format!("https://{host}"),
        }],
    });
    make_sync(move |_| doc.clone())
}

pub async fn serve(
    cache: HybridCache<String, CachedRecord>,
    identity: Identity,
    repo: Repo,
    host: Option<String>,
    _shutdown: CancellationToken,
) -> Result<(), ServerError> {
    let repo = Arc::new(repo);
    let api_service = OpenApiService::new(
        Xrpc {
            cache,
            identity,
            repo,
        },
        "Slingshot",
        env!("CARGO_PKG_VERSION"),
    )
    .server("http://localhost:3000")
    .url_prefix("/xrpc");

    let mut app = Route::new()
        .nest("/", api_service.scalar())
        .nest("/openapi.json", api_service.spec_endpoint())
        .nest("/xrpc/", api_service);

    if let Some(host) = host {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("alskfjalksdjf");

        app = app.at("/.well-known/did.json", get_did_doc(&host));

        let auto_cert = AutoCert::builder()
            .directory_url(LETS_ENCRYPT_PRODUCTION)
            .domain(&host)
            .build()
            .map_err(ServerError::AcmeBuildError)?;

        run(TcpListener::bind("0.0.0.0:443").acme(auto_cert), app).await
    } else {
        run(TcpListener::bind("127.0.0.1:3000"), app).await
    }
}

async fn run<L>(listener: L, app: Route) -> Result<(), ServerError>
where
    L: Listener + 'static,
{
    let app = app
        .with(
            Cors::new()
                .allow_origin("*")
                .allow_methods([Method::GET])
                .allow_credentials(false),
        )
        .with(Tracing);
    Server::new(listener)
        .name("slingshot")
        .run(app)
        .await
        .map_err(ServerError::ServerExited)
}
