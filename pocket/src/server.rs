use poem::{
    endpoint::make_sync,
    Endpoint,
    Route,
    Server,
    EndpointExt,
    http::{Method, HeaderMap},
    middleware::{CatchPanic, Cors, Tracing},
    listener::TcpListener,
};
use poem_openapi::{
    ContactObject,
    ExternalDocumentObject,
    OpenApi,
    OpenApiService,
    Tags,
    Object,
    ApiResponse,
    types::Example,
    auth::Bearer,
    payload::Json,
    SecurityScheme,
};
use crate::verify;
use serde::Serialize;
use serde_json::{Value, json};


#[derive(Debug, SecurityScheme)]
#[oai(ty = "bearer")]
struct BlahAuth(Bearer);


#[derive(Tags)]
enum ApiTags {
    /// Bluesky-compatible APIs.
    #[oai(rename = "app.bsky.* queries")]
    AppBsky,
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
            error: "PreferencesNotFound".to_string(),
            message: "No preferences were found for this user".to_string(),
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

#[derive(Object)]
#[oai(example = true)]
struct GetBskyPrefsResponseObject {
    /// at-uri for this record
    preferences: Value,
}
impl Example for GetBskyPrefsResponseObject {
    fn example() -> Self {
        Self {
            preferences: json!({
                "hello": "world",
            }),
        }
    }
}

#[derive(ApiResponse)]
enum GetBskyPrefsResponse {
    /// Record found
    #[oai(status = 200)]
    Ok(Json<GetBskyPrefsResponseObject>),
    /// Bad request or no preferences to return
    #[oai(status = 400)]
    BadRequest(XrpcError),
    // /// Server errors
    // #[oai(status = 500)]
    // ServerError(XrpcError),
}

struct Xrpc {
    domain: String,
}

#[OpenApi]
impl Xrpc {
    /// app.bsky.actor.getPreferences
    ///
    /// get stored bluesky prefs
    #[oai(
        path = "/app.bsky.actor.getPreferences",
        method = "get",
        tag = "ApiTags::AppBsky"
    )]
    async fn app_bsky_get_prefs(
        &self,
        BlahAuth(auth): BlahAuth,
        m: &HeaderMap,
    ) -> GetBskyPrefsResponse {
        log::warn!("hm: {m:?}");
        match verify(
            &format!("did:web:{}#bsky_appview", self.domain),
            "app.bsky.actor.getPreferences",
            &auth.token,
        ).await {
            Ok(did) => log::info!("wooo! {did}"),
            Err(err) => return GetBskyPrefsResponse::BadRequest(xrpc_error("booo", err)),
        };
        log::warn!("got bearer: {:?}", auth.token);
        GetBskyPrefsResponse::Ok(Json(GetBskyPrefsResponseObject::example()))
    }

    /// app.bsky.actor.putPreferences
    ///
    /// store bluesky prefs
    #[oai(
        path = "/app.bsky.actor.putPreferences",
        method = "post",
        tag = "ApiTags::AppBsky"
    )]
    async fn app_bsky_put_prefs(
        &self,
        Json(prefs): Json<Value>,
    ) -> () {
        log::warn!("received prefs: {prefs:?}");
        ()
    }
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
fn get_did_doc(domain: &str) -> impl Endpoint + use<> {
    let doc = poem::web::Json(AppViewDoc {
        id: format!("did:web:{domain}"),
        service: [AppViewService {
            id: "#bsky_appview".to_string(),
            r#type: "PocketBlueskyPreferences".to_string(),
            service_endpoint: format!("https://{domain}"),
        }],
    });
    make_sync(move |_| doc.clone())
}

pub async fn serve(
    domain: &str,
) -> () {
    let api_service = OpenApiService::new(
        Xrpc { domain: domain.to_string() },
        "Pocket",
        env!("CARGO_PKG_VERSION"),
    )
    .server(domain)
    .url_prefix("/xrpc")
    .contact(
        ContactObject::new()
            .name("@microcosm.blue")
            .url("https://bsky.app/profile/microcosm.blue"),
    )
    // .description(include_str!("../api-description.md"))
    .external_document(ExternalDocumentObject::new(
        "https://microcosm.blue/pocket",
    ));

    let app = Route::new()
        .at("/.well-known/did.json", get_did_doc(&domain))
        .nest("/xrpc/", api_service)
        // .at("/", StaticFileEndpoint::new("./static/index.html"))
        // .nest("/openapi", api_service.spec_endpoint())
        .with(
            Cors::new()
                .allow_method(Method::GET)
                .allow_method(Method::POST)
        )
        .with(CatchPanic::new())
        .with(Tracing);

    let listener = TcpListener::bind("127.0.0.1:3000");
    Server::new(listener)
        .name("pocket")
        .run(app)
        .await
        .unwrap();

}
