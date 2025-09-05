use crate::TokenVerifier;
use poem::{
    Endpoint, EndpointExt, Route, Server,
    endpoint::{StaticFileEndpoint, make_sync},
    http::Method,
    listener::TcpListener,
    middleware::{CatchPanic, Cors, SizeLimit, Tracing},
};
use poem_openapi::{
    ApiResponse, ContactObject, ExternalDocumentObject, Object, OpenApi, OpenApiService,
    SecurityScheme, Tags,
    auth::Bearer,
    payload::{Json, PlainText},
    types::Example,
};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Debug, SecurityScheme)]
#[oai(ty = "bearer")]
struct XrpcAuth(Bearer);

#[derive(Tags)]
enum ApiTags {
    /// Custom pocket APIs
    #[oai(rename = "Pocket APIs")]
    Pocket,
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
}

#[derive(ApiResponse)]
enum PutBskyPrefsResponse {
    /// Record found
    #[oai(status = 200)]
    Ok(PlainText<String>),
    /// Bad request or no preferences to return
    #[oai(status = 400)]
    BadRequest(XrpcError),
    // /// Server errors
    // #[oai(status = 500)]
    // ServerError(XrpcError),
}

struct Xrpc {
    verifier: TokenVerifier,
}

#[OpenApi]
impl Xrpc {
    /// com.bad-example.pocket.getPreferences
    ///
    /// get stored preferencess
    #[oai(
        path = "/com.bad-example.pocket.getPreferences",
        method = "get",
        tag = "ApiTags::Pocket"
    )]
    async fn pocket_get_prefs(&self, XrpcAuth(auth): XrpcAuth) -> GetBskyPrefsResponse {
        let (did, aud) = match self
            .verifier
            .verify("com.bad-example.pocket.getPreferences", &auth.token)
            .await
        {
            Ok(d) => d,
            Err(e) => return GetBskyPrefsResponse::BadRequest(xrpc_error("boooo", e.to_string())),
        };
        log::info!("verified did: {did}/{aud}");
        // TODO: fetch from storage
        GetBskyPrefsResponse::Ok(Json(GetBskyPrefsResponseObject::example()))
    }

    /// com.bad-example.pocket.putPreferences
    ///
    /// store bluesky prefs
    #[oai(
        path = "/com.bad-example.pocket.putPreferences",
        method = "post",
        tag = "ApiTags::Pocket"
    )]
    async fn pocket_put_prefs(
        &self,
        XrpcAuth(auth): XrpcAuth,
        Json(prefs): Json<Value>,
    ) -> PutBskyPrefsResponse {
        let (did, aud) = match self
            .verifier
            .verify("com.bad-example.pocket.putPreferences", &auth.token)
            .await
        {
            Ok(d) => d,
            Err(e) => return PutBskyPrefsResponse::BadRequest(xrpc_error("boooo", e.to_string())),
        };
        log::info!("verified did: {did}/{aud}");
        log::warn!("received prefs: {prefs:?}");
        // TODO: put prefs into storage
        PutBskyPrefsResponse::Ok(PlainText("hiiiiii".to_string()))
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
    service: [AppViewService; 2],
}
/// Serve a did document for did:web for this to be an xrpc appview
fn get_did_doc(domain: &str) -> impl Endpoint + use<> {
    let doc = poem::web::Json(AppViewDoc {
        id: format!("did:web:{domain}"),
        service: [
            AppViewService {
                id: "#pocket_prefs".to_string(),
                r#type: "PocketPreferences".to_string(),
                service_endpoint: format!("https://{domain}"),
            },
            AppViewService {
                id: "#bsky_appview".to_string(),
                r#type: "BlueskyAppview".to_string(),
                service_endpoint: format!("https://{domain}"),
            },
        ],
    });
    make_sync(move |_| doc.clone())
}

pub async fn serve(domain: &str) -> () {
    let verifier = TokenVerifier::default();
    let api_service = OpenApiService::new(Xrpc { verifier }, "Pocket", env!("CARGO_PKG_VERSION"))
        .server(domain)
        .url_prefix("/xrpc")
        .contact(
            ContactObject::new()
                .name("@microcosm.blue")
                .url("https://bsky.app/profile/microcosm.blue"),
        )
        .description(include_str!("../api-description.md"))
        .external_document(ExternalDocumentObject::new("https://microcosm.blue/pocket"));

    let app = Route::new()
        .nest("/openapi", api_service.spec_endpoint())
        .nest("/xrpc/", api_service)
        .at("/.well-known/did.json", get_did_doc(domain))
        .at("/", StaticFileEndpoint::new("./static/index.html"))
        .with(SizeLimit::new(100 * 2_usize.pow(10)))
        .with(
            Cors::new()
                .allow_method(Method::GET)
                .allow_method(Method::POST),
        )
        .with(CatchPanic::new())
        .with(Tracing);

    let listener = TcpListener::bind("127.0.0.1:3000");
    Server::new(listener).name("pocket").run(app).await.unwrap();
}
