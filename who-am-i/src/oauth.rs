use atrium_api::{agent::SessionManager, types::string::Did};
use atrium_common::resolver::Resolver;
use atrium_identity::{
    did::{CommonDidResolver, CommonDidResolverConfig, DEFAULT_PLC_DIRECTORY_URL},
    handle::{AtprotoHandleResolver, AtprotoHandleResolverConfig, DnsTxtResolver},
};
use atrium_oauth::{
    AtprotoLocalhostClientMetadata, AuthorizeOptions, CallbackParams, DefaultHttpClient,
    KnownScope, OAuthClient, OAuthClientConfig, OAuthResolverConfig, Scope,
    store::{session::MemorySessionStore, state::MemoryStateStore},
};
use hickory_resolver::{ResolveError, TokioResolver};
use serde::Deserialize;
use std::sync::Arc;
use thiserror::Error;

const READONLY_SCOPE: [Scope; 1] = [Scope::Known(KnownScope::Atproto)];

#[derive(Debug, Deserialize)]
pub struct CallbackErrorParams {
    error: String,
    error_description: Option<String>,
    #[allow(dead_code)]
    state: Option<String>, // TODO: we _should_ use state to associate the auth request but how to do that with atrium is unclear
    iss: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum OAuthCallbackParams {
    Granted(CallbackParams),
    Failed(CallbackErrorParams),
}

type Client = OAuthClient<
    MemoryStateStore,
    MemorySessionStore,
    CommonDidResolver<DefaultHttpClient>,
    AtprotoHandleResolver<HickoryDnsTxtResolver, DefaultHttpClient>,
>;

#[derive(Clone)]
pub struct OAuth {
    client: Arc<Client>,
    did_resolver: Arc<CommonDidResolver<DefaultHttpClient>>,
}

#[derive(Debug, Error)]
pub enum AuthSetupError {
    #[error("failed to intiialize atrium client: {0}")]
    AtriumClientError(atrium_oauth::Error),
    #[error("failed to initialize hickory dns resolver: {0}")]
    HickoryResolverError(ResolveError),
}

#[derive(Debug, Error)]
pub enum OAuthCompleteError {
    #[error("the user denied request: {description:?} (from {issuer:?})")]
    Denied {
        description: Option<String>,
        issuer: Option<String>,
    },
    #[error("the request failed: {error}: {description:?} (from {issuer:?})")]
    Failed {
        error: String,
        description: Option<String>,
        issuer: Option<String>,
    },
    #[error("failed to complete oauth callback: {0}")]
    CallbackFailed(atrium_oauth::Error),
    #[error("the authorized session did not contain a DID")]
    NoDid,
}

#[derive(Debug, Error)]
pub enum ResolveHandleError {
    #[error("failed to resolve: {0}")]
    ResolutionFailed(#[from] atrium_identity::Error),
    #[error("identity resolved but no handle found for user")]
    NoHandle,
    #[error("found handle {0:?} but it appears invalid: {1}")]
    InvalidHandle(String, &'static str),
}

impl OAuth {
    pub fn new() -> Result<Self, AuthSetupError> {
        let http_client = Arc::new(DefaultHttpClient::default());
        let did_resolver = || {
            CommonDidResolver::new(CommonDidResolverConfig {
                plc_directory_url: DEFAULT_PLC_DIRECTORY_URL.to_string(),
                http_client: http_client.clone(),
            })
        };
        let dns_txt_resolver =
            HickoryDnsTxtResolver::new().map_err(AuthSetupError::HickoryResolverError)?;
        let client_config = OAuthClientConfig {
            client_metadata: AtprotoLocalhostClientMetadata {
                redirect_uris: Some(vec![String::from("http://127.0.0.1:9997/authorized")]),
                scopes: Some(READONLY_SCOPE.to_vec()),
            },
            keys: None,
            resolver: OAuthResolverConfig {
                did_resolver: did_resolver(),
                handle_resolver: AtprotoHandleResolver::new(AtprotoHandleResolverConfig {
                    dns_txt_resolver,
                    http_client: Arc::clone(&http_client),
                }),
                authorization_server_metadata: Default::default(),
                protected_resource_metadata: Default::default(),
            },
            state_store: MemoryStateStore::default(),
            session_store: MemorySessionStore::default(),
        };

        let client = OAuthClient::new(client_config).map_err(AuthSetupError::AtriumClientError)?;

        Ok(Self {
            client: Arc::new(client),
            did_resolver: Arc::new(did_resolver()),
        })
    }

    pub async fn begin(&self, handle: &str) -> Result<String, atrium_oauth::Error> {
        let auth_opts = AuthorizeOptions {
            scopes: READONLY_SCOPE.to_vec(),
            ..Default::default()
        };
        self.client.authorize(handle, auth_opts).await
    }

    /// Finally, resolve the oauth flow to a verified DID
    pub async fn complete(&self, params: OAuthCallbackParams) -> Result<Did, OAuthCompleteError> {
        let params = match params {
            OAuthCallbackParams::Granted(params) => params,
            OAuthCallbackParams::Failed(p) if p.error == "access_denied" => {
                return Err(OAuthCompleteError::Denied {
                    description: p.error_description.clone(),
                    issuer: p.iss.clone(),
                });
            }
            OAuthCallbackParams::Failed(p) => {
                return Err(OAuthCompleteError::Failed {
                    error: p.error.clone(),
                    description: p.error_description.clone(),
                    issuer: p.iss.clone(),
                });
            }
        };
        let (session, _) = self
            .client
            .callback(params)
            .await
            .map_err(OAuthCompleteError::CallbackFailed)?;
        let Some(did) = session.did().await else {
            return Err(OAuthCompleteError::NoDid);
        };
        Ok(did)
    }

    pub async fn resolve_handle(&self, did: Did) -> Result<String, ResolveHandleError> {
        // TODO: this is only half the resolution? or is atrium checking dns?
        let doc = self.did_resolver.resolve(&did).await?;
        let Some(aka) = doc.also_known_as else {
            return Err(ResolveHandleError::NoHandle);
        };
        let Some(at_uri_handle) = aka.first() else {
            return Err(ResolveHandleError::NoHandle);
        };
        if aka.len() > 1 {
            eprintln!("more than one handle found for {did:?}");
        }
        let Some(bare_handle) = at_uri_handle.strip_prefix("at://") else {
            return Err(ResolveHandleError::InvalidHandle(
                at_uri_handle.to_string(),
                "did not start with 'at://'",
            ));
        };
        if bare_handle.is_empty() {
            return Err(ResolveHandleError::InvalidHandle(
                at_uri_handle.to_string(),
                "empty handle",
            ));
        }
        Ok(bare_handle.to_string())
    }
}

pub struct HickoryDnsTxtResolver(TokioResolver);

impl HickoryDnsTxtResolver {
    fn new() -> Result<Self, ResolveError> {
        Ok(Self(TokioResolver::builder_tokio()?.build()))
    }
}

impl DnsTxtResolver for HickoryDnsTxtResolver {
    async fn resolve(
        &self,
        query: &str,
    ) -> core::result::Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        match self.0.txt_lookup(query).await {
            Ok(r) => {
                metrics::counter!("whoami_resolve_dns_txt", "success" => "true").increment(1);
                Ok(r.iter().map(|r| r.to_string()).collect())
            }
            Err(e) => {
                metrics::counter!("whoami_resolve_dns_txt", "success" => "false").increment(1);
                Err(e.into())
            }
        }
    }
}
