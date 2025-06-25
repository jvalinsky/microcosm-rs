use atrium_identity::{
    did::{CommonDidResolver, CommonDidResolverConfig, DEFAULT_PLC_DIRECTORY_URL},
    handle::{AtprotoHandleResolver, AtprotoHandleResolverConfig},
};
use atrium_oauth::{
    AuthorizeOptions,
    store::{session::MemorySessionStore, state::MemoryStateStore},
    AtprotoLocalhostClientMetadata, DefaultHttpClient, KnownScope, OAuthClient, OAuthClientConfig,
    OAuthResolverConfig, Scope,
};
use std::sync::Arc;
use crate::HickoryDnsTxtResolver;

pub type Client = OAuthClient<
    MemoryStateStore,
    MemorySessionStore,
    CommonDidResolver<DefaultHttpClient>,
    AtprotoHandleResolver<HickoryDnsTxtResolver, DefaultHttpClient>,
>;

pub fn client() -> Client {
    let http_client = Arc::new(DefaultHttpClient::default());
    let config = OAuthClientConfig {
        client_metadata: AtprotoLocalhostClientMetadata {
            redirect_uris: Some(vec![String::from("http://127.0.0.1:9997/authorized")]),
            scopes: Some(vec![
                Scope::Known(KnownScope::Atproto),
            ]),
        },
        keys: None,
        resolver: OAuthResolverConfig {
            did_resolver: CommonDidResolver::new(CommonDidResolverConfig {
                plc_directory_url: DEFAULT_PLC_DIRECTORY_URL.to_string(),
                http_client: Arc::clone(&http_client),
            }),
            handle_resolver: AtprotoHandleResolver::new(AtprotoHandleResolverConfig {
                dns_txt_resolver: HickoryDnsTxtResolver::default(),
                http_client: Arc::clone(&http_client),
            }),
            authorization_server_metadata: Default::default(),
            protected_resource_metadata: Default::default(),
        },
        // A store for saving state data while the user is being redirected to the authorization server.
        state_store: MemoryStateStore::default(),
        // A store for saving session data.
        session_store: MemorySessionStore::default(),
    };
    let Ok(client) = OAuthClient::new(config) else {
        panic!("failed to create oauth client");
    };
    client
}

pub async fn authorize(client: &Client, handle: &str) -> String {
    let Ok(url) = client.authorize(
        handle,
        AuthorizeOptions {
            scopes: vec![
                Scope::Known(KnownScope::Atproto),
            ],
            ..Default::default()
        },
    )
    .await else {
        panic!("failed to authorize");
    };
    url
}
