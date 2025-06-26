use atrium_api::types::string::Did;
use atrium_common::resolver::Resolver;
use atrium_identity::did::{CommonDidResolver, CommonDidResolverConfig, DEFAULT_PLC_DIRECTORY_URL};
use atrium_oauth::DefaultHttpClient;
use std::sync::Arc;

pub async fn resolve_identity(did: String) -> String {
    let http_client = Arc::new(DefaultHttpClient::default());
    let resolver = CommonDidResolver::new(CommonDidResolverConfig {
        plc_directory_url: DEFAULT_PLC_DIRECTORY_URL.to_string(),
        http_client: Arc::clone(&http_client),
    });
    let doc = resolver.resolve(&Did::new(did).unwrap()).await.unwrap(); // TODO: this is only half the resolution? or is atrium checking dns?
    if let Some(aka) = doc.also_known_as {
        if let Some(f) = aka.first() {
            return f.to_string();
        }
    }
    "who knows".to_string()
}
