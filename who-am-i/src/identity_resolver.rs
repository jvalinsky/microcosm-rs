use atrium_api::types::string::Did;
use atrium_common::resolver::Resolver;
use atrium_identity::did::{CommonDidResolver, CommonDidResolverConfig, DEFAULT_PLC_DIRECTORY_URL};
use atrium_oauth::DefaultHttpClient;
use std::sync::Arc;

pub async fn resolve_identity(did: String) -> Option<String> {
    let http_client = Arc::new(DefaultHttpClient::default());
    let resolver = CommonDidResolver::new(CommonDidResolverConfig {
        plc_directory_url: DEFAULT_PLC_DIRECTORY_URL.to_string(),
        http_client: Arc::clone(&http_client),
    });
    let doc = resolver.resolve(&Did::new(did).unwrap()).await.unwrap(); // TODO: this is only half the resolution? or is atrium checking dns?
    doc.also_known_as.and_then(|mut aka| {
        if aka.is_empty() {
            None
        } else {
            Some(aka.remove(0))
        }
    })
}
