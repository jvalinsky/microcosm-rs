// originally from weaver: https://github.com/rsform/weaver/blob/ee08213a85e09889b9bd66beceecee92ac025801/crates/weaver-common/src/resolver.rs
// MPL 2.0: https://github.com/rsform/weaver/blob/ee08213a85e09889b9bd66beceecee92ac025801/LICENSE

use atrium_identity::handle::DnsTxtResolver;
use hickory_resolver::TokioResolver;

pub struct HickoryDnsTxtResolver {
    resolver: TokioResolver,
}

impl Default for HickoryDnsTxtResolver {
    fn default() -> Self {
        Self {
            resolver: TokioResolver::builder_tokio()
                .expect("failed to create resolver")
                .build(),
        }
    }
}

impl DnsTxtResolver for HickoryDnsTxtResolver {
    async fn resolve(
        &self,
        query: &str,
    ) -> core::result::Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self
            .resolver
            .txt_lookup(query)
            .await?
            .iter()
            .map(|txt| txt.to_string())
            .collect())
    }
}
