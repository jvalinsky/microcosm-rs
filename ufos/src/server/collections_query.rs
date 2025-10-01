use crate::Nsid;
use async_trait::async_trait;
use dropshot::{
    ApiEndpointBodyContentType, ExtractorMetadata, HttpError, Query, RequestContext, ServerContext,
    SharedExtractor,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::HashSet;

/// The real type that gets deserialized
#[derive(Debug, Deserialize, JsonSchema)]
pub struct MultiCollectionQuery {
    pub collection: Vec<String>,
}

/// The fake corresponding type for docs that dropshot won't freak out about a
/// vec for
#[derive(Deserialize, JsonSchema)]
#[allow(dead_code)]
struct MultiCollectionQueryForDocs {
    /// One or more collection [NSID](https://atproto.com/specs/nsid)s
    ///
    /// Pass this parameter multiple times to specify multiple collections, like
    /// `collection=app.bsky.feed.like&collection=app.bsky.feed.post`
    collection: String,
}

impl TryFrom<MultiCollectionQuery> for HashSet<Nsid> {
    type Error = HttpError;
    fn try_from(mcq: MultiCollectionQuery) -> Result<Self, Self::Error> {
        let mut out = HashSet::with_capacity(mcq.collection.len());
        for c in mcq.collection {
            let nsid = Nsid::new(c).map_err(|e| {
                HttpError::for_bad_request(
                    None,
                    format!("failed to convert collection to an NSID: {e:?}"),
                )
            })?;
            out.insert(nsid);
        }
        Ok(out)
    }
}

// The `SharedExtractor` implementation for Query<QueryType> describes how to
// construct an instance of `Query<QueryType>` from an HTTP request: namely, by
// parsing the query string to an instance of `QueryType`.
#[async_trait]
impl SharedExtractor for MultiCollectionQuery {
    async fn from_request<Context: ServerContext>(
        ctx: &RequestContext<Context>,
    ) -> Result<MultiCollectionQuery, HttpError> {
        let raw_query = ctx.request.uri().query().unwrap_or("");
        let q = serde_qs::from_str(raw_query).map_err(|e| {
            HttpError::for_bad_request(None, format!("unable to parse query string: {e}"))
        })?;
        Ok(q)
    }

    fn metadata(body_content_type: ApiEndpointBodyContentType) -> ExtractorMetadata {
        // HACK: query type switcheroo: passing MultiCollectionQuery to
        // `metadata` would "helpfully" panic because dropshot believes we can
        // only have scalar types in a  query.
        //
        // so instead we have a fake second type whose only job is to look the
        // same as MultiCollectionQuery exept that it has `String` instead of
        // `Vec<String>`, which dropshot will accept, and generate ~close-enough
        // docs for.
        <Query<MultiCollectionQueryForDocs> as SharedExtractor>::metadata(body_content_type)
    }
}
