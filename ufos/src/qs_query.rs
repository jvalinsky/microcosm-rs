use async_trait::async_trait;
use dropshot::{
    ApiEndpointBodyContentType, ExclusiveExtractor, ExtractorMetadata, HttpError, RequestContext,
    RequestInfo, ServerContext, SharedExtractor,
};
/// copied from https://github.com/oxidecomputer/dropshot/blob/695e1d8872c988c43066eb0848c87c127eeda361/dropshot/src/extractor/query.rs
/// Apache 2.0: https://github.com/oxidecomputer/dropshot/blob/695e1d8872c988c43066eb0848c87c127eeda361/LICENSE
use schemars::JsonSchema;
use serde::de::DeserializeOwned;

/// `VecsAllowedQuery<QueryType>` is an extractor used to deserialize an
/// instance of `QueryType` from an HTTP request's query string.  `QueryType`
/// is any structure of yours that implements [serde::Deserialize] and
/// [schemars::JsonSchema].  See the crate documentation for more information.
#[derive(Debug)]
pub struct VecsAllowedQuery<QueryType: DeserializeOwned + JsonSchema + Send + Sync> {
    inner: QueryType,
}
impl<QueryType: DeserializeOwned + JsonSchema + Send + Sync> VecsAllowedQuery<QueryType> {
    // TODO drop this in favor of Deref?  + Display and Debug for convenience?
    pub fn into_inner(self) -> QueryType {
        self.inner
    }
}

/// Given an HTTP request, pull out the query string and attempt to deserialize
/// it as an instance of `QueryType`.
fn http_request_load_query<QueryType>(
    request: &RequestInfo,
) -> Result<VecsAllowedQuery<QueryType>, HttpError>
where
    QueryType: DeserializeOwned + JsonSchema + Send + Sync,
{
    let raw_query_string = request.uri().query().unwrap_or("");
    // TODO-correctness: are query strings defined to be urlencoded in this way?
    match serde_qs::from_str(raw_query_string) {
        Ok(q) => Ok(VecsAllowedQuery { inner: q }),
        Err(e) => Err(HttpError::for_bad_request(
            None,
            format!("unable to parse query string: {}", e),
        )),
    }
}

// The `SharedExtractor` implementation for Query<QueryType> describes how to
// construct an instance of `Query<QueryType>` from an HTTP request: namely, by
// parsing the query string to an instance of `QueryType`.
// TODO-cleanup We shouldn't have to use the "'static" bound on `QueryType`
// here.  It seems like we ought to be able to use 'async_trait, but that
// doesn't seem to be defined.
#[async_trait]
impl<QueryType> SharedExtractor for VecsAllowedQuery<QueryType>
where
    QueryType: JsonSchema + DeserializeOwned + Send + Sync + 'static,
{
    async fn from_request<Context: ServerContext>(
        rqctx: &RequestContext<Context>,
    ) -> Result<VecsAllowedQuery<QueryType>, HttpError> {
        http_request_load_query(&rqctx.request)
    }

    fn metadata(body_content_type: ApiEndpointBodyContentType) -> ExtractorMetadata {
        // HACK: would love to use Query here but it "helpfully" panics when it sees a Vec.
        // we can't really get at enough of Query's logic to use it directly, sadly, so the
        // resulting openapi docs suck (query params are listed as body payload, example
        // codes make no sense, etc.)
        //
        // trying to hack the resulting ExtractorMetadata to look like Query's is a pain:
        // things almost work out but then something in dropshot won't be `pub` and it falls
        // apart. maybe it's possible, i didn't get it in the time i had.
        dropshot::TypedBody::<QueryType>::metadata(body_content_type)
    }
}
