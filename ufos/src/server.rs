use crate::index_html::INDEX_HTML;
use crate::storage::StoreReader;
use crate::store_types::HourTruncatedCursor;
use crate::{ConsumerInfo, Nsid, NsidCount, QueryPeriod, TopCollections, UFOsRecord};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::Body;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::HttpError;
use dropshot::HttpResponseHeaders;
use dropshot::HttpResponseOk;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::ServerBuilder;
use http::{Response, StatusCode};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

struct Context {
    pub spec: Arc<serde_json::Value>,
    storage: Box<dyn StoreReader>,
}

fn dt_to_cursor(dt: DateTime<Utc>) -> Result<HourTruncatedCursor, HttpError> {
    let t = dt.timestamp_micros();
    if t < 0 {
        Err(HttpError::for_bad_request(None, "timestamp too old".into()))
    } else {
        let t = t as u64;
        let t_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        const ONE_HOUR: u64 = 60 * 60 * 1_000_000;
        if t < t_now || (t - t_now <= 2 * ONE_HOUR) {
            Err(HttpError::for_bad_request(None, "future timestamp".into()))
        } else {
            Ok(HourTruncatedCursor::truncate_raw_u64(t))
        }
    }
}

/// Serve index page as html
#[endpoint {
    method = GET,
    path = "/",
    /*
     * not useful to have this in openapi
     */
    unpublished = true,
}]
async fn index(_ctx: RequestContext<Context>) -> Result<Response<Body>, HttpError> {
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "text/html")
        .body(INDEX_HTML.into())?)
}

/// Meta: get the openapi spec for this api
#[endpoint {
    method = GET,
    path = "/openapi",
    /*
     * not useful to have this in openapi
     */
    unpublished = true,
}]
async fn get_openapi(ctx: RequestContext<Context>) -> OkCorsResponse<serde_json::Value> {
    let spec = (*ctx.context().spec).clone();
    ok_cors(spec)
}

#[derive(Debug, Serialize, JsonSchema)]
struct MetaInfo {
    storage_name: String,
    storage: serde_json::Value,
    consumer: ConsumerInfo,
}
/// Get meta information about UFOs itself
#[endpoint {
    method = GET,
    path = "/meta"
}]
async fn get_meta_info(ctx: RequestContext<Context>) -> OkCorsResponse<MetaInfo> {
    let Context { storage, .. } = ctx.context();
    let failed_to_get =
        |what| move |e| HttpError::for_internal_error(format!("failed to get {what}: {e:?}"));

    let storage_info = storage
        .get_storage_stats()
        .await
        .map_err(failed_to_get("storage info"))?;

    let consumer = storage
        .get_consumer_info()
        .await
        .map_err(failed_to_get("consumer info"))?;

    ok_cors(MetaInfo {
        storage_name: storage.name(),
        storage: storage_info,
        consumer,
    })
}
fn to_multiple_nsids(s: &str) -> Result<Vec<Nsid>, String> {
    let mut out = Vec::new();
    for collection in s.split(',') {
        let Ok(nsid) = Nsid::new(collection.to_string()) else {
            return Err(format!("collection {collection:?} was not a valid NSID"));
        };
        out.push(nsid);
    }
    Ok(out)
}

#[derive(Debug, Deserialize, JsonSchema)]
struct RecordsCollectionsQuery {
    collection: Option<String>, // JsonSchema not implemented for Nsid :(
}
#[derive(Debug, Serialize, JsonSchema)]
struct ApiRecord {
    did: String,
    collection: String,
    rkey: String,
    record: Box<serde_json::value::RawValue>,
    time_us: u64,
}
impl From<UFOsRecord> for ApiRecord {
    fn from(ufo: UFOsRecord) -> Self {
        Self {
            did: ufo.did.to_string(),
            collection: ufo.collection.to_string(),
            rkey: ufo.rkey.to_string(),
            record: ufo.record,
            time_us: ufo.cursor.to_raw_u64(),
        }
    }
}
/// Get recent records by collection
///
/// Multiple collections are supported. They will be delivered in one big array with no
/// specified order.
#[endpoint {
    method = GET,
    path = "/records",
}]
async fn get_records_by_collections(
    ctx: RequestContext<Context>,
    collection_query: Query<RecordsCollectionsQuery>,
) -> OkCorsResponse<Vec<ApiRecord>> {
    let Context { storage, .. } = ctx.context();
    let mut limit = 42;
    let query = collection_query.into_inner();
    let collections = if let Some(provided_collection) = query.collection {
        to_multiple_nsids(&provided_collection)
            .map_err(|reason| HttpError::for_bad_request(None, reason))?
    } else {
        let all_collections_should_be_nsids: Vec<String> = storage
            .get_top_collections()
            .await
            .map_err(|e| {
                HttpError::for_internal_error(format!("failed to get top collections: {e:?}"))
            })?
            .into();
        let mut all_collections = Vec::with_capacity(all_collections_should_be_nsids.len());
        for raw_nsid in all_collections_should_be_nsids {
            let nsid = Nsid::new(raw_nsid).map_err(|e| {
                HttpError::for_internal_error(format!("failed to parse nsid: {e:?}"))
            })?;
            all_collections.push(nsid);
        }

        limit = 12;
        all_collections
    };

    let records = storage
        .get_records_by_collections(&collections, limit, true)
        .await
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?
        .into_iter()
        .map(|r| r.into())
        .collect();

    ok_cors(records)
}

#[derive(Debug, Deserialize, JsonSchema)]
struct TotalSeenCollectionsQuery {
    collection: String, // JsonSchema not implemented for Nsid :(
}
#[derive(Debug, Serialize, JsonSchema)]
struct TotalCounts {
    total_records: u64,
    dids_estimate: u64,
}
/// Get total records seen by collection
#[endpoint {
    method = GET,
    path = "/records/total-seen"
}]
async fn get_records_total_seen(
    ctx: RequestContext<Context>,
    collection_query: Query<TotalSeenCollectionsQuery>,
) -> OkCorsResponse<HashMap<String, TotalCounts>> {
    let Context { storage, .. } = ctx.context();

    let query = collection_query.into_inner();
    let collections = to_multiple_nsids(&query.collection)
        .map_err(|reason| HttpError::for_bad_request(None, reason))?;

    let mut seen_by_collection = HashMap::with_capacity(collections.len());

    for collection in &collections {
        let (total_records, dids_estimate) = storage
            .get_counts_by_collection(collection)
            .await
            .map_err(|e| HttpError::for_internal_error(format!("boooo: {e:?}")))?;

        seen_by_collection.insert(
            collection.to_string(),
            TotalCounts {
                total_records,
                dids_estimate,
            },
        );
    }

    ok_cors(seen_by_collection)
}

#[derive(Debug, Serialize, JsonSchema)]
struct CollectionsResponse {
    /// Each known collection and its associated statistics
    ///
    /// The order is unspecified.
    collections: Vec<NsidCount>,
    /// Include in a follow-up request to get the next page of results, if more are available
    cursor: Option<String>,
}
#[derive(Debug, Deserialize, JsonSchema)]
struct AllCollectionsQuery {
    /// The maximum number of collections to return in one request.
    #[schemars(range(min = 1, max = 200), default = "all_collections_default_limit")]
    limit: usize,
    /// Always omit the cursor for the first request. If more collections than the limit are available, the response will contain a non-null `cursor` to include with the next request.
    cursor: Option<String>,
    /// Limit collections and statistics to those seen after this UTC datetime
    since: Option<DateTime<Utc>>,
    /// Limit collections and statistics to those seen before this UTC datetime
    until: Option<DateTime<Utc>>,
}
fn all_collections_default_limit() -> usize {
    100
}
#[endpoint {
    method = GET,
    path = "/collections/all"
}]
/// Get all collections
///
/// There have been a lot of collections seen in the ATmosphere, well over 400 at time of writing, so you *will* need to make a series of paginaged requests with `cursor`s to get them all.
///
/// The set of collections across multiple requests is not guaranteed to be a perfectly consistent snapshot:
///
/// - all collection NSIDs observed before the first request will be included in the results
///
/// - *new* NSIDs observed in the firehose *while paging* might be included or excluded from the final set
///
/// - no duplicate NSIDs will occur in the combined results
///
/// In practice this is close enough for most use-cases to not worry about.
///
/// Statistics are bucketed hourly, so the most granular effecitve time boundary for `since` and `until` is one hour.
async fn get_all_collections(
    ctx: RequestContext<Context>,
    query: Query<AllCollectionsQuery>,
) -> OkCorsResponse<CollectionsResponse> {
    let Context { storage, .. } = ctx.context();
    let q = query.into_inner();

    if !(1..=200).contains(&q.limit) {
        let msg = format!("limit not in 1..=200: {}", q.limit);
        return Err(HttpError::for_bad_request(None, msg));
    }

    let cursor = q
        .cursor
        .and_then(|c| if c.is_empty() { None } else { Some(c) })
        .map(|c| URL_SAFE_NO_PAD.decode(&c))
        .transpose()
        .map_err(|e| HttpError::for_bad_request(None, format!("invalid cursor: {e:?}")))?;

    let since = q.since.map(dt_to_cursor).transpose()?;
    let until = q.until.map(dt_to_cursor).transpose()?;

    let (collections, next_cursor) = storage
        .get_all_collections(q.limit, cursor, since, until)
        .await
        .map_err(|e| HttpError::for_internal_error(format!("oh shoot: {e:?}")))?;

    let next_cursor = next_cursor.map(|c| URL_SAFE_NO_PAD.encode(c));

    ok_cors(CollectionsResponse {
        collections,
        cursor: next_cursor,
    })
}

/// Get top collections by record count
#[endpoint {
    method = GET,
    path = "/collections/by-count"
}]
async fn get_top_collections_by_count(
    ctx: RequestContext<Context>,
) -> OkCorsResponse<Vec<NsidCount>> {
    let Context { storage, .. } = ctx.context();
    let collections = storage
        .get_top_collections_by_count(100, QueryPeriod::all_time())
        .await
        .map_err(|e| HttpError::for_internal_error(format!("oh shoot: {e:?}")))?;

    ok_cors(collections)
}

/// Get top collections by estimated unique DIDs
#[endpoint {
    method = GET,
    path = "/collections/by-dids"
}]
async fn get_top_collections_by_dids(
    ctx: RequestContext<Context>,
) -> OkCorsResponse<Vec<NsidCount>> {
    let Context { storage, .. } = ctx.context();
    let collections = storage
        .get_top_collections_by_dids(100, QueryPeriod::all_time())
        .await
        .map_err(|e| HttpError::for_internal_error(format!("oh shoot: {e:?}")))?;

    ok_cors(collections)
}

/// Get top collections
///
/// The format of this API response will be changing soon.
#[endpoint {
    method = GET,
    path = "/collections",
    /*
     * this is going away
     */
    unpublished = true,
}]
async fn get_top_collections(ctx: RequestContext<Context>) -> OkCorsResponse<TopCollections> {
    let Context { storage, .. } = ctx.context();
    let collections = storage
        .get_top_collections()
        .await
        .map_err(|e| HttpError::for_internal_error(format!("boooo: {e:?}")))?;

    ok_cors(collections)
}

pub async fn serve(storage: impl StoreReader + 'static) -> Result<(), String> {
    let log = ConfigLogging::StderrTerminal {
        level: ConfigLoggingLevel::Info,
    }
    .to_logger("hello-ufos")
    .map_err(|e| e.to_string())?;

    let mut api = ApiDescription::new();

    api.register(index).unwrap();
    api.register(get_openapi).unwrap();
    api.register(get_meta_info).unwrap();
    api.register(get_records_by_collections).unwrap();
    api.register(get_records_total_seen).unwrap();
    api.register(get_all_collections).unwrap();
    api.register(get_top_collections_by_count).unwrap();
    api.register(get_top_collections_by_dids).unwrap();
    api.register(get_top_collections).unwrap();

    let context = Context {
        spec: Arc::new(
            api.openapi(
                "UFOs: Every lexicon in the ATmosphere",
                env!("CARGO_PKG_VERSION")
                    .parse()
                    .inspect_err(|e| {
                        log::warn!("failed to parse cargo package version for openapi: {e:?}")
                    })
                    .unwrap_or(semver::Version::new(0, 0, 1)),
            )
            .description("Samples and statistics of atproto records by their collection NSID")
            .contact_name("part of @microcosm.blue")
            .contact_url("https://microcosm.blue")
            .json()
            .map_err(|e| e.to_string())?,
        ),
        storage: Box::new(storage),
    };

    ServerBuilder::new(api, context, log)
        .config(ConfigDropshot {
            bind_address: "0.0.0.0:9999".parse().unwrap(),
            ..Default::default()
        })
        .start()
        .map_err(|error| format!("failed to start server: {}", error))?
        .await
}

/// awkward helpers
type OkCorsResponse<T> = Result<HttpResponseHeaders<HttpResponseOk<T>>, HttpError>;
fn ok_cors<T: Send + Sync + Serialize + JsonSchema>(t: T) -> OkCorsResponse<T> {
    let mut res = HttpResponseHeaders::new_unnamed(HttpResponseOk(t));
    res.headers_mut()
        .insert("access-control-allow-origin", "*".parse().unwrap());
    Ok(res)
}
