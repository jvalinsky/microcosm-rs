use crate::index_html::INDEX_HTML;
use crate::storage::StoreReader;
use crate::store_types::{HourTruncatedCursor, WeekTruncatedCursor};
use crate::{ConsumerInfo, Cursor, JustCount, Nsid, NsidCount, OrderCollectionsBy, UFOsRecord};
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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
        if t > t_now && (t - t_now > 2 * ONE_HOUR) {
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
fn to_multiple_nsids(s: &str) -> Result<HashSet<Nsid>, String> {
    let mut out = HashSet::new();
    for collection in s.split(',') {
        let Ok(nsid) = Nsid::new(collection.to_string()) else {
            return Err(format!("collection {collection:?} was not a valid NSID"));
        };
        out.insert(nsid);
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
        limit = 12;
        let min_time_ago = SystemTime::now() - Duration::from_secs(86_400 * 3); // we want at least 3 days of data
        let since: WeekTruncatedCursor = Cursor::at(min_time_ago).into();
        let (collections, _) = storage
            .get_collections(
                1000,
                Default::default(),
                Some(since.try_as().unwrap()),
                None,
            )
            .await
            .map_err(|e| HttpError::for_internal_error(e.to_string()))?;
        collections
            .into_iter()
            .map(|c| Nsid::new(c.nsid).unwrap())
            .collect()
    };

    let records = storage
        .get_records_by_collections(collections, limit, true)
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
#[serde(rename_all = "kebab-case")]
pub enum CollectionsQueryOrder {
    RecordsCreated,
    DidsEstimate,
}
impl From<&CollectionsQueryOrder> for OrderCollectionsBy {
    fn from(q: &CollectionsQueryOrder) -> Self {
        match q {
            CollectionsQueryOrder::RecordsCreated => OrderCollectionsBy::RecordsCreated,
            CollectionsQueryOrder::DidsEstimate => OrderCollectionsBy::DidsEstimate,
        }
    }
}
#[derive(Debug, Deserialize, JsonSchema)]
struct CollectionsQuery {
    /// The maximum number of collections to return in one request.
    ///
    /// Default: `100` normally, `32` if `order` is specified.
    #[schemars(range(min = 1, max = 200))]
    limit: Option<usize>,
    /// Get a paginated response with more collections.
    ///
    /// Always omit the cursor for the first request. If more collections than the limit are available, the response will contain a non-null `cursor` to include with the next request.
    ///
    /// `cursor` is mutually exclusive with `order`.
    cursor: Option<String>,
    /// Limit collections and statistics to those seen after this UTC datetime
    since: Option<DateTime<Utc>>,
    /// Limit collections and statistics to those seen before this UTC datetime
    until: Option<DateTime<Utc>>,
    /// Get a limited, sorted list
    ///
    /// Mutually exclusive with `cursor` -- sorted results cannot be paged.
    order: Option<CollectionsQueryOrder>,
}

/// Get collection with statistics
///
/// ## To fetch a full list:
///
/// Omit the `order` parameter and page through the results using the `cursor`. There have been a lot of collections seen in the ATmosphere, well over 400 at time of writing, so you *will* need to make a series of paginaged requests with `cursor`s to get them all.
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
/// ## To fetch the top collection NSIDs:
///
/// Specify the `order` parameter (must be either `records-created` or `did-estimate`). Note that ordered results cannot be paged.
///
/// All statistics are bucketed hourly, so the most granular effecitve time boundary for `since` and `until` is one hour.
#[endpoint {
    method = GET,
    path = "/collections"
}]
async fn get_collections(
    ctx: RequestContext<Context>,
    query: Query<CollectionsQuery>,
) -> OkCorsResponse<CollectionsResponse> {
    let Context { storage, .. } = ctx.context();
    let q = query.into_inner();

    if q.cursor.is_some() && q.order.is_some() {
        let msg = "`cursor` is mutually exclusive with `order`. ordered results cannot be paged.";
        return Err(HttpError::for_bad_request(None, msg.to_string()));
    }

    let order = if let Some(ref o) = q.order {
        o.into()
    } else {
        let cursor = q
            .cursor
            .and_then(|c| if c.is_empty() { None } else { Some(c) })
            .map(|c| URL_SAFE_NO_PAD.decode(&c))
            .transpose()
            .map_err(|e| HttpError::for_bad_request(None, format!("invalid cursor: {e:?}")))?;
        OrderCollectionsBy::Lexi { cursor }
    };

    let limit = match (q.limit, q.order) {
        (Some(limit), _) => limit,
        (None, Some(_)) => 32,
        (None, None) => 100,
    };

    if !(1..=200).contains(&limit) {
        let msg = format!("limit not in 1..=200: {}", limit);
        return Err(HttpError::for_bad_request(None, msg));
    }

    let since = q.since.map(dt_to_cursor).transpose()?;
    let until = q.until.map(dt_to_cursor).transpose()?;

    let (collections, next_cursor) = storage
        .get_collections(limit, order, since, until)
        .await
        .map_err(|e| HttpError::for_internal_error(format!("oh shoot: {e:?}")))?;

    let next_cursor = next_cursor.map(|c| URL_SAFE_NO_PAD.encode(c));

    ok_cors(CollectionsResponse {
        collections,
        cursor: next_cursor,
    })
}

#[derive(Debug, Deserialize, JsonSchema)]
struct CollectionTimeseriesQuery {
    collection: String, // JsonSchema not implemented for Nsid :(
    /// Limit collections and statistics to those seen after this UTC datetime
    ///
    /// default: 1 week ago
    since: Option<DateTime<Utc>>,
    /// Limit collections and statistics to those seen before this UTC datetime
    ///
    /// default: now
    until: Option<DateTime<Utc>>,
    /// time steps between data, in seconds
    ///
    /// the step will be rounded down to the nearest hour
    ///
    /// default: 86400 (24hrs)
    #[schemars(range(min = 3600))]
    step: Option<u64>,
    // todo: rolling averages
}
#[derive(Debug, Serialize, JsonSchema)]
struct CollectionTimeseriesResponse {
    range: Vec<DateTime<Utc>>,
    series: HashMap<String, Vec<JustCount>>,
}
/// Get timeseries data
#[endpoint {
    method = GET,
    path = "/timeseries"
}]
async fn get_timeseries(
    ctx: RequestContext<Context>,
    query: Query<CollectionTimeseriesQuery>,
) -> OkCorsResponse<CollectionTimeseriesResponse> {
    let Context { storage, .. } = ctx.context();
    let q = query.into_inner();

    let since = q.since.map(dt_to_cursor).transpose()?.unwrap_or_else(|| {
        let week_ago_secs = 7 * 86_400;
        let week_ago = SystemTime::now() - Duration::from_secs(week_ago_secs);
        Cursor::at(week_ago).into()
    });

    let until = q.until.map(dt_to_cursor).transpose()?;

    let step = if let Some(secs) = q.step {
        if secs < 3600 {
            let msg = format!("step is too small: {}", secs);
            return Err(HttpError::for_bad_request(None, msg));
        }
        (secs / 3600) * 3600 // trucate to hour
    } else {
        86_400
    };

    let nsid = Nsid::new(q.collection).map_err(|e| {
        HttpError::for_bad_request(None, format!("collection was not a valid NSID: {:?}", e))
    })?;

    let (range_cursors, series) = storage
        .get_timeseries(vec![nsid], since, until, step)
        .await
        .map_err(|e| HttpError::for_internal_error(format!("oh shoot: {e:?}")))?;

    let range = range_cursors
        .into_iter()
        .map(|c| DateTime::<Utc>::from_timestamp_micros(c.to_raw_u64() as i64).unwrap())
        .collect();

    let series = series
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.iter().map(Into::into).collect()))
        .collect();

    ok_cors(CollectionTimeseriesResponse { range, series })
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
    api.register(get_collections).unwrap();
    api.register(get_timeseries).unwrap();

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
