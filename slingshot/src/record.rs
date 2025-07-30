//! cached record storage

use crate::{Identity, error::RecordError};
use atrium_api::types::string::{Cid, Did, Handle};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::str::FromStr;
use std::time::Duration;
use url::Url;

#[derive(Debug, Serialize, Deserialize)]
pub struct RawRecord {
    cid: Cid,
    record: String,
}

// TODO: should be able to do typed CID
impl From<(Cid, Box<RawValue>)> for RawRecord {
    fn from((cid, rv): (Cid, Box<RawValue>)) -> Self {
        Self {
            cid,
            record: rv.get().to_string(),
        }
    }
}

/// only for use with stored (validated) values, not general strings
impl From<&RawRecord> for (Cid, Box<RawValue>) {
    fn from(RawRecord { cid, record }: &RawRecord) -> Self {
        (
            cid.clone(),
            RawValue::from_string(record.to_string())
                .expect("stored string from RawValue to be valid"),
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CachedRecord {
    Found(RawRecord),
    Deleted,
}

//////// upstream record fetching

#[derive(Deserialize)]
struct RecordResponseObject {
    #[allow(dead_code)] // expect it to be there but we ignore it
    uri: String,
    /// CID for this exact version of the record
    ///
    /// this is optional in the spec and that's potentially TODO for slingshot
    cid: Option<String>,
    /// the record itself as JSON
    value: Box<RawValue>,
}

#[derive(Clone)]
pub struct Repo {
    identity: Identity,
    client: Client,
}

impl Repo {
    pub fn new(identity: Identity) -> Self {
        let client = Client::builder()
            .user_agent(format!(
                "microcosm slingshot v{} (dev: @bad-example.com)",
                env!("CARGO_PKG_VERSION")
            ))
            .no_proxy()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();
        Repo { identity, client }
    }

    pub async fn get_record(
        &self,
        did_or_handle: String,
        collection: String,
        rkey: String,
        cid: Option<String>,
    ) -> Result<CachedRecord, RecordError> {
        let did = match Did::new(did_or_handle.clone()) {
            Ok(did) => did,
            Err(_) => {
                let handle = Handle::new(did_or_handle).map_err(|_| RecordError::BadRepo)?;
                let Some(did) = self.identity.handle_to_did(handle).await? else {
                    return Err(RecordError::NotFound("could not resolve and verify handle"));
                };
                did
            }
        };
        let Some(pds) = self.identity.did_to_pds(did.clone()).await? else {
            return Err(RecordError::NotFound("could not get pds for DID"));
        };

        // TODO: throttle by host probably, generally guard against outgoing requests

        let mut params = vec![
            ("repo", did.to_string()),
            ("collection", collection),
            ("rkey", rkey),
        ];
        if let Some(cid) = cid {
            params.push(("cid", cid));
        }
        let mut url = Url::parse_with_params(&pds, &params)?;
        url.set_path("/xrpc/com.atproto.repo.getRecord");

        let res = self
            .client
            .get(url)
            .send()
            .await
            .map_err(RecordError::SendError)?
            .error_for_status()
            .map_err(RecordError::StatusError)? // TODO atproto error handling (think about handling not found)
            .json::<RecordResponseObject>()
            .await
            .map_err(RecordError::ParseJsonError)?; // todo...

        let Some(cid) = res.cid else {
            return Err(RecordError::MissingUpstreamCid);
        };
        let cid = Cid::from_str(&cid).map_err(|e| RecordError::BadUpstreamCid(e.to_string()))?;

        Ok(CachedRecord::Found(RawRecord {
            cid,
            record: res.value.to_string(),
        }))
    }
}
