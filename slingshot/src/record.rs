use serde_json::value::RawValue;
use serde::{Serialize, Deserialize};
use jetstream::exports::Cid;

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
            RawValue::from_string(record.to_string()).expect("stored string from RawValue to be valid"),
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CachedRecord {
    Found(RawRecord),
    Deleted,
}
