use serde_json::value::RawValue;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct RawRecord(String);

impl From<Box<RawValue>> for RawRecord {
    fn from(rv: Box<RawValue>) -> Self {
        Self(rv.get().to_string())
    }
}

/// only for use with stored (validated) values, not general strings
impl From<RawRecord> for Box<RawValue> {
    fn from(RawRecord(s): RawRecord) -> Self {
        RawValue::from_string(s).expect("stored string from RawValue to be valid")
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CachedRecord {
    Found(RawRecord),
    Deleted,
}
