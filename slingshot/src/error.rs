use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConsumerError {
    #[error(transparent)]
    JetstreamConnectionError(#[from] jetstream::error::ConnectionError),
    #[error(transparent)]
    JetstreamConfigValidationError(#[from] jetstream::error::ConfigValidationError),
    #[error("jetstream ended")]
    JetstreamEnded,
    #[error("delay queue output dropped")]
    DelayQueueOutputDropped,
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("server exited: {0}")]
    ServerExited(String),
}

#[derive(Debug, Error)]
pub enum IdentityError {
    #[error("whatever: {0}")]
    WhateverError(String),
    #[error("bad DID: {0}")]
    BadDid(&'static str),
    #[error("identity types got mixed up: {0}")]
    IdentityValTypeMixup(String),
    #[error("foyer error: {0}")]
    FoyerError(#[from] foyer::Error),

    #[error("failed to resolve: {0}")]
    ResolutionFailed(#[from] atrium_identity::Error),
    // #[error("identity resolved but no handle found for user")]
    // NoHandle,
    #[error("found handle {0:?} but it appears invalid: {1}")]
    InvalidHandle(String, &'static str),

    #[error("could not convert atrium did doc to partial mini doc: {0}")]
    BadDidDoc(String),

    #[error("wrong key for clearing refresh queue: {0}")]
    RefreshQueueKeyError(&'static str),
}

#[derive(Debug, Error)]
pub enum MainTaskError {
    #[error(transparent)]
    ConsumerTaskError(#[from] ConsumerError),
    #[error(transparent)]
    ServerTaskError(#[from] ServerError),
    #[error(transparent)]
    IdentityTaskError(#[from] IdentityError),
}

#[derive(Debug, Error)]
pub enum RecordError {
    #[error("identity error: {0}")]
    IdentityError(#[from] IdentityError),
    #[error("repo could not be validated as either a DID or an atproto handle")]
    BadRepo,
    #[error("could not get record: {0}")]
    NotFound(&'static str),
    #[error("could nto parse pds url: {0}")]
    UrlParseError(#[from] url::ParseError),
    #[error("reqwest send failed: {0}")]
    SendError(reqwest::Error),
    #[error("reqwest raised for status: {0}")]
    StatusError(reqwest::Error),
    #[error("reqwest failed to parse json: {0}")]
    ParseJsonError(reqwest::Error),
    #[error("upstream getRecord did not include a CID")]
    MissingUpstreamCid,
    #[error("upstream CID was not valid: {0}")]
    BadUpstreamCid(String),
}
