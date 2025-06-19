use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConsumerError {
    #[error(transparent)]
    JetstreamConnectionError(#[from] jetstream::error::ConnectionError),
    #[error(transparent)]
    JetstreamConfigValidationError(#[from] jetstream::error::ConfigValidationError),
    #[error(transparent)]
    JsonParseError(#[from] tinyjson::JsonParseError),
    #[error("jetstream ended")]
    JetstreamEnded
}
