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
pub enum MainTaskError {
    #[error(transparent)]
    ConsumerTaskError(#[from] ConsumerError),
    #[error(transparent)]
    ServerTaskError(#[from] ServerError),
}
