use thiserror::Error;

#[derive(Debug, Error)]
pub enum MainTaskError {
    #[error(transparent)]
    ConsumerTaskError(#[from] ConsumerError),
    #[error(transparent)]
    ServerTaskError(#[from] ServerError),
    #[error(transparent)]
    DelayTaskError(#[from] DelayError),
}

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
pub enum DelayError {
    #[error("delay ended")]
    DelayEnded,
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("failed to configure server logger: {0}")]
    ConfigLogError(std::io::Error),
    #[error("failed to render json for openapi: {0}")]
    OpenApiJsonFail(serde_json::Error),
    #[error(transparent)]
    FailedToBuildServer(#[from] dropshot::BuildError),
    #[error("server exited: {0}")]
    ServerExited(String),
    #[error("server closed badly: {0}")]
    BadClose(String),
}
