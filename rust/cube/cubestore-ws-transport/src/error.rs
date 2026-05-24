use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("invalid url: {0}")]
    InvalidUrl(String),

    #[error("auth header build error: {0}")]
    Auth(String),

    #[error("connect failed: {0}")]
    Connect(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("query error: {0}")]
    Query(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("disconnected")]
    Disconnected,

    #[error("client closed")]
    Closed,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
