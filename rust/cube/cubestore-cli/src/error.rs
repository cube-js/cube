use cubestore_ws_transport::arrow::error::ArrowError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CliError {
    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),
}
