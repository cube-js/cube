use crate::CubeError;
use async_trait::async_trait;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum ShutdownMode {
    // Note that these values are ordered from least-urgent to most-urgent.

    // Postgres "Smart" mode leaves connections up until the client terminates them.
    Smart,
    // Shuts down connections when they have no pending operations.
    SemiFast,
    // Sends fatal error messages to clients and shuts down as soon as it can.  Same as Postgres "Fast" mode.
    Fast,
}

#[async_trait]
pub trait ProcessingLoop: Send + Sync {
    async fn processing_loop(&self) -> Result<(), CubeError>;

    async fn stop_processing(&self, mode: ShutdownMode) -> Result<(), CubeError>;
}
