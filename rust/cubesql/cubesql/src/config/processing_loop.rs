use crate::CubeError;
use async_trait::async_trait;

#[async_trait]
pub trait ProcessingLoop: Send + Sync {
    async fn processing_loop(&self) -> Result<(), CubeError>;

    async fn stop_processing(&self) -> Result<(), CubeError>;
}
