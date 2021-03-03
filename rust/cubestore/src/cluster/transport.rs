use crate::cluster::message::NetworkMessage;
use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::CubeError;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;

#[async_trait]
pub trait ClusterTransport: DIService {
    async fn send_to_worker(
        &self,
        worker_node: String,
        m: NetworkMessage,
    ) -> Result<NetworkMessage, CubeError>;
}

pub struct ClusterTransportImpl {
    config: Arc<dyn ConfigObj>,
}

crate::di_service!(ClusterTransportImpl, [ClusterTransport]);

impl ClusterTransportImpl {
    pub fn new(config: Arc<dyn ConfigObj>) -> Arc<Self> {
        Arc::new(Self { config })
    }
}

#[async_trait]
impl ClusterTransport for ClusterTransportImpl {
    async fn send_to_worker(
        &self,
        worker_node: String,
        m: NetworkMessage,
    ) -> Result<NetworkMessage, CubeError> {
        let mut stream = tokio::time::timeout(
            Duration::from_secs(self.config.connection_timeout()),
            TcpStream::connect(worker_node),
        )
        .await??;
        m.send(&mut stream).await?;
        return Ok(NetworkMessage::receive(&mut stream).await?);
    }
}

#[async_trait]
pub trait MetaStoreTransport: DIService {
    async fn meta_store_call(&self, m: NetworkMessage) -> Result<NetworkMessage, CubeError>;
}

pub struct MetaStoreTransportImpl {
    config: Arc<dyn ConfigObj>,
}

crate::di_service!(MetaStoreTransportImpl, [MetaStoreTransport]);

impl MetaStoreTransportImpl {
    pub fn new(config: Arc<dyn ConfigObj>) -> Arc<Self> {
        Arc::new(Self { config })
    }
}

#[async_trait]
impl MetaStoreTransport for MetaStoreTransportImpl {
    async fn meta_store_call(&self, m: NetworkMessage) -> Result<NetworkMessage, CubeError> {
        let meta_remote_addr = self
            .config
            .metastore_remote_address()
            .as_ref()
            .expect("Meta store remote addr is not defined")
            .to_string();
        let mut stream = tokio::time::timeout(
            Duration::from_secs(self.config.connection_timeout()),
            TcpStream::connect(meta_remote_addr),
        )
        .await??;
        m.send(&mut stream).await?;
        let message = NetworkMessage::receive(&mut stream).await?;
        Ok(message)
    }
}
