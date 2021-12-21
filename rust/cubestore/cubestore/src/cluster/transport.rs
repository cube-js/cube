use crate::cluster::message::NetworkMessage;
use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::CubeError;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;

/// Client-side connection for exchanging messages between the server and the client.
/// Created by [ClusterTransport].
#[async_trait]
pub trait WorkerConnection: Send + Sync {
    /// If connection is open, send the message to the server and return true.
    /// If connection is closed, return false.
    async fn maybe_send(&mut self, m: NetworkMessage) -> Result<bool, CubeError>;
    /// If connection is open, receive the message from the server and return it.
    /// If connection is closed, return None.
    async fn maybe_receive(&mut self) -> Result<Option<NetworkMessage>, CubeError>;
}

impl dyn WorkerConnection {
    pub async fn send(&mut self, m: NetworkMessage) -> Result<(), CubeError> {
        let sent = self.maybe_send(m).await?;
        if sent {
            Ok(())
        } else {
            Err(CubeError::internal("connection closed".to_string()))
        }
    }

    pub async fn receive(&mut self) -> Result<NetworkMessage, CubeError> {
        match self.maybe_receive().await? {
            Some(m) => Ok(m),
            None => Err(CubeError::internal("connection closed".to_string())),
        }
    }
}

#[async_trait]
pub trait ClusterTransport: DIService {
    async fn connect_to_worker(
        &self,
        worker_node: String,
    ) -> Result<Box<dyn WorkerConnection>, CubeError>;
}

impl dyn ClusterTransport {
    pub async fn send_to_worker(
        &self,
        worker_node: String,
        m: NetworkMessage,
    ) -> Result<NetworkMessage, CubeError> {
        let mut c = self.connect_to_worker(worker_node).await?;
        c.send(m).await?;
        c.receive().await
    }
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

struct Connection {
    stream: TcpStream,
}

#[async_trait]
impl WorkerConnection for Connection {
    async fn maybe_send(&mut self, m: NetworkMessage) -> Result<bool, CubeError> {
        m.maybe_send(&mut self.stream).await
    }

    async fn maybe_receive(&mut self) -> Result<Option<NetworkMessage>, CubeError> {
        NetworkMessage::maybe_receive(&mut self.stream).await
    }
}

#[async_trait]
impl ClusterTransport for ClusterTransportImpl {
    async fn connect_to_worker(
        &self,
        worker_node: String,
    ) -> Result<Box<dyn WorkerConnection>, CubeError> {
        let stream = tokio::time::timeout(
            Duration::from_secs(self.config.connection_timeout()),
            TcpStream::connect(worker_node.to_string()),
        )
        .await
        .map_err(|_| CubeError::internal(format!("Connection timeout to {}. Please check your worker connection env variables (CUBESTORE_WORKERS, CUBESTORE_WORKER_PORT, etc.).", worker_node)))?
        .map_err(|e| CubeError::internal(format!("Can't connect to {}: {}", worker_node, e)))?;
        Ok(Box::new(Connection { stream }))
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
            TcpStream::connect(&meta_remote_addr),
        )
        .await
        .map_err(|_| {
            CubeError::internal(format!(
                "Connection timeout to {}. Please check your meta connection env variables (CUBESTORE_META_ADDR, CUBESTORE_META_PORT, etc.).",
                meta_remote_addr
            ))
        })?
        .map_err(|e| {
            CubeError::internal(format!("Can't connect to {}: {}", meta_remote_addr, e))
        })?;
        m.send(&mut stream).await?;
        let message = NetworkMessage::receive(&mut stream).await?;
        Ok(message)
    }
}
