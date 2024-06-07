use crate::cachestore::{QueueKey, QueueResultAckEvent};
use crate::metastore::MetaStoreEvent;
use crate::CubeError;
use tokio::sync::broadcast::Receiver;

pub struct RocksCacheStoreListener {
    receiver: Receiver<MetaStoreEvent>,
}

impl RocksCacheStoreListener {
    pub fn new(receiver: Receiver<MetaStoreEvent>) -> Self {
        Self { receiver }
    }

    pub async fn wait_for_queue_ack_by_key(
        self,
        key: QueueKey,
    ) -> Result<Option<QueueResultAckEvent>, CubeError> {
        match key {
            QueueKey::ById(id) => self.wait_for_queue_ack_by_id(id).await,
            QueueKey::ByPath(path) => self.wait_for_queue_ack_by_path(path).await,
        }
    }

    pub async fn wait_for_queue_ack_by_id(
        mut self,
        id: u64,
    ) -> Result<Option<QueueResultAckEvent>, CubeError> {
        loop {
            let event = self.receiver.recv().await?;
            if let MetaStoreEvent::AckQueueItem(ack_event) = event {
                if ack_event.id == id {
                    return Ok(Some(ack_event));
                }
            }
        }
    }

    pub async fn wait_for_queue_ack_by_path(
        mut self,
        path: String,
    ) -> Result<Option<QueueResultAckEvent>, CubeError> {
        loop {
            let event = self.receiver.recv().await?;
            if let MetaStoreEvent::AckQueueItem(ack_event) = event {
                if ack_event.path == path {
                    return Ok(Some(ack_event));
                }
            }
        }
    }
}
