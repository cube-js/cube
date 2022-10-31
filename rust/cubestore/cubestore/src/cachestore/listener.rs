use crate::cachestore::QueueResultAckEvent;
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

    pub async fn wait_for_queue_ack(
        mut self,
        path: String,
    ) -> Result<Option<QueueResultAckEvent>, CubeError> {
        loop {
            let event = self.receiver.recv().await?;
            if let MetaStoreEvent::AckQueueItem(payload) = event {
                if payload.path == path {
                    return Ok(Some(payload));
                }
            }
        }
    }
}
