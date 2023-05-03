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
