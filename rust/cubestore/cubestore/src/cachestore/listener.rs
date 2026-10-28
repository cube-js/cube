use crate::cachestore::QueueResultAckEvent;
use crate::metastore::MetaStoreEvent;
use crate::CubeError;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;

pub struct RocksCacheStoreListener {
    receiver: Receiver<MetaStoreEvent>,
}

impl RocksCacheStoreListener {
    pub fn new(receiver: Receiver<MetaStoreEvent>) -> Self {
        Self { receiver }
    }

    /// Returns `None` when the receiver lagged behind the channel: the ack might have been
    /// dropped, so the caller has to check the store before waiting again.
    pub async fn wait_for_queue_ack_by_id(
        &mut self,
        id: u64,
    ) -> Result<Option<QueueResultAckEvent>, CubeError> {
        loop {
            let event = match self.receiver.recv().await {
                Ok(event) => event,
                Err(RecvError::Lagged(_)) => return Ok(None),
                Err(e) => return Err(e.into()),
            };
            if let MetaStoreEvent::AckQueueItem(ack_event) = event {
                if ack_event.id == id {
                    return Ok(Some(ack_event));
                }
            }
        }
    }
}
