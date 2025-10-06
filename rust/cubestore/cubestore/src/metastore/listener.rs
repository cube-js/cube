use crate::metastore::MetaStoreEvent;
use crate::CubeError;
use async_trait::async_trait;
use log::error;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;
use tokio::sync::Notify;

#[async_trait]
pub trait MetastoreListener: Send + Sync {
    async fn wait_for_event(&self, event_fn: MetastoreListenerWaitFun) -> Result<(), CubeError>;
}

pub type MetastoreListenerWaitFun = Box<dyn Fn(&MetaStoreEvent) -> bool + Send + Sync>;
pub struct MetastoreListenerImpl {
    event_receiver: Mutex<Receiver<MetaStoreEvent>>,
    wait_fns: Mutex<Vec<(Arc<Notify>, MetastoreListenerWaitFun)>>,
}

#[async_trait]
impl MetastoreListener for MetastoreListenerImpl {
    async fn wait_for_event(&self, event_fn: MetastoreListenerWaitFun) -> Result<(), CubeError> {
        let notify = Arc::new(Notify::new());
        self.wait_fns.lock().await.push((notify.clone(), event_fn));
        notify.notified().await;
        Ok(())
    }
}

pub struct MockMetastoreListener;

#[async_trait]
impl MetastoreListener for MockMetastoreListener {
    async fn wait_for_event(&self, _event_fn: MetastoreListenerWaitFun) -> Result<(), CubeError> {
        Ok(())
    }
}

impl MockMetastoreListener {
    pub fn new() -> MockMetastoreListener {
        MockMetastoreListener
    }
}

impl MetastoreListenerImpl {
    pub fn new(event_receiver: Receiver<MetaStoreEvent>) -> Arc<MetastoreListenerImpl> {
        Arc::new(MetastoreListenerImpl {
            event_receiver: Mutex::new(event_receiver),
            wait_fns: Mutex::new(Vec::new()),
        })
    }

    pub async fn run_listener(&self) -> Result<(), CubeError> {
        loop {
            let event = self.event_receiver.lock().await.recv().await?;
            let res = self.process_event(&event).await;
            if let Err(e) = res {
                error!("Error processing event {:?}: {}", event, e);
            }
        }
    }

    pub async fn run_listener_until(
        &self,
        last_event_fn: impl Fn(MetaStoreEvent) -> bool,
    ) -> Result<(), CubeError> {
        loop {
            let event = self.event_receiver.lock().await.recv().await?;
            let res = self.process_event(&event).await;
            if let Err(e) = res {
                error!("Error processing event {:?}: {}", event, e);
            }
            if last_event_fn(event) {
                return Ok(());
            }
        }
    }

    async fn process_event(&self, event: &MetaStoreEvent) -> Result<(), CubeError> {
        let mut wait_fns = self.wait_fns.lock().await;
        let mut to_notify = Vec::new();

        wait_fns.retain(|(notify, wait_fn)| {
            if wait_fn(event) {
                to_notify.push(notify.clone());
                false
            } else {
                true
            }
        });

        drop(wait_fns);

        for notify in to_notify {
            notify.notify_waiters();
        }

        Ok(())
    }
}
