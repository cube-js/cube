use crate::metastore::MetaStoreEvent;
use crate::CubeError;
use tokio::sync::broadcast::Receiver;
use async_trait::async_trait;
use log::{error};
use tokio::sync::{Mutex};
use tokio::sync::Notify;
use std::sync::Arc;

#[async_trait]
pub trait MetastoreListener: Send + Sync {
    async fn wait_for_event(&self, event_fn: Box<dyn Fn(MetaStoreEvent) -> bool + Send + Sync>) -> Result<(), CubeError>;
}

pub struct MetastoreListenerImpl {
    event_receiver: Mutex<Receiver<MetaStoreEvent>>,
    wait_fns: Mutex<Vec<(Arc<Notify>, Box<dyn Fn(MetaStoreEvent) -> bool + Send + Sync>)>>
}

#[async_trait]
impl MetastoreListener for MetastoreListenerImpl {
    async fn wait_for_event(&self, event_fn: Box<dyn Fn(MetaStoreEvent) -> bool + Send + Sync>) -> Result<(), CubeError> {
        let notify = Arc::new(Notify::new());
        self.wait_fns.lock().await.push((notify.clone(), event_fn));
        notify.notified().await;
        Ok(())
    }
}

pub struct MockMetastoreListener;

#[async_trait]
impl MetastoreListener for MockMetastoreListener {
    async fn wait_for_event(&self, _event_fn: Box<dyn Fn(MetaStoreEvent) -> bool + Send + Sync>) -> Result<(), CubeError> {
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
        Arc::new(MetastoreListenerImpl { event_receiver: Mutex::new(event_receiver), wait_fns: Mutex::new(Vec::new()) })
    }

    pub async fn run_listener(&self) -> Result<(), CubeError> {
        loop {
            let event = self.event_receiver.lock().await.recv().await?;
            let res = self.process_event(event.clone()).await;
            if let Err(e) = res {
                error!("Error processing event {:?}: {}", event, e);
            }
        }
    }

    pub async fn run_listener_until(&self, last_event_fn: impl Fn(MetaStoreEvent) -> bool) -> Result<(), CubeError> {
        loop {
            let event = self.event_receiver.lock().await.recv().await?;
            let res = self.process_event(event.clone()).await;
            if let Err(e) = res {
                error!("Error processing event {:?}: {}", event, e);
            }
            if last_event_fn(event) {
                return Ok(())
            }
        }
    }

    async fn process_event(&self, event: MetaStoreEvent) -> Result<(), CubeError> {
        let mut wait_fns = self.wait_fns.lock().await;
        let to_notify = wait_fns.drain_filter(|(_, wait_fn)| wait_fn(event.clone())).collect::<Vec<_>>();
        for (notify, _) in to_notify {
            notify.notify();
        }
        Ok(())
    }
}