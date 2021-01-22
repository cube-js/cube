use crate::config::ConfigObj;
use crate::remotefs::{RemoteFile, RemoteFs};
use crate::CubeError;
use async_trait::async_trait;
use core::fmt;
use deadqueue::unlimited;
use log::error;
use smallvec::alloc::fmt::Formatter;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::{broadcast, watch, RwLock};

pub struct QueueRemoteFs {
    config: Arc<dyn ConfigObj>,
    remote_fs: Arc<dyn RemoteFs>,
    upload_queue: unlimited::Queue<RemoteFsOp>,
    download_queue: unlimited::Queue<RemoteFsOp>,
    // TODO not used
    deleted: RwLock<HashSet<String>>,
    downloading: RwLock<HashSet<String>>,
    _result_receiver: broadcast::Receiver<RemoteFsOpResult>,
    result_sender: broadcast::Sender<RemoteFsOpResult>,
    stopped_rx: watch::Receiver<bool>,
    stopped_tx: watch::Sender<bool>,
}

impl Debug for QueueRemoteFs {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.remote_fs.fmt(f)
    }
}

#[derive(Debug)]
pub enum RemoteFsOp {
    Upload(String),
    Delete(String),
    Download(String),
}

#[derive(Debug, Clone)]
pub enum RemoteFsOpResult {
    Upload(String, Result<(), CubeError>),
    Delete(String, Result<(), CubeError>),
    Download(String, Result<String, CubeError>),
}

impl QueueRemoteFs {
    pub fn new(config: Arc<dyn ConfigObj>, remote_fs: Arc<dyn RemoteFs>) -> Arc<Self> {
        let (stopped_tx, stopped_rx) = watch::channel(false);
        let (tx, rx) = broadcast::channel(16384);
        Arc::new(Self {
            config,
            remote_fs,
            upload_queue: unlimited::Queue::new(),
            download_queue: unlimited::Queue::new(),
            deleted: RwLock::new(HashSet::new()),
            downloading: RwLock::new(HashSet::new()),
            result_sender: tx,
            _result_receiver: rx,
            stopped_tx,
            stopped_rx,
        })
    }

    pub fn start_processing_loops(queue_remote_fs: Arc<Self>) {
        for _ in 0..queue_remote_fs.config.upload_concurrency() {
            let to_move = queue_remote_fs.clone();
            tokio::spawn(async move {
                let mut stopped_rx = to_move.stopped_rx.clone();
                loop {
                    let to_process = tokio::select! {
                        to_process = to_move.upload_queue.pop() => {
                            to_process
                        }
                        stopped = stopped_rx.recv() => {
                            if let Some(true) = stopped {
                                return;
                            }
                            continue;
                        }
                    };

                    if let Err(err) = to_move.upload_loop(to_process).await {
                        error!("Error during upload: {:?}", err);
                    }
                }
            });
        }

        for _ in 0..queue_remote_fs.config.download_concurrency() {
            let to_move = queue_remote_fs.clone();
            tokio::spawn(async move {
                let mut stopped_rx = to_move.stopped_rx.clone();
                loop {
                    let to_process = tokio::select! {
                        to_process = to_move.download_queue.pop() => {
                            to_process
                        }
                        stopped = stopped_rx.recv() => {
                            if let Some(true) = stopped {
                                return;
                            }
                            continue;
                        }
                    };

                    if let Err(err) = to_move.download_loop(to_process).await {
                        error!("Error during download: {:?}", err);
                    }
                }
            });
        }
    }

    pub fn stop_processing_loops(&self) -> Result<(), CubeError> {
        Ok(self.stopped_tx.broadcast(true)?)
    }

    async fn upload_loop(&self, to_process: RemoteFsOp) -> Result<(), CubeError> {
        match to_process {
            RemoteFsOp::Upload(file) => {
                if !self.deleted.read().await.contains(file.as_str()) {
                    self.result_sender.send(RemoteFsOpResult::Upload(
                        file.to_string(),
                        self.remote_fs.upload_file(file.as_str()).await,
                    ))?;
                }
            }
            RemoteFsOp::Delete(file) => {
                self.result_sender.send(RemoteFsOpResult::Delete(
                    file.to_string(),
                    self.remote_fs.delete_file(file.as_str()).await,
                ))?;
            }
            x => panic!("Unexpected operation: {:?}", x),
        }
        Ok(())
    }

    async fn download_loop(&self, to_process: RemoteFsOp) -> Result<(), CubeError> {
        match to_process {
            RemoteFsOp::Download(file) => {
                let result = self.remote_fs.download_file(file.as_str()).await;
                let mut downloading = self.downloading.write().await;
                self.result_sender
                    .send(RemoteFsOpResult::Download(file.to_string(), result))?;
                downloading.remove(&file);
            }
            x => panic!("Unexpected operation: {:?}", x),
        }
        Ok(())
    }
}

#[async_trait]
impl RemoteFs for QueueRemoteFs {
    async fn upload_file(&self, remote_path: &str) -> Result<(), CubeError> {
        let mut receiver = self.result_sender.subscribe();
        self.upload_queue
            .push(RemoteFsOp::Upload(remote_path.to_string()));
        loop {
            let res = receiver.recv().await?;
            if let RemoteFsOpResult::Upload(file, result) = res {
                if &file == remote_path {
                    return result;
                }
            }
        }
    }

    async fn download_file(&self, remote_path: &str) -> Result<String, CubeError> {
        let mut receiver = self.result_sender.subscribe();
        {
            let mut downloading = self.downloading.write().await;
            if !downloading.contains(remote_path) {
                self.download_queue
                    .push(RemoteFsOp::Download(remote_path.to_string()));
                downloading.insert(remote_path.to_string());
            }
        }
        loop {
            let res = receiver.recv().await?;
            if let RemoteFsOpResult::Download(file, result) = res {
                if &file == remote_path {
                    return result;
                }
            }
        }
    }

    async fn delete_file(&self, remote_path: &str) -> Result<(), CubeError> {
        let mut receiver = self.result_sender.subscribe();
        self.upload_queue
            .push(RemoteFsOp::Delete(remote_path.to_string()));
        loop {
            let res = receiver.recv().await?;
            if let RemoteFsOpResult::Delete(file, result) = res {
                if &file == remote_path {
                    return result;
                }
            }
        }
    }

    async fn list(&self, remote_prefix: &str) -> Result<Vec<String>, CubeError> {
        self.remote_fs.list(remote_prefix).await
    }

    async fn list_with_metadata(&self, remote_prefix: &str) -> Result<Vec<RemoteFile>, CubeError> {
        self.remote_fs.list_with_metadata(remote_prefix).await
    }

    async fn local_path(&self) -> String {
        self.remote_fs.local_path().await
    }

    async fn local_file(&self, remote_path: &str) -> Result<String, CubeError> {
        self.remote_fs.local_file(remote_path).await
    }
}
