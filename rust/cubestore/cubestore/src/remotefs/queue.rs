use crate::config::ConfigObj;
use crate::di_service;
use crate::remotefs::{RemoteFile, RemoteFs};
use crate::util::lock::acquire_lock;
use crate::CubeError;
use async_trait::async_trait;
use core::fmt;
use datafusion::cube_ext;
use deadqueue::unlimited;
use futures::future::join_all;
use log::error;
use smallvec::alloc::fmt::Formatter;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fs::Metadata;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{broadcast, watch, RwLock};
use tokio::time::Duration;

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
        f.debug_struct("QueueRemoteFs")
            .field("remote_fs", &self.remote_fs)
            .finish()
    }
}

#[derive(Debug)]
pub enum RemoteFsOp {
    Upload {
        temp_upload_path: String,
        remote_path: String,
    },
    Delete(String),
    Download(String, Option<u64>),
}

#[derive(Debug, Clone)]
pub enum RemoteFsOpResult {
    Upload(String, Result<u64, CubeError>),
    Delete(String, Result<(), CubeError>),
    Download(String, Result<String, CubeError>),
}

di_service!(QueueRemoteFs, [RemoteFs]);

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

    pub async fn wait_processing_loops(queue_remote_fs: Arc<Self>) -> Result<(), CubeError> {
        let mut futures = Vec::new();
        for _ in 0..queue_remote_fs.config.upload_concurrency() {
            let to_move = queue_remote_fs.clone();
            futures.push(cube_ext::spawn(async move {
                let mut stopped_rx = to_move.stopped_rx.clone();
                loop {
                    let to_process = tokio::select! {
                        to_process = to_move.upload_queue.pop() => {
                            to_process
                        }
                        res = stopped_rx.changed() => {
                            if res.is_err() || *stopped_rx.borrow() {
                                return;
                            }
                            continue;
                        }
                    };

                    if let Err(err) = to_move.upload_loop(to_process).await {
                        error!("Error during upload: {:?}", err);
                    }
                }
            }));
        }

        for _ in 0..queue_remote_fs.config.download_concurrency() {
            let to_move = queue_remote_fs.clone();
            futures.push(cube_ext::spawn(async move {
                let mut stopped_rx = to_move.stopped_rx.clone();
                loop {
                    let to_process = tokio::select! {
                        to_process = to_move.download_queue.pop() => {
                            to_process
                        }
                        res = stopped_rx.changed() => {
                            if res.is_err() || *stopped_rx.borrow() {
                                return;
                            }
                            continue;
                        }
                    };

                    if let Err(err) = to_move.download_loop(to_process).await {
                        error!("Error during download: {:?}", err);
                    }
                }
            }));
        }

        let to_move = queue_remote_fs.clone();
        if queue_remote_fs.config.upload_to_remote() {
            futures.push(tokio::task::spawn(async move {
                to_move.cleanup_loop().await;
            }));
        }
        join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }

    pub fn stop_processing_loops(&self) -> Result<(), CubeError> {
        Ok(self.stopped_tx.send(true)?)
    }

    async fn upload_loop(&self, to_process: RemoteFsOp) -> Result<(), CubeError> {
        match to_process {
            RemoteFsOp::Upload {
                temp_upload_path,
                remote_path,
            } => {
                if !acquire_lock("upload loop deleted", self.deleted.read())
                    .await?
                    .contains(remote_path.as_str())
                {
                    let res = self
                        .remote_fs
                        .upload_file(&temp_upload_path, &remote_path)
                        .await;
                    self.result_sender
                        .send(RemoteFsOpResult::Upload(remote_path, res))?;
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
            RemoteFsOp::Download(file, expected_file_size) => {
                let result = self
                    .remote_fs
                    .download_file(file.as_str(), expected_file_size)
                    .await;
                let mut downloading =
                    acquire_lock("download loop downloading", self.downloading.write()).await?;
                self.result_sender
                    .send(RemoteFsOpResult::Download(file.to_string(), result))?;
                downloading.remove(&file);
            }
            x => panic!("Unexpected operation: {:?}", x),
        }
        Ok(())
    }

    async fn cleanup_local_files_loop(&self) {
        let local_dir = self.local_path().await;
        let mut stopped_rx = self.stopped_rx.clone();
        let cleanup_interval = Duration::from_secs(self.config.local_files_cleanup_interval_secs());
        let cleanup_local_files_delay = Duration::from_secs(self.config.local_files_cleanup_delay_secs());

        loop {
            // Do the cleanup every now and then.
            tokio::select! {
                () = tokio::time::sleep(cleanup_interval) => {},
                res = stopped_rx.changed() => {
                    if res.is_err() || *stopped_rx.borrow() {
                        return;
                    }
                }
            }

            let local_dir_copy = local_dir.clone();
            let res_local_files = cube_ext::spawn_blocking(
                move || -> Result<(HashSet<String>, u64), std::io::Error> {
                    let mut local_files = HashSet::new();
                    let mut local_files_size = 0;
                    for res_entry in Path::new(&local_dir_copy).read_dir()? {
                        let entry = match res_entry {
                            Err(_) => continue, // ignore errors, might come from concurrent fs ops.
                            Ok(e) => e,
                        };

                        let ft = match entry.file_type() {
                            Err(_) => continue,
                            Ok(ft) => ft,
                        };
                        if !ft.is_file() {
                            continue;
                        }

                        if let Ok(metadata) = entry.metadata() {
                            if metadata.created().elapsed().map_or(true, |e| e < cleanup_local_files_delay) {
                                continue;
                            }
                        } else {
                            continue;
                        };

                        let file_name = match entry.file_name().into_string() {
                            Err(_) => {
                                log::error!("could not convert file name {:?}", entry.file_name());
                                continue;
                            }
                            Ok(name) => name,
                        };

                        if file_name.ends_with("parquet") {
                            local_files.insert(file_name);
                        }

                    }
                    Ok(local_files)
                },
            )
            .await
            .unwrap();

            let mut local_files = match res_local_files {
                Err(e) => {
                    log::error!("error while trying to list local files: {}", e);
                    continue;
                }
                Ok(f) => f,
            };

            if local_files_size.is_empty() {
                continue;
            }

            let res_remote_files = self.list("").await;
            let remote_files = match res_remote_files {
                Err(e) => {
                    log::error!("could not get the list of remote files: {}", e);
                    continue;
                }
                Ok(f) => f,
            };

            // Only keep the files we want to remove in `local_files`.
            for f in remote_files {
                local_files.remove(&f);
            }

            if !local_files.is_empty() {
                log::debug!(
                    "Cleaning up {} files that were removed remotely",
                    local_files.len()
                );
                log::trace!("The files being removed are {:?}", local_files);
            }

            let local_dir_copy = local_dir.clone();
            cube_ext::spawn_blocking(move || {
                for f in local_files {
                    let _ = std::fs::remove_file(Path::new(&local_dir_copy).join(f));
                }
            })
            .await
            .unwrap();
        }

    }

    /// Periodically cleans up the local directory from the files removed on the remote side.
    /// This function currently removes only direct sibling files and does not touch subdirectories.
    /// So e.g. we remove the `.parquet` files, but not directories like `metastore` or heartbeat.
    ///
    /// Uploads typically live in the `uploads` directory while being prepared and only get moved
    /// to be direct siblings **after** appearing on the server.
    async fn cleanup_loop(&self) -> () {
        let local_dir = self.local_path().await;
        let mut stopped_rx = self.stopped_rx.clone();
        let cleanup_interval = Duration::from_secs(self.config.local_files_cleanup_interval_secs());
        let cleanup_files_size_threshold = self.config.local_files_cleanup_size_threshold();
        let cleanup_local_files_delay = self.config.local_files_cleanup_delay_secs();
        loop {
            // Do the cleanup every now and then.
            tokio::select! {
                () = tokio::time::sleep(cleanup_interval) => {},
                res = stopped_rx.changed() => {
                    if res.is_err() || *stopped_rx.borrow() {
                        return;
                    }
                }
            }

            // Important to collect local files **before** remote to avoid invalid removals.
            // We rely on RemoteFs implementations to upload the file to the server before they make
            // it available on the local filesystem.
            let local_dir_copy = local_dir.clone();
            let res_local_files = cube_ext::spawn_blocking(
                move || -> Result<(HashSet<String>, u64), std::io::Error> {
                    let mut local_files = HashSet::new();
                    let mut local_files_size = 0;
                    for res_entry in Path::new(&local_dir_copy).read_dir()? {
                        let entry = match res_entry {
                            Err(_) => continue, // ignore errors, might come from concurrent fs ops.
                            Ok(e) => e,
                        };

                        let ft = match entry.file_type() {
                            Err(_) => continue,
                            Ok(ft) => ft,
                        };
                        if !ft.is_file() {
                            continue;
                        }

                        local_files_size = if let Ok(metadata) = entry.metadata() {
                            metadata.len()
                        } else {
                            0
                        };

                        let file_name = match entry.file_name().into_string() {
                            Err(_) => {
                                log::error!("could not convert file name {:?}", entry.file_name());
                                continue;
                            }
                            Ok(name) => name,
                        };

                        local_files.insert(file_name);
                    }
                    Ok((local_files, local_files_size))
                },
            )
            .await
            .unwrap();

            let (mut local_files, local_files_size) = match res_local_files {
                Err(e) => {
                    log::error!("error while trying to list local files: {}", e);
                    continue;
                }
                Ok(f) => f,
            };

            if local_files_size < cleanup_files_size_threshold {
                continue;
            }

            let res_remote_files = self.list("").await;
            let remote_files = match res_remote_files {
                Err(e) => {
                    log::error!("could not get the list of remote files: {}", e);
                    continue;
                }
                Ok(f) => f,
            };

            // Only keep the files we want to remove in `local_files`.
            for f in remote_files {
                local_files.remove(&f);
            }

            if !local_files.is_empty() {
                log::debug!(
                    "Cleaning up {} files that were removed remotely",
                    local_files.len()
                );
                log::trace!("The files being removed are {:?}", local_files);
            }

            let local_dir_copy = local_dir.clone();
            cube_ext::spawn_blocking(move || {
                for f in local_files {
                    let _ = std::fs::remove_file(Path::new(&local_dir_copy).join(f));
                }
            })
            .await
            .unwrap();
        }
    }
}

#[async_trait]
impl RemoteFs for QueueRemoteFs {
    async fn upload_file(
        &self,
        local_upload_path: &str,
        remote_path: &str,
    ) -> Result<u64, CubeError> {
        if !self.config.upload_to_remote() {
            log::info!("Skipping upload {}", remote_path);
            return Ok(tokio::fs::metadata(local_upload_path).await?.len());
        }
        let mut receiver = self.result_sender.subscribe();
        self.upload_queue.push(RemoteFsOp::Upload {
            temp_upload_path: local_upload_path.to_string(),
            remote_path: remote_path.to_string(),
        });
        loop {
            let res = receiver.recv().await?;
            if let RemoteFsOpResult::Upload(file, result) = res {
                if &file == remote_path {
                    return result;
                }
            }
        }
    }

    async fn download_file(
        &self,
        remote_path: &str,
        expected_file_size: Option<u64>,
    ) -> Result<String, CubeError> {
        // We might be lucky and the file has already been downloaded.
        if let Ok(local_path) = self.local_file(remote_path).await {
            let metadata = tokio::fs::metadata(&local_path).await;
            if metadata.is_ok() {
                if let Err(e) = QueueRemoteFs::check_file_size(
                    remote_path,
                    expected_file_size,
                    &local_path,
                    metadata.unwrap(),
                )
                .await
                {
                    return Err(e);
                }
                return Ok(local_path);
            }
        }
        let mut receiver = self.result_sender.subscribe();
        {
            let mut downloading =
                acquire_lock("download file downloading", self.downloading.write()).await?;
            if !downloading.contains(remote_path) {
                self.download_queue.push(RemoteFsOp::Download(
                    remote_path.to_string(),
                    expected_file_size,
                ));
                downloading.insert(remote_path.to_string());
            }
        }
        loop {
            let res = receiver.recv().await?;
            if let RemoteFsOpResult::Download(file, result) = res {
                if &file == remote_path {
                    match result {
                        Ok(f) => {
                            let local_path = self.local_file(remote_path).await?;
                            let metadata = tokio::fs::metadata(&local_path).await.map_err(|e| {
                                CubeError::internal(format!(
                                    "Error while listing local file for consistency check {}: {}",
                                    local_path, e
                                ))
                            })?;
                            if let Err(e) = QueueRemoteFs::check_file_size(
                                remote_path,
                                expected_file_size,
                                &local_path,
                                metadata,
                            )
                            .await
                            {
                                return Err(e);
                            }
                            return Ok(f);
                        }
                        Err(err) => {
                            //Check if file doesn't exists in remoteFs
                            if self.remote_fs.list(&file).await?.is_empty() {
                                return Err(CubeError::corrupt_data(format!(
                                    "File {} doesn't exist in remote file system",
                                    file
                                )));
                            }
                            return Err(err);
                        }
                    }
                }
            }
        }
    }

    async fn delete_file(&self, remote_path: &str) -> Result<(), CubeError> {
        if !self.config.upload_to_remote() {
            log::info!("Skipping delete {}", remote_path);
            return Ok(());
        }
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

impl QueueRemoteFs {
    async fn check_file_size(
        remote_path: &str,
        expected_file_size: Option<u64>,
        local_path: &str,
        metadata: Metadata,
    ) -> Result<(), CubeError> {
        if let Some(expected_file_size) = expected_file_size {
            let actual_size = metadata.len();
            if actual_size != expected_file_size {
                tokio::fs::remove_file(local_path).await?;
                return Err(CubeError::corrupt_data(format!(
                    "Expected file size for '{}' is {} but {} received",
                    remote_path, expected_file_size, actual_size
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::config::Config;
    use crate::remotefs::LocalDirRemoteFs;
    use std::env;
    use std::fs::File;
    use std::io::Write;
    enum MockFSError {
        None,
        WrongSize,
        MissingFile,
    }
    struct MockFs {
        base_fs: Arc<LocalDirRemoteFs>,
        error: MockFSError,
        download_error: bool,
    }
    impl Debug for MockFs {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            f.debug_struct("MockFs").finish()
        }
    }

    di_service!(MockFs, [RemoteFs]);

    #[async_trait]
    impl RemoteFs for MockFs {
        async fn upload_file(
            &self,
            local_upload_path: &str,
            remote_path: &str,
        ) -> Result<u64, CubeError> {
            let res = self
                .base_fs
                .upload_file(local_upload_path, remote_path)
                .await;
            if let Ok(size) = res {
                self.check_upload_file(remote_path, size).await?
            }
            res
        }

        async fn download_file(
            &self,
            remote_path: &str,
            expected_file_size: Option<u64>,
        ) -> Result<String, CubeError> {
            let res = self
                .base_fs
                .download_file(remote_path, expected_file_size)
                .await;
            if self.download_error {
                return Err(CubeError::internal("test download error".to_string()));
            }
            res
        }

        async fn delete_file(&self, _remote_path: &str) -> Result<(), CubeError> {
            Ok(())
        }

        async fn list(&self, remote_prefix: &str) -> Result<Vec<String>, CubeError> {
            self.base_fs.list(remote_prefix).await
        }

        async fn list_with_metadata(
            &self,
            remote_prefix: &str,
        ) -> Result<Vec<RemoteFile>, CubeError> {
            let mut res = self
                .base_fs
                .list_with_metadata(remote_prefix)
                .await
                .unwrap();
            match self.error {
                MockFSError::MissingFile => {
                    res.remove(0);
                }
                MockFSError::WrongSize => {
                    res[0].file_size = 1;
                }
                MockFSError::None => {}
            }
            Ok(res)
        }

        async fn local_path(&self) -> String {
            self.base_fs.local_path().await
        }

        async fn local_file(&self, remote_path: &str) -> Result<String, CubeError> {
            self.base_fs.local_file(remote_path).await
        }
    }

    fn make_test_csv() -> std::path::PathBuf {
        let dir = env::temp_dir();

        let path = tempfile::Builder::new()
            .prefix("foo.csv")
            .tempfile_in(dir)
            .unwrap()
            .path()
            .to_path_buf();

        let mut file = File::create(path.clone()).unwrap();

        file.write_all("id,city,arr,t\n".as_bytes()).unwrap();
        file.write_all("1,San Francisco,\"[\"\"Foo\"\",\"\"Bar\"\",\"\"FooBar\"\"]\",\"2021-01-24 12:12:23 UTC\"\n".as_bytes()).unwrap();
        file.write_all("2,\"New York\",\"[\"\"\"\"]\",2021-01-24 19:12:23 UTC\n".as_bytes())
            .unwrap();
        file.write_all("3,New York,,2021-01-25 19:12:23 UTC\n".as_bytes())
            .unwrap();
        file.write_all("4,New York,\"\",2021-01-25 19:12:23 UTC\n".as_bytes())
            .unwrap();
        file.write_all("5,New York,\"\",2021-01-25 19:12:23 UTC\n".as_bytes())
            .unwrap();

        path
    }
    #[tokio::test]
    async fn queue_upload() {
        let config = Config::test("upload_retries_all_fail");
        config.configure_injector().await;
        let failed_fs = Arc::new(MockFs {
            base_fs: config.injector().get_service("original_remote_fs").await,
            error: MockFSError::None,
            download_error: false,
        });
        let queue_fs = QueueRemoteFs::new(config.config_obj(), failed_fs.clone());

        let path = make_test_csv();

        let r = tokio::spawn(QueueRemoteFs::wait_processing_loops(queue_fs.clone()));
        let res = queue_fs
            .upload_file(path.to_str().unwrap(), "temp-upload/foo.csv")
            .await;
        queue_fs.stop_processing_loops().unwrap();
        r.await.unwrap().unwrap();
        assert!(res.is_ok());
        let _ = std::fs::remove_dir_all(config.local_dir());
        let _ = std::fs::remove_dir_all(config.remote_dir());
    }
    #[tokio::test]
    async fn queue_upload_wrong_size() {
        let config = Config::test("upload_upload_wrong_size");
        config.configure_injector().await;
        let failed_fs = Arc::new(MockFs {
            base_fs: config.injector().get_service("original_remote_fs").await,
            error: MockFSError::WrongSize,
            download_error: false,
        });
        let queue_fs = QueueRemoteFs::new(config.config_obj(), failed_fs.clone());

        let path = make_test_csv();

        let r = tokio::spawn(QueueRemoteFs::wait_processing_loops(queue_fs.clone()));
        let res = queue_fs
            .upload_file(path.to_str().unwrap(), "temp-upload/foo.csv")
            .await;
        queue_fs.stop_processing_loops().unwrap();
        r.await.unwrap().unwrap();
        assert!(res.is_err());
        let _ = std::fs::remove_dir_all(config.local_dir());
        let _ = std::fs::remove_dir_all(config.remote_dir());
    }
    #[tokio::test]
    async fn queue_upload_missing_file() {
        let config = Config::test("upload_missing_file");
        config.configure_injector().await;
        let failed_fs = Arc::new(MockFs {
            base_fs: config.injector().get_service("original_remote_fs").await,
            error: MockFSError::MissingFile,
            download_error: false,
        });
        let queue_fs = QueueRemoteFs::new(config.config_obj(), failed_fs.clone());

        let path = make_test_csv();

        let r = tokio::spawn(QueueRemoteFs::wait_processing_loops(queue_fs.clone()));
        let res = queue_fs
            .upload_file(path.to_str().unwrap(), "temp-upload/foo.csv")
            .await;
        queue_fs.stop_processing_loops().unwrap();
        r.await.unwrap().unwrap();
        assert!(res.is_err());
        let _ = std::fs::remove_dir_all(config.local_dir());
        let _ = std::fs::remove_dir_all(config.remote_dir());
    }
    #[tokio::test]
    async fn queue_download_missing_file() {
        let config = Config::test("download_missing_file");
        config.configure_injector().await;

        let queue_fs = QueueRemoteFs::new(
            config.config_obj(),
            config.injector().get_service("original_remote_fs").await,
        );
        let r = tokio::spawn(QueueRemoteFs::wait_processing_loops(queue_fs.clone()));
        let res = queue_fs.download_file("temp-upload/foo.csv", None).await;
        match res {
            Ok(_) => assert!(false),
            Err(e) => assert!(e.is_corrupt_data()),
        };
        queue_fs.stop_processing_loops().unwrap();
        r.await.unwrap().unwrap();
        let _ = std::fs::remove_dir_all(config.local_dir());
        let _ = std::fs::remove_dir_all(config.remote_dir());
    }
    #[tokio::test]
    async fn queue_download_wrong_file_size() {
        let config = Config::test("download_wrong_file_size");
        config.configure_injector().await;

        let path = make_test_csv();
        let queue_fs = QueueRemoteFs::new(
            config.config_obj(),
            config.injector().get_service("original_remote_fs").await,
        );
        let r = tokio::spawn(QueueRemoteFs::wait_processing_loops(queue_fs.clone()));
        queue_fs
            .upload_file(path.to_str().unwrap(), "temp-upload/foo.csv")
            .await
            .unwrap();

        std::fs::remove_file(queue_fs.local_file("temp-upload/foo.csv").await.unwrap()).unwrap();

        let res = queue_fs.download_file("temp-upload/foo.csv", Some(1)).await;

        match res {
            Ok(_) => assert!(false),
            Err(e) => assert!(e.is_corrupt_data()),
        };
        queue_fs.stop_processing_loops().unwrap();
        r.await.unwrap().unwrap();
        let _ = std::fs::remove_dir_all(config.local_dir());
        let _ = std::fs::remove_dir_all(config.remote_dir());
    }
    #[tokio::test]
    async fn queue_download_remotefs_error() {
        let config = Config::test("download_remotefs_error");
        config.configure_injector().await;

        let failed_fs = Arc::new(MockFs {
            base_fs: config.injector().get_service("original_remote_fs").await,
            error: MockFSError::None,
            download_error: true,
        });
        let path = make_test_csv();
        let queue_fs = QueueRemoteFs::new(config.config_obj(), failed_fs.clone());

        let r = tokio::spawn(QueueRemoteFs::wait_processing_loops(queue_fs.clone()));

        queue_fs
            .upload_file(path.to_str().unwrap(), "temp-upload/foo.csv")
            .await
            .unwrap();
        std::fs::remove_file(queue_fs.local_file("temp-upload/foo.csv").await.unwrap()).unwrap();

        let res = queue_fs.download_file("temp-upload/foo.csv", None).await;

        match res {
            Ok(_) => assert!(false),
            Err(e) => {
                assert!(!e.is_corrupt_data());
                assert_eq!(e.message, "test download error")
            }
        };
        queue_fs.stop_processing_loops().unwrap();
        r.await.unwrap().unwrap();
        let _ = std::fs::remove_dir_all(config.local_dir());
        let _ = std::fs::remove_dir_all(config.remote_dir());
    }
}
