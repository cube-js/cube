use crate::config::ConfigObj;
use crate::metastore::MetaStore;
use crate::remotefs::RemoteFs;
use crate::{app_metrics, CubeError};
use chrono::Utc;
use datafusion::cube_ext;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

pub struct RemoteFsCleanup {
    config: Arc<dyn ConfigObj>,
    remote_fs: Arc<dyn RemoteFs>,
    metastore: Arc<dyn MetaStore>,
    stopped_token: CancellationToken,
}

crate::di_service!(RemoteFsCleanup, []);

impl RemoteFsCleanup {
    pub fn new(
        config: Arc<dyn ConfigObj>,
        remote_fs: Arc<dyn RemoteFs>,
        metastore: Arc<dyn MetaStore>,
    ) -> Self {
        Self {
            config,
            remote_fs,
            metastore,
            stopped_token: CancellationToken::new(),
        }
    }

    pub async fn wait_local_cleanup_loop(&self) {
        self.cleanup_local_files_loop().await;
    }

    #[allow(dead_code)]
    pub async fn wait_remote_fs_cleanup_loop(&self, stat_tags: Vec<String>) {
        self.cleanup_remotefs_loop(stat_tags).await;
    }

    pub fn stop(&self) {
        self.stopped_token.cancel()
    }

    async fn cleanup_remotefs_loop(&self, stat_tags: Vec<String>) -> () {
        let token = self.stopped_token.child_token();
        let cleanup_interval =
            Duration::from_secs(self.config.remote_files_cleanup_interval_secs());
        let cleanup_local_files_delay = self.config.remote_files_cleanup_delay_secs() as i64;
        let remote_fs = self.remote_fs.clone();
        loop {
            // Do the cleanup every now and then.
            tokio::select! {
                () = tokio::time::sleep(cleanup_interval) => {},
                _ = token.cancelled() => {
                    return;
                }
            }

            let res_remote_files = remote_fs.list_with_metadata("").await;
            let remote_files = match res_remote_files {
                Err(e) => {
                    log::error!("could not get the list of remote files: {}", e);
                    continue;
                }
                Ok(f) => f,
            };

            let files_from_metastore = match self.metastore.get_all_filenames().await {
                Err(e) => {
                    log::error!("could not get the list of files from metastore: {}", e);
                    continue;
                }
                Ok(f) => f.into_iter().collect::<HashSet<_>>(),
            };

            let mut files_to_remove = Vec::new();
            let mut files_to_remove_size = 0;

            for f in remote_files {
                if files_from_metastore.get(f.remote_path()).is_some() {
                    continue;
                }
                if Utc::now()
                    .signed_duration_since(f.updated().clone())
                    .num_seconds()
                    < cleanup_local_files_delay
                {
                    continue;
                }
                files_to_remove.push(f.remote_path().to_string());
                files_to_remove_size += f.file_size;
            }
            if !files_to_remove.is_empty() {
                app_metrics::REMOTE_FS_FILES_TO_REMOVE
                    .report_with_tags(files_to_remove.len() as i64, Some(&stat_tags));

                app_metrics::REMOTE_FS_FILES_SIZE_TO_REMOVE
                    .report_with_tags(files_to_remove_size as i64, Some(&stat_tags));
            }
        }
    }

    async fn cleanup_local_files_loop(&self) {
        let token = self.stopped_token.child_token();
        let remote_fs = self.remote_fs.clone();
        let local_dir = remote_fs.local_path().await;
        let cleanup_interval = Duration::from_secs(self.config.local_files_cleanup_interval_secs());
        let cleanup_local_files_delay =
            Duration::from_secs(self.config.local_files_cleanup_delay_secs());

        loop {
            // Do the cleanup every now and then.
            tokio::select! {
                () = tokio::time::sleep(cleanup_interval) => {},
                _ = token.cancelled() => {
                    return;
                }
            }

            let local_dir_copy = local_dir.clone();
            let res_local_files =
                cube_ext::spawn_blocking(move || -> Result<HashSet<String>, std::io::Error> {
                    let mut local_files = HashSet::new();
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

                        let file_name = match entry.file_name().into_string() {
                            Err(_) => {
                                log::error!("could not convert file name {:?}", entry.file_name());
                                continue;
                            }
                            Ok(name) => name,
                        };

                        if !file_name.ends_with(".parquet") {
                            continue;
                        }

                        let should_deleted = if let Ok(metadata) = entry.metadata() {
                            match metadata.created() {
                                Ok(created) => {
                                    if created
                                        .elapsed()
                                        .map_or(true, |e| e < cleanup_local_files_delay)
                                    {
                                        false
                                    } else {
                                        true
                                    }
                                }
                                Err(e) => {
                                    log::error!(
                                        "error while getting created time for file {:?}:{}",
                                        entry.file_name(),
                                        e
                                    );
                                    false
                                }
                            }
                        } else {
                            false
                        };
                        if !should_deleted {
                            continue;
                        }

                        local_files.insert(file_name);
                    }
                    Ok(local_files)
                })
                .await
                .unwrap();

            let mut local_files = match res_local_files {
                Err(e) => {
                    log::error!("error while trying to list local files: {}", e);
                    continue;
                }
                Ok(f) => f,
            };

            if local_files.is_empty() {
                continue;
            }

            let files_from_metastore = match self.metastore.get_all_filenames().await {
                Err(e) => {
                    log::error!("could not get the list of files from metastore: {}", e);
                    continue;
                }
                Ok(f) => f,
            };

            // Only keep the files we want to remove in `local_files`.
            for f in files_from_metastore {
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
/* pub trait RemoteFsCleanup: DIService + Send + Sync + Debug {
    async fn waiting_cleanup_local_files_loop(&self);
} */
