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
        let cleanup_files_delay = self.config.remote_files_cleanup_delay_secs() as i64;
        let remote_fs = self.remote_fs.clone();
        let mut files_to_remove: HashSet<String> = HashSet::new();
        let cleanup_enabled = self.config.enable_remove_orphaned_remote_files();
        let batch_size = self.config.remote_files_cleanup_batch_size();
        loop {
            // Do the cleanup every now and then.
            tokio::select! {
                () = tokio::time::sleep(cleanup_interval) => {},
                _ = token.cancelled() => {
                    return;
                }
            }
            //We delete files on the next iteration after building the file list in order to give time for requests that may use these files to complete
            if cleanup_enabled && !files_to_remove.is_empty() {
                log::debug!("Cleaning up {} files in remote fs", files_to_remove.len());
                log::trace!("The files being removed are {:?}", files_to_remove);
                //Double check that files don't exists in metastore
                let files_from_metastore = match self.metastore.get_all_filenames().await {
                    Err(e) => {
                        log::error!("could not get the list of files from metastore: {}", e);
                        continue;
                    }
                    Ok(f) => f,
                };

                // Only keep the files we want to remove in `local_files`.
                for f in files_from_metastore {
                    files_to_remove.remove(&f);
                }
                for f in files_to_remove.iter() {
                    if let Err(e) = self.remote_fs.delete_file(f.clone()).await {
                        log::error!("Error while deleting {} in remote fs: {}", f, e);
                    }
                }
            }

            files_to_remove.clear();

            let res_remote_files = remote_fs.list_with_metadata("".to_string()).await;
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

            let mut files_to_remove_size = 0;
            let mut files_to_remove_count = 0;

            for f in remote_files {
                let file_name = f.remote_path();
                if !file_name.ends_with(".parquet") {
                    continue;
                }
                if files_from_metastore.get(file_name).is_some() {
                    continue;
                }
                if Utc::now()
                    .signed_duration_since(f.updated().clone())
                    .num_seconds()
                    < cleanup_files_delay
                {
                    continue;
                }
                files_to_remove_size += f.file_size;
                files_to_remove_count += 1;
                if (files_to_remove.len() as u64) < batch_size {
                    files_to_remove.insert(file_name.to_string());
                }
            }
            app_metrics::REMOTE_FS_FILES_TO_REMOVE
                .report_with_tags(files_to_remove_count as i64, Some(&stat_tags));

            app_metrics::REMOTE_FS_FILES_SIZE_TO_REMOVE
                .report_with_tags(files_to_remove_size as i64, Some(&stat_tags));
        }
    }

    async fn cleanup_local_files_loop(&self) {
        let token = self.stopped_token.child_token();
        let remote_fs = self.remote_fs.clone();
        let local_dir = remote_fs.local_path().await.unwrap();
        let cleanup_interval = Duration::from_secs(self.config.local_files_cleanup_interval_secs());
        let cleanup_local_files_delay =
            Duration::from_secs(self.config.local_files_cleanup_delay_secs());

        let mut files_to_remove: HashSet<String> = HashSet::new();
        loop {
            // Do the cleanup every now and then.
            tokio::select! {
                () = tokio::time::sleep(cleanup_interval) => {},
                _ = token.cancelled() => {
                    return;
                }
            }

            //We delete files on the next iteration after building the file list in order to give time for requests that may use these files to complete
            if !files_to_remove.is_empty() {
                log::debug!(
                    "Cleaning up {} files that were removed remotely",
                    files_to_remove.len()
                );
                log::trace!("The files being removed are {:?}", files_to_remove);
                //Double check that files don't exists in metastore
                let files_from_metastore = match self.metastore.get_all_filenames().await {
                    Err(e) => {
                        log::error!("could not get the list of files from metastore: {}", e);
                        continue;
                    }
                    Ok(f) => f,
                };

                // Only keep the files we want to remove in `local_files`.
                for f in files_from_metastore {
                    files_to_remove.remove(&f);
                }

                let local_dir_copy = local_dir.clone();
                let mut files_to_remove_to_move = HashSet::new();
                std::mem::swap(&mut files_to_remove, &mut files_to_remove_to_move);
                cube_ext::spawn_blocking(move || {
                    for f in files_to_remove_to_move {
                        let _ = std::fs::remove_file(Path::new(&local_dir_copy).join(f));
                    }
                })
                .await
                .unwrap();
            }
            files_to_remove.clear();

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
                files_to_remove = local_files;
            }
        }
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use crate::config::Config;
    use futures_timer::Delay;

    fn is_root_partition(name: &str) -> bool {
        name.starts_with("1-") && !name.ends_with(".chunk.parquet")
    }

    fn remove_root_paritition(names: Vec<String>) -> Vec<String> {
        names
            .into_iter()
            .filter(|name| !is_root_partition(name))
            .collect::<Vec<_>>()
    }

    #[tokio::test]
    async fn queue_cleanup_local_files() {
        Config::test("cleanup_local_files")
            .update_config(|mut c| {
                c.local_files_cleanup_delay_secs = 2;
                c.local_files_cleanup_interval_secs = 1;
                c
            })
            .start_test(async move |services| {
                let service = services.sql_service;
                let meta_store = services.meta_store;
                let remote_fs = services.injector.get_service_typed::<dyn RemoteFs>().await;
                let _ = service.exec_query("CREATE SCHEMA test").await.unwrap();
                let _ = service
                    .exec_query("CREATE TABLE test.tst (a int, b int)")
                    .await
                    .unwrap();
                let _ = service
                    .exec_query("INSERT INTO test.tst (a, b) VALUES (10, 10), (20 , 20)")
                    .await
                    .unwrap();
                let _ = service
                    .exec_query("INSERT INTO test.tst (a, b) VALUES (20, 20), (40 , 40)")
                    .await
                    .unwrap();
                let files = remove_root_paritition(meta_store.get_all_filenames().await.unwrap());
                assert_eq!(files.len(), 2);
                for f in files.iter() {
                    let path = remote_fs.local_file(f.clone()).await.unwrap();
                    assert!(Path::new(&path).exists());
                }
                let path = remote_fs.local_file("metastore".to_string()).await.unwrap();
                assert!(Path::new(&path).exists());

                meta_store
                    .delete_chunks_without_checks(vec![1])
                    .await
                    .unwrap();

                assert_eq!(
                    remove_root_paritition(meta_store.get_all_filenames().await.unwrap()).len(),
                    1
                );
                for f in files.iter() {
                    let path = remote_fs.local_file(f.clone()).await.unwrap();
                    assert!(Path::new(&path).exists());
                }
                Delay::new(Duration::from_millis(4000)).await; // TODO logger init conflict

                let path = remote_fs.local_file(files[0].clone()).await.unwrap();
                assert!(!Path::new(&path).exists());

                let path = remote_fs.local_file(files[1].clone()).await.unwrap();
                assert!(Path::new(&path).exists());

                let path = remote_fs.local_file("metastore".to_string()).await.unwrap();
                assert!(Path::new(&path).exists());

                let _ = service.exec_query("SELECT * FROM test.tst").await.unwrap();
            })
            .await;
    }
}
