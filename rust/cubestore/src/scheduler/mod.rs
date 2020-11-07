use crate::metastore::{MetaStoreEvent, MetaStore, TableId, RowKey};
use std::sync::Arc;
use crate::CubeError;
use tokio::sync::broadcast::{Receiver};
use crate::cluster::Cluster;
use crate::metastore::job::{JobType, Job};
use log::{error};
use crate::remotefs::RemoteFs;
use crate::store::{WALStore, ChunkStore};

pub struct SchedulerImpl {
    meta_store: Arc<dyn MetaStore>,
    cluster: Arc<dyn Cluster>,
    remote_fs: Arc<dyn RemoteFs>,
    event_receiver: Receiver<MetaStoreEvent>
}

impl SchedulerImpl {
    pub fn new(
        meta_store: Arc<dyn MetaStore>,
        cluster: Arc<dyn Cluster>,
        remote_fs: Arc<dyn RemoteFs>,
        event_receiver: Receiver<MetaStoreEvent>
    ) -> SchedulerImpl {
        SchedulerImpl {
            meta_store,
            cluster,
            remote_fs,
            event_receiver
        }
    }

    pub async fn run_scheduler(&mut self) -> Result<(), CubeError> {
        loop {
            let event = self.event_receiver.recv().await?;
            let res = self.process_event(event.clone()).await;
            if let Err(e) = res {
                error!("Error processing event {:?}: {}", event, e);
            }
        }
    }

    pub async fn run_scheduler_until(&mut self, last_event_fn: impl Fn(MetaStoreEvent) -> bool) -> Result<(), CubeError> {
        loop {
            let event = self.event_receiver.recv().await?;
            let res = self.process_event(event.clone()).await;
            if let Err(e) = res {
                error!("Error processing event {:?}: {}", event, e);
            }
            if last_event_fn(event) {
                return Ok(())
            }
        }
    }

    async fn process_event(&mut self, event: MetaStoreEvent) -> Result<(), CubeError> {
        if let
        MetaStoreEvent::Insert(TableId::WALs, row_id) |
        MetaStoreEvent::Update(TableId::WALs, row_id) = event {
            let wal = self.meta_store.get_wal(row_id).await?;
            if wal.get_row().uploaded() {
                self.schedule_wal_to_process(row_id).await?;
            }
        }
        if let
        MetaStoreEvent::Insert(TableId::Chunks, row_id) |
        MetaStoreEvent::Update(TableId::Chunks, row_id) = event {
            let chunk = self.meta_store.get_chunk(row_id).await?;
            if chunk.get_row().uploaded() {
                let partition = self.meta_store.get_partition(chunk.get_row().get_partition_id()).await?;
                if partition.get_row().is_active() {
                    // TODO config
                    let chunk_sizes = self.meta_store.get_partition_chunk_sizes(chunk.get_row().get_partition_id()).await?;
                    let chunks = self.meta_store.get_chunks_by_partition(chunk.get_row().get_partition_id()).await?;
                    if chunk_sizes > 500000 || chunks.len() > 16 {
                        self.schedule_partition_to_compact(chunk.get_row().get_partition_id()).await?;
                    }
                } else {
                    self.schedule_repartition(chunk.get_row().get_partition_id()).await?;
                }
            }
        }
        if let
        MetaStoreEvent::Insert(TableId::Tables, row_id)= event {
            self.schedule_table_import(row_id).await?;
        }
        if let
        MetaStoreEvent::Delete(TableId::WALs, row_id)= event {
            self.remote_fs.delete_file(WALStore::wal_remote_path(row_id).as_str()).await?
        }
        if let
        MetaStoreEvent::Delete(TableId::Chunks, row_id)= event {
            self.remote_fs.delete_file(ChunkStore::chunk_remote_path(row_id).as_str()).await?
        }
        if let
        MetaStoreEvent::Update(TableId::Partitions, row_id) = event {
            let partition = self.meta_store.get_partition(row_id).await?;
            if !partition.get_row().is_active() {
                self.schedule_repartition(row_id).await?;
            }
        }
        if let
        MetaStoreEvent::DeleteJob(job)= event {
            if let JobType::Repartition = job.get_row().job_type() {
                if let RowKey::Table(TableId::Partitions, partition_id) = job.get_row().row_reference() {
                    if self.meta_store.get_partition_chunk_sizes(*partition_id).await? > 0 {
                        self.schedule_repartition(*partition_id).await?;
                    }
                } else {
                    panic!("Unexpected row reference: {:?}", job.get_row().row_reference());
                }
            }
        }
        Ok(())
    }

    async fn schedule_repartition(&self, partition_id: u64) -> Result<(), CubeError> {
        let node = self.cluster.server_name().to_string(); // TODO find best node to run import
        let job = self.meta_store.add_job(Job::new(RowKey::Table(TableId::Partitions, partition_id), JobType::Repartition, node.to_string())).await?;
        if job.is_some() {
            // TODO queue failover
            self.cluster.notify_job_runner(node).await?;
        }
        Ok(())
    }

    async fn schedule_table_import(&self, table_id: u64) -> Result<(), CubeError> {
        let node = self.cluster.server_name().to_string(); // TODO find best node to run import
        let job = self.meta_store.add_job(Job::new(RowKey::Table(TableId::Tables, table_id), JobType::TableImport, node.to_string())).await?;
        if job.is_some() {
            // TODO queue failover
            self.cluster.notify_job_runner(node).await?;
        }
        Ok(())
    }

    async fn schedule_wal_to_process(&self, wal_id: u64) -> Result<(), CubeError> {
        let wal_node_name = self.cluster.server_name().to_string(); // TODO move to WAL
        let job = self.meta_store.add_job(Job::new(RowKey::Table(TableId::WALs, wal_id), JobType::WalPartitioning, wal_node_name.clone())).await?;
        if job.is_some() {
            // TODO queue failover
            self.cluster.notify_job_runner(wal_node_name).await?;
        }
        Ok(())
    }

    async fn schedule_partition_to_compact(&self, partition_id: u64) -> Result<(), CubeError> {
        let wal_node_name = self.cluster.server_name().to_string(); // TODO move to WAL
        let job = self.meta_store.add_job(Job::new(RowKey::Table(TableId::Partitions, partition_id), JobType::PartitionCompaction, wal_node_name.clone())).await?;
        if job.is_some() {
            // TODO queue failover
            self.cluster.notify_job_runner(wal_node_name).await?;
        }
        Ok(())
    }
}