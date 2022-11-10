use crate::config::{Config, ConfigObj};
use crate::metastore::metastore_fs::{MetaStoreFs, RocksMetaStoreFs};
use crate::metastore::table::TablePath;
use crate::metastore::MetaStoreEvent;
use crate::remotefs::LocalDirRemoteFs;
use crate::util::aborting_join_handle::AbortingJoinHandle;
use crate::util::time_span::warn_long;

use crate::CubeError;
use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use datafusion::cube_ext;

use log::{info, trace};
use rocksdb::backup::BackupEngineOptions;
use rocksdb::checkpoint::Checkpoint;
use rocksdb::{Snapshot, WriteBatch, WriteBatchIterator, DB};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Cursor;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::{env, mem, time};
use tokio::fs;
use tokio::fs::File;
use tokio::sync::broadcast::Sender;
use tokio::sync::{oneshot, Notify, RwLock};

macro_rules! enum_from_primitive_impl {
    ($name:ident, $( $variant:ident )*) => {
        impl From<u32> for $name {
            fn from(n: u32) -> Self {
                $( if n == $name::$variant as u32 {
                    $name::$variant
                } else )* {
                    panic!("Unknown {}: {}", stringify!($name), n);
                }
            }
        }
    };
}

macro_rules! enum_from_primitive {
    (
        $( #[$enum_attr:meta] )*
        pub enum $name:ident {
            $( $( $( #[$variant_attr:meta] )* $variant:ident ),+ = $discriminator:expr ),*
        }
    ) => {
        $( #[$enum_attr] )*
        pub enum $name {
            $( $( $( #[$variant_attr] )* $variant ),+ = $discriminator ),*
        }
        enum_from_primitive_impl! { $name, $( $( $variant )+ )* }
    };
}

enum_from_primitive! {
    #[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize, Hash)]
    pub enum TableId {
        Schemas = 0x0100,
        Tables = 0x0200,
        Indexes = 0x0300,
        Partitions = 0x0400,
        Chunks = 0x0500,
        WALs = 0x0600,
        Jobs = 0x0700,
        Sources = 0x0800,
        MultiIndexes = 0x0900,
        MultiPartitions = 0x0A00
    }
}

pub fn get_fixed_prefix() -> usize {
    13
}

pub type SecondaryKey = Vec<u8>;
pub type IndexId = u32;

#[derive(Clone)]
pub struct MemorySequence {
    seq_store: Arc<Mutex<HashMap<TableId, u64>>>,
}

impl MemorySequence {
    pub fn new(seq_store: Arc<Mutex<HashMap<TableId, u64>>>) -> Self {
        Self { seq_store }
    }

    pub fn next_seq(&self, table_id: TableId, snapshot_value: u64) -> Result<u64, CubeError> {
        let mut store = self.seq_store.lock()?;
        let mut current = *store.entry(table_id).or_insert(snapshot_value);
        current += 1;
        store.insert(table_id, current);
        Ok(current)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum RowKey {
    Table(TableId, u64),
    Sequence(TableId),
    SecondaryIndex(IndexId, SecondaryKey, u64),
    SecondaryIndexInfo { index_id: IndexId },
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct SecondaryIndexInfo {
    pub version: u32,
}

impl RowKey {
    pub fn from_bytes(bytes: &[u8]) -> RowKey {
        let mut reader = Cursor::new(bytes);
        match reader.read_u8().unwrap() {
            1 => RowKey::Table(TableId::from(reader.read_u32::<BigEndian>().unwrap()), {
                // skip zero for fixed key padding
                reader.read_u64::<BigEndian>().unwrap();
                reader.read_u64::<BigEndian>().unwrap()
            }),
            2 => RowKey::Sequence(TableId::from(reader.read_u32::<BigEndian>().unwrap())),
            3 => {
                let table_id = IndexId::from(reader.read_u32::<BigEndian>().unwrap());
                let mut secondary_key: SecondaryKey = SecondaryKey::new();
                let sc_length = bytes.len() - 13;
                for _i in 0..sc_length {
                    secondary_key.push(reader.read_u8().unwrap());
                }
                let row_id = reader.read_u64::<BigEndian>().unwrap();

                RowKey::SecondaryIndex(table_id, secondary_key, row_id)
            }
            4 => {
                let index_id = IndexId::from(reader.read_u32::<BigEndian>().unwrap());

                RowKey::SecondaryIndexInfo { index_id }
            }
            v => panic!("Unknown key prefix: {}", v),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut wtr = vec![];
        match self {
            RowKey::Table(table_id, row_id) => {
                wtr.write_u8(1).unwrap();
                wtr.write_u32::<BigEndian>(*table_id as u32).unwrap();
                wtr.write_u64::<BigEndian>(0).unwrap();
                wtr.write_u64::<BigEndian>(row_id.clone()).unwrap();
            }
            RowKey::Sequence(table_id) => {
                wtr.write_u8(2).unwrap();
                wtr.write_u32::<BigEndian>(*table_id as u32).unwrap();
            }
            RowKey::SecondaryIndex(index_id, secondary_key, row_id) => {
                wtr.write_u8(3).unwrap();
                wtr.write_u32::<BigEndian>(*index_id as IndexId).unwrap();
                for &n in secondary_key {
                    wtr.write_u8(n).unwrap();
                }
                wtr.write_u64::<BigEndian>(row_id.clone()).unwrap();
            }
            RowKey::SecondaryIndexInfo { index_id } => {
                wtr.write_u8(4).unwrap();
                wtr.write_u32::<BigEndian>(*index_id as IndexId).unwrap();
            }
        }
        wtr
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum WriteBatchEntry {
    Put { key: Box<[u8]>, value: Box<[u8]> },
    Delete { key: Box<[u8]> },
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WriteBatchContainer {
    entries: Vec<WriteBatchEntry>,
}

impl WriteBatchContainer {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn write_batch(&self) -> WriteBatch {
        let mut batch = WriteBatch::default();
        for entry in self.entries.iter() {
            match entry {
                WriteBatchEntry::Put { key, value } => batch.put(key, value),
                WriteBatchEntry::Delete { key } => batch.delete(key),
            }
        }
        batch
    }

    pub async fn write_to_file(&self, file_name: &str) -> Result<(), CubeError> {
        let mut ser = flexbuffers::FlexbufferSerializer::new();
        self.serialize(&mut ser)?;
        let mut file = File::create(file_name).await?;
        Ok(tokio::io::AsyncWriteExt::write_all(&mut file, ser.view()).await?)
    }

    pub async fn read_from_file(file_name: &str) -> Result<Self, CubeError> {
        let mut file = File::open(file_name).await?;

        let mut buffer = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut file, &mut buffer).await?;
        let r = flexbuffers::Reader::get_root(&buffer)?;
        Ok(Self::deserialize(r)?)
    }
}

impl WriteBatchIterator for WriteBatchContainer {
    fn put(&mut self, key: Box<[u8]>, value: Box<[u8]>) {
        self.entries.push(WriteBatchEntry::Put { key, value });
    }

    fn delete(&mut self, key: Box<[u8]>) {
        self.entries.push(WriteBatchEntry::Delete { key });
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct IdRow<T: Clone> {
    pub(crate) id: u64,
    pub(crate) row: T,
}

impl<T: Clone> IdRow<T> {
    pub fn new(id: u64, row: T) -> IdRow<T> {
        IdRow { id, row }
    }
    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub fn get_row(&self) -> &T {
        &self.row
    }

    pub fn into_row(self) -> T {
        self.row
    }
}

pub struct KeyVal {
    pub key: Vec<u8>,
    pub val: Vec<u8>,
}

pub struct BatchPipe<'a> {
    db: &'a DB,
    write_batch: WriteBatch,
    events: Vec<MetaStoreEvent>,
    pub invalidate_tables_cache: bool,
}

impl<'a> BatchPipe<'a> {
    pub fn new(db: &'a DB) -> BatchPipe<'a> {
        BatchPipe {
            db,
            write_batch: WriteBatch::default(),
            events: Vec::new(),
            invalidate_tables_cache: false,
        }
    }

    pub fn batch(&mut self) -> &mut WriteBatch {
        &mut self.write_batch
    }

    pub fn add_event(&mut self, event: MetaStoreEvent) {
        self.events.push(event);
    }

    pub fn batch_write_rows(self) -> Result<Vec<MetaStoreEvent>, CubeError> {
        let db = self.db;
        db.write(self.write_batch)?;
        Ok(self.events)
    }

    pub fn invalidate_tables_cache(&mut self) {
        self.invalidate_tables_cache = true;
    }
}

#[derive(Clone)]
pub struct DbTableRef<'a> {
    pub db: &'a DB,
    pub snapshot: &'a Snapshot<'a>,
    pub mem_seq: MemorySequence,
}

#[async_trait]
pub trait MetaStoreTable: Send + Sync {
    type T: Serialize + Clone + Debug + 'static;

    async fn all_rows(&self) -> Result<Vec<IdRow<Self::T>>, CubeError>;

    async fn row_by_id_or_not_found(&self, id: u64) -> Result<IdRow<Self::T>, CubeError>;

    async fn delete(&self, id: u64) -> Result<IdRow<Self::T>, CubeError>;
}

#[macro_export]
macro_rules! meta_store_table_impl {
    ($name: ident, $table: ty, $rocks_table: ident) => {
        pub struct $name {
            rocks_meta_store: Arc<RocksStore>,
        }

        impl $name {
            fn table<'a>(db: DbTableRef<'a>) -> $rocks_table<'a> {
                <$rocks_table>::new(db)
            }
        }

        #[async_trait]
        impl MetaStoreTable for $name {
            type T = $table;

            async fn all_rows(&self) -> Result<Vec<IdRow<Self::T>>, CubeError> {
                self.rocks_meta_store
                    .read_operation_out_of_queue(move |db_ref| Ok(Self::table(db_ref).all_rows()?))
                    .await
            }

            async fn row_by_id_or_not_found(&self, id: u64) -> Result<IdRow<Self::T>, CubeError> {
                self.rocks_meta_store
                    .read_operation(move |db_ref| Ok(Self::table(db_ref).get_row_or_not_found(id)?))
                    .await
            }

            async fn delete(&self, id: u64) -> Result<IdRow<Self::T>, CubeError> {
                self.rocks_meta_store
                    .write_operation(
                        move |db_ref, batch| Ok(Self::table(db_ref).delete(id, batch)?),
                    )
                    .await
            }
        }
    };
}

pub trait RocksStoreDetails: Send + Sync {
    fn open_db(&self, path: &Path) -> Result<DB, CubeError>;

    fn migrate(&self, table_ref: DbTableRef) -> Result<(), CubeError>;

    fn get_name(&self) -> &'static str;
}

#[derive(Clone)]
pub struct RocksStore {
    pub db: Arc<DB>,
    pub config: Arc<dyn ConfigObj>,
    seq_store: Arc<Mutex<HashMap<TableId, u64>>>,
    listeners: Arc<RwLock<Vec<Sender<MetaStoreEvent>>>>,
    metastore_fs: Arc<dyn MetaStoreFs>,
    last_checkpoint_time: Arc<RwLock<SystemTime>>,
    write_notify: Arc<Notify>,
    pub(crate) write_completed_notify: Arc<Notify>,
    last_upload_seq: Arc<RwLock<u64>>,
    last_check_seq: Arc<RwLock<u64>>,
    pub(crate) cached_tables: Arc<Mutex<Option<Arc<Vec<TablePath>>>>>,
    rw_loop_tx: std::sync::mpsc::SyncSender<
        Box<dyn FnOnce() -> Result<(), CubeError> + Send + Sync + 'static>,
    >,
    _rw_loop_join_handle: Arc<AbortingJoinHandle<()>>,
    details: Arc<dyn RocksStoreDetails>,
}

pub fn check_if_exists(name: &String, existing_keys_len: usize) -> Result<(), CubeError> {
    if existing_keys_len > 1 {
        let e = CubeError::user(format!(
            "Schema with name '{}' has more than one id. Something went wrong.",
            name
        ));
        return Err(e);
    } else if existing_keys_len == 0 {
        let e = CubeError::user(format!("Schema with name '{}' does not exist.", name));
        return Err(e);
    }
    Ok(())
}

impl RocksStore {
    pub fn with_listener(
        path: &Path,
        listeners: Vec<Sender<MetaStoreEvent>>,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
        details: Arc<dyn RocksStoreDetails>,
    ) -> Arc<RocksStore> {
        let meta_store =
            RocksStore::with_listener_impl(path, listeners, metastore_fs, config, details);
        Arc::new(meta_store)
    }

    pub fn with_listener_impl(
        path: &Path,
        listeners: Vec<Sender<MetaStoreEvent>>,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
        details: Arc<dyn RocksStoreDetails>,
    ) -> RocksStore {
        let db = details.open_db(path).unwrap();
        let db_arc = Arc::new(db);

        let (rw_loop_tx, rw_loop_rx) = std::sync::mpsc::sync_channel::<
            Box<dyn FnOnce() -> Result<(), CubeError> + Send + Sync + 'static>,
        >(32_768);

        let join_handle = cube_ext::spawn_blocking(move || loop {
            match rw_loop_rx.recv() {
                Ok(fun) => {
                    if let Err(e) = fun() {
                        log::error!("Error during read write loop execution: {}", e);
                    }
                }
                Err(_) => {
                    return;
                }
            }
        });

        let meta_store = RocksStore {
            db: db_arc.clone(),
            seq_store: Arc::new(Mutex::new(HashMap::new())),
            listeners: Arc::new(RwLock::new(listeners)),
            metastore_fs,
            last_checkpoint_time: Arc::new(RwLock::new(SystemTime::now())),
            write_notify: Arc::new(Notify::new()),
            write_completed_notify: Arc::new(Notify::new()),
            last_upload_seq: Arc::new(RwLock::new(db_arc.latest_sequence_number())),
            last_check_seq: Arc::new(RwLock::new(db_arc.latest_sequence_number())),
            config,
            cached_tables: Arc::new(Mutex::new(None)),
            rw_loop_tx,
            _rw_loop_join_handle: Arc::new(AbortingJoinHandle::new(join_handle)),
            details,
        };

        meta_store
    }

    pub fn new(
        path: &Path,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
        details: Arc<dyn RocksStoreDetails>,
    ) -> Arc<RocksStore> {
        Self::with_listener(path, vec![], metastore_fs, config, details)
    }

    pub async fn load_from_dump(
        path: &Path,
        dump_path: &Path,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
        details: Arc<dyn RocksStoreDetails>,
    ) -> Result<Arc<RocksStore>, CubeError> {
        if !fs::metadata(path).await.is_ok() {
            let mut backup =
                rocksdb::backup::BackupEngine::open(&BackupEngineOptions::default(), dump_path)?;
            backup.restore_from_latest_backup(
                &path,
                &path,
                &rocksdb::backup::RestoreOptions::default(),
            )?;
        } else {
            info!(
                "Using existing {} in {}",
                details.get_name(),
                path.as_os_str().to_string_lossy()
            );
        }

        let meta_store = Self::new(path, metastore_fs, config, details);
        Self::check_all_indexes(&meta_store).await?;

        Ok(meta_store)
    }

    pub async fn check_all_indexes(meta_store: &Arc<Self>) -> Result<(), CubeError> {
        let meta_store_to_move = meta_store.clone();

        cube_ext::spawn_blocking(move || {
            let table_ref = DbTableRef {
                db: &meta_store_to_move.db,
                snapshot: &meta_store_to_move.db.snapshot(),
                mem_seq: MemorySequence::new(meta_store_to_move.seq_store.clone()),
            };

            if let Err(e) = meta_store_to_move.details.migrate(table_ref) {
                log::error!("Error during checking indexes: {}", e);
            }
        })
        .await?;

        Ok(())
    }

    pub async fn add_listener(&self, listener: Sender<MetaStoreEvent>) {
        self.listeners.write().await.push(listener);
    }

    pub async fn write_operation<F, R>(&self, f: F) -> Result<R, CubeError>
    where
        F: for<'a> FnOnce(DbTableRef<'a>, &'a mut BatchPipe) -> Result<R, CubeError>
            + Send
            + Sync
            + 'static,
        R: Send + Sync + 'static,
    {
        let db = self.db.clone();
        let mem_seq = MemorySequence::new(self.seq_store.clone());
        let db_to_send = db.clone();
        let cached_tables = self.cached_tables.clone();
        let store_name = self.details.get_name();

        let rw_loop_sender = self.rw_loop_tx.clone();
        let (tx, rx) = oneshot::channel::<Result<(R, Vec<MetaStoreEvent>), CubeError>>();

        cube_ext::spawn_blocking(move || {
            let res = rw_loop_sender.send(Box::new(move || {
                let db_span = warn_long("metastore write operation", Duration::from_millis(100));

                let mut batch = BatchPipe::new(db_to_send.as_ref());
                let snapshot = db_to_send.snapshot();
                let res = f(
                    DbTableRef {
                        db: db_to_send.as_ref(),
                        snapshot: &snapshot,
                        mem_seq,
                    },
                    &mut batch,
                );
                match res {
                    Ok(res) => {
                        if batch.invalidate_tables_cache {
                            *cached_tables.lock().unwrap() = None;
                        }
                        let write_result = batch.batch_write_rows()?;
                        tx.send(Ok((res, write_result))).map_err(|_| {
                            CubeError::internal(format!(
                                "[{}] Write operation result receiver has been dropped",
                                store_name
                            ))
                        })?;
                    }
                    Err(e) => {
                        tx.send(Err(e)).map_err(|_| {
                            CubeError::internal(format!(
                                "[{}] Write operation result receiver has been dropped",
                                store_name
                            ))
                        })?;
                    }
                }

                mem::drop(db_span);

                Ok(())
            }));
            if let Err(e) = res {
                log::error!("[{}] Error during read write loop send: {}", store_name, e);
            }
        })
        .await?;
        let (spawn_res, events) = rx.await??;

        self.write_notify.notify_waiters();

        for listener in self.listeners.read().await.clone().iter_mut() {
            for event in events.iter() {
                listener.send(event.clone())?;
            }
        }
        Ok(spawn_res)
    }

    pub async fn run_upload(&self) -> Result<(), CubeError> {
        let time = SystemTime::now();
        trace!("Persisting meta store snapshot");
        let last_check_seq = self.last_check_seq().await;
        let last_db_seq = self.db.latest_sequence_number();
        if last_check_seq == last_db_seq {
            trace!("Persisting meta store snapshot: nothing to update");
            return Ok(());
        }
        let last_upload_seq = self.last_upload_seq().await;
        let (serializer, min, max) = {
            let updates = self.db.get_updates_since(last_upload_seq)?;
            let mut serializer = WriteBatchContainer::new();

            let mut seq_numbers = Vec::new();

            updates.into_iter().for_each(|(n, write_batch)| {
                seq_numbers.push(n);
                write_batch.iterate(&mut serializer);
            });
            (
                serializer,
                seq_numbers.iter().min().map(|v| *v),
                seq_numbers.iter().max().map(|v| *v),
            )
        };

        if max.is_some() {
            let checkpoint_time = self.last_checkpoint_time.read().await;
            let log_name = format!(
                "{}-logs/{}.flex",
                self.get_store_path(&checkpoint_time),
                min.unwrap()
            );
            self.metastore_fs.upload_log(&log_name, &serializer).await?;
            let mut seq = self.last_upload_seq.write().await;
            *seq = max.unwrap();
            self.write_completed_notify.notify_waiters();
        }

        let last_checkpoint_time: SystemTime = self.last_checkpoint_time.read().await.clone();
        if last_checkpoint_time
            + time::Duration::from_secs(self.config.meta_store_snapshot_interval())
            < SystemTime::now()
        {
            info!("Uploading meta store check point");
            self.upload_check_point().await?;
        }

        let mut check_seq = self.last_check_seq.write().await;
        *check_seq = last_db_seq;

        info!(
            "Persisting meta store snapshot: done ({:?})",
            time.elapsed()?
        );

        Ok(())
    }

    pub async fn upload_check_point(&self) -> Result<(), CubeError> {
        let mut check_point_time = self.last_checkpoint_time.write().await;

        let (remote_path, checkpoint_path) = {
            let _db = self.db.clone();
            *check_point_time = SystemTime::now();
            self.prepare_checkpoint(&check_point_time).await?
        };

        self.metastore_fs
            .upload_checkpoint(remote_path, checkpoint_path)
            .await?;
        self.write_completed_notify.notify_waiters();
        Ok(())
    }

    async fn last_upload_seq(&self) -> u64 {
        *self.last_upload_seq.read().await
    }

    async fn last_check_seq(&self) -> u64 {
        *self.last_check_seq.read().await
    }

    fn get_store_path(&self, checkpoint_time: &SystemTime) -> String {
        format!(
            "{}-{}",
            self.details.get_name(),
            checkpoint_time
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        )
    }

    async fn prepare_checkpoint(
        &self,
        checkpoint_time: &SystemTime,
    ) -> Result<(String, PathBuf), CubeError> {
        let remote_path = self.get_store_path(checkpoint_time);
        let checkpoint_path = self.db.path().join("..").join(remote_path.clone());

        let path_to_move = checkpoint_path.clone();
        let db_to_move = self.db.clone();

        cube_ext::spawn_blocking(move || -> Result<(), CubeError> {
            let checkpoint = Checkpoint::new(db_to_move.as_ref())?;
            checkpoint.create_checkpoint(path_to_move.as_path())?;
            Ok(())
        })
        .await??;

        Ok((remote_path, checkpoint_path))
    }

    pub async fn read_operation<F, R>(&self, f: F) -> Result<R, CubeError>
    where
        F: for<'a> FnOnce(DbTableRef<'a>) -> Result<R, CubeError> + Send + Sync + 'static,
        R: Send + Sync + 'static,
    {
        let mem_seq = MemorySequence::new(self.seq_store.clone());
        let db_to_send = self.db.clone();
        let store_name = self.details.get_name();

        let rw_loop_sender = self.rw_loop_tx.clone();
        let (tx, rx) = oneshot::channel::<Result<R, CubeError>>();

        cube_ext::spawn_blocking(move || {
            let res = rw_loop_sender.send(Box::new(move || {
                let db_span = warn_long("metastore read operation", Duration::from_millis(100));

                let snapshot = db_to_send.snapshot();
                let res = f(DbTableRef {
                    db: db_to_send.as_ref(),
                    snapshot: &snapshot,
                    mem_seq,
                });

                tx.send(res).map_err(|_| {
                    CubeError::internal(format!(
                        "[{}] Read operation result receiver has been dropped",
                        store_name
                    ))
                })?;

                mem::drop(db_span);

                Ok(())
            }));
            if let Err(e) = res {
                log::error!("Error during read write loop send: {}", e);
            }
        })
        .await?;

        rx.await?
    }

    pub async fn read_operation_out_of_queue<F, R>(&self, f: F) -> Result<R, CubeError>
    where
        F: for<'a> FnOnce(DbTableRef<'a>) -> Result<R, CubeError> + Send + Sync + 'static,
        R: Send + Sync + 'static,
    {
        let mem_seq = MemorySequence::new(self.seq_store.clone());
        let db_to_send = self.db.clone();

        cube_ext::spawn_blocking(move || {
            let db_span = warn_long(
                "metastore read operation out of queue",
                Duration::from_millis(100),
            );

            let snapshot = db_to_send.snapshot();
            let res = f(DbTableRef {
                db: db_to_send.as_ref(),
                snapshot: &snapshot,
                mem_seq,
            });

            mem::drop(db_span);

            res
        })
        .await?
    }

    pub fn prepare_test_metastore(
        test_name: &str,
        details: Arc<dyn RocksStoreDetails>,
    ) -> (Arc<LocalDirRemoteFs>, Arc<RocksStore>) {
        let config = Config::test(test_name);
        let store_path = env::current_dir()
            .unwrap()
            .join(format!("test-{}-local", test_name));
        let remote_store_path = env::current_dir()
            .unwrap()
            .join(format!("test-{}-remote", test_name));
        let _ = std::fs::remove_dir_all(store_path.clone());
        let _ = std::fs::remove_dir_all(remote_store_path.clone());
        let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());
        let meta_store = RocksStore::new(
            store_path.clone().join(details.get_name()).as_path(),
            RocksMetaStoreFs::new(remote_fs.clone()),
            config.config_obj(),
            details,
        );
        (remote_fs, meta_store)
    }

    pub fn cleanup_test_metastore(test_name: &str) {
        let store_path = env::current_dir()
            .unwrap()
            .join(format!("test-{}-local", test_name));
        let remote_store_path = env::current_dir()
            .unwrap()
            .join(format!("test-{}-remote", test_name));
        let _ = std::fs::remove_dir_all(store_path.clone());
        let _ = std::fs::remove_dir_all(remote_store_path.clone());
    }

    pub async fn has_pending_changes(&self) -> Result<bool, CubeError> {
        let db = self.db.clone();
        Ok(db
            .get_updates_since(self.last_upload_seq().await)?
            .next()
            .is_some())
    }
}
