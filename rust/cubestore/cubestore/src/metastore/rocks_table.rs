use crate::metastore::rocks_store::TableId;
use crate::metastore::{
    get_fixed_prefix, BatchPipe, DbTableRef, IdRow, IndexId, KeyVal, MemorySequence,
    MetaStoreEvent, RocksSecondaryIndexValue, RocksSecondaryIndexValueTTLExtended,
    RocksSecondaryIndexValueVersion, RocksTableStats, RowKey, SecondaryIndexInfo, SecondaryKeyHash,
    TableInfo,
};
use crate::CubeError;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use chrono::{DateTime, Utc};
use cuberockstore::rocksdb::{
    DBIterator, Direction, IteratorMode, ReadOptions, Snapshot, WriteBatch, DB,
};
use itertools::Itertools;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::sync::Arc;
use std::time::SystemTime;

#[macro_export]
macro_rules! rocks_table_impl {
    ($table: ty, $rocks_table: ident, $table_id: expr, $indexes: block) => {
        pub(crate) struct $rocks_table<'a> {
            db: crate::metastore::DbTableRef<'a>,
        }

        impl<'a> $rocks_table<'a> {
            pub fn new(db: crate::metastore::DbTableRef<'a>) -> Self {
                Self { db }
            }
        }

        impl<'a> crate::metastore::BaseRocksTable for $rocks_table<'a> {
            fn migrate_table(
                &self,
                _batch: &mut cuberockstore::rocksdb::WriteBatch,
                table_info: crate::metastore::TableInfo,
            ) -> Result<(), crate::CubeError> {
                Err(crate::CubeError::internal(format!(
                    "Unable to migrate table from {}. There is no support for auto migrations. Please implement migration.",
                    table_info.version
                )))
            }
        }

        crate::rocks_table_new!($table, $rocks_table, $table_id, $indexes);
    };
}

#[macro_export]
macro_rules! rocks_table_new {
    ($table: ty, $rocks_table: ident, $table_id: expr, $indexes: block) => {
        impl<'a> crate::metastore::RocksTable for $rocks_table<'a> {
            type T = $table;

            fn db(&self) -> &cuberockstore::rocksdb::DB {
                self.db.db
            }

            fn snapshot(&self) -> &cuberockstore::rocksdb::Snapshot<'_> {
                self.db.snapshot
            }

            fn mem_seq(&self) -> &crate::metastore::MemorySequence {
                &self.db.mem_seq
            }

            fn table_ref(&self) -> &crate::metastore::DbTableRef<'_> {
                &self.db
            }

            fn table_id() -> TableId {
                $table_id
            }

            fn index_id(index_num: IndexId) -> IndexId {
                if index_num > 99 {
                    panic!("Too big index id: {}", index_num);
                }
                $table_id as IndexId + index_num
            }

            fn deserialize_row<'de, D>(
                &self,
                deserializer: D,
            ) -> Result<$table, <D as Deserializer<'de>>::Error>
            where
                D: Deserializer<'de>,
            {
                <$table>::deserialize(deserializer)
            }

            fn indexes() -> Vec<Box<dyn crate::metastore::BaseRocksSecondaryIndex<$table>>> {
                $indexes
            }

            fn update_event(
                &self,
                old_row: crate::metastore::IdRow<Self::T>,
                new_row: crate::metastore::IdRow<Self::T>,
            ) -> crate::metastore::MetaStoreEvent {
                paste::expr! { crate::metastore::MetaStoreEvent::[<Update $table>](old_row, new_row) }
            }

            fn delete_event(&self, row: crate::metastore::IdRow<Self::T>) -> crate::metastore::MetaStoreEvent {
                paste::expr! { crate::metastore::MetaStoreEvent::[<Delete $table>](row) }
            }
        }

        impl<'a> core::fmt::Debug for $rocks_table<'a> {
            fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
                f.write_fmt(format_args!("{}", stringify!($rocks_table)))?;
                Ok(())
            }
        }
    };
}

pub trait BaseRocksSecondaryIndex<T>: Debug {
    fn index_value(&self, row: &T) -> Vec<u8>;

    fn index_key_by(&self, row: &T) -> Vec<u8>;

    fn get_id(&self) -> u32;

    fn key_hash(&self, row: &T) -> u64 {
        let key_bytes = self.index_key_by(row);
        self.hash_bytes(&key_bytes)
    }

    fn hash_bytes(&self, key_bytes: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        key_bytes.hash(&mut hasher);
        hasher.finish()
    }

    fn is_unique(&self) -> bool;

    fn is_ttl(&self) -> bool;

    fn store_ttl_extended_info(&self) -> bool;

    fn get_expire(&self, _row: &T) -> Option<DateTime<Utc>>;

    fn version(&self) -> u32;

    fn value_version(&self) -> RocksSecondaryIndexValueVersion;
}

pub trait RocksSecondaryIndex<T, K: Hash>: BaseRocksSecondaryIndex<T> {
    fn typed_key_by(&self, row: &T) -> K;

    fn raw_value_size(&self, _row: &T) -> u32 {
        0
    }

    fn key_to_bytes(&self, key: &K) -> Vec<u8>;

    fn typed_key_hash(&self, row_key: &K) -> u64 {
        let key_bytes = self.key_to_bytes(row_key);
        self.hash_bytes(&key_bytes)
    }

    fn index_value(&self, row: &T) -> Vec<u8> {
        let hash = self.key_to_bytes(&self.typed_key_by(row));

        if RocksSecondaryIndex::is_ttl(self) {
            let expire = RocksSecondaryIndex::get_expire(self, row);

            if RocksSecondaryIndex::store_ttl_extended_info(self) {
                RocksSecondaryIndexValue::HashAndTTLExtended(
                    &hash,
                    expire,
                    RocksSecondaryIndexValueTTLExtended {
                        lfu: 0,
                        // Specify the current time as protection from LRU eviction
                        lru: Some(Utc::now()),
                        raw_size: self.raw_value_size(row),
                    },
                )
                .to_bytes(RocksSecondaryIndex::value_version(self))
                .unwrap()
            } else {
                RocksSecondaryIndexValue::HashAndTTL(&hash, expire)
                    .to_bytes(RocksSecondaryIndex::value_version(self))
                    .unwrap()
            }
        } else {
            RocksSecondaryIndexValue::Hash(&hash)
                .to_bytes(RocksSecondaryIndex::value_version(self))
                .unwrap()
        }
    }

    fn index_key_by(&self, row: &T) -> Vec<u8> {
        self.key_to_bytes(&self.typed_key_by(row))
    }

    fn get_id(&self) -> u32;

    fn is_unique(&self) -> bool;

    fn version(&self) -> u32;

    fn value_version(&self) -> RocksSecondaryIndexValueVersion {
        if RocksSecondaryIndex::is_ttl(self) {
            RocksSecondaryIndexValueVersion::WithTTLSupport
        } else {
            RocksSecondaryIndexValueVersion::OnlyHash
        }
    }

    fn is_ttl(&self) -> bool {
        false
    }

    fn store_ttl_extended_info(&self) -> bool {
        false
    }

    fn get_expire(&self, _row: &T) -> Option<DateTime<Utc>> {
        None
    }
}

impl<T, I> BaseRocksSecondaryIndex<T> for I
where
    I: RocksSecondaryIndex<T, String>,
{
    fn index_value(&self, row: &T) -> Vec<u8> {
        RocksSecondaryIndex::index_value(self, row)
    }

    fn index_key_by(&self, row: &T) -> Vec<u8> {
        RocksSecondaryIndex::index_key_by(self, row)
    }

    fn get_id(&self) -> u32 {
        RocksSecondaryIndex::get_id(self)
    }

    fn is_unique(&self) -> bool {
        RocksSecondaryIndex::is_unique(self)
    }

    fn is_ttl(&self) -> bool {
        RocksSecondaryIndex::is_ttl(self)
    }

    fn store_ttl_extended_info(&self) -> bool {
        RocksSecondaryIndex::store_ttl_extended_info(self)
    }

    fn get_expire(&self, row: &T) -> Option<DateTime<Utc>> {
        RocksSecondaryIndex::get_expire(self, row)
    }

    fn version(&self) -> u32 {
        RocksSecondaryIndex::version(self)
    }

    fn value_version(&self) -> RocksSecondaryIndexValueVersion {
        RocksSecondaryIndex::value_version(self)
    }
}

pub struct TableScanIter<'a, RT: RocksTable + ?Sized> {
    table_id: TableId,
    table: &'a RT,
    iter: DBIterator<'a>,
}

impl<'a, RT: RocksTable<T = T> + ?Sized, T> Iterator for TableScanIter<'a, RT>
where
    T: Serialize + Clone + Debug + Send,
{
    type Item = Result<IdRow<T>, CubeError>;

    fn next(&mut self) -> Option<Self::Item> {
        let option = self.iter.next();
        if let Some(res) = option {
            let (key, value) = res.unwrap();
            if let RowKey::Table(table_id, row_id) = RowKey::from_bytes(&key) {
                if table_id != self.table_id {
                    return None;
                }

                Some(self.table.deserialize_id_row(row_id, &value))
            } else {
                None
            }
        } else {
            None
        }
    }
}

pub struct IndexScanIter<'a, RT: RocksTable + ?Sized> {
    table: &'a RT,
    index_id: u32,
    secondary_key_val: Vec<u8>,
    secondary_key_hash: SecondaryKeyHash,
    iter: DBIterator<'a>,
}

impl<'a, RT: RocksTable<T = T> + ?Sized, T> Iterator for IndexScanIter<'a, RT>
where
    T: Serialize + Clone + Debug + Send,
{
    type Item = Result<IdRow<T>, CubeError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let option = self.iter.next();
            if let Some(res) = option {
                let (key, value) = res.unwrap();
                if let RowKey::SecondaryIndex(_, secondary_index_hash, row_id) =
                    RowKey::from_bytes(&key)
                {
                    if &secondary_index_hash != self.secondary_key_hash.as_slice() {
                        return None;
                    }

                    if self.secondary_key_val.as_slice() != value.as_ref() {
                        continue;
                    }

                    let result = match self.table.get_row(row_id) {
                        Ok(Some(row)) => Ok(row),
                        Ok(None) => {
                            let index = self.table.get_index_by_id(self.index_id);
                            match self.table.rebuild_index(&index) {
                                Ok(_) => {
                                    Err(CubeError::internal(format!(
                                        "Row exists in secondary index however missing in {:?} table: {}. Repairing index.",
                                        self.table, row_id
                                    )))
                                }
                                Err(e) => {
                                    Err(CubeError::internal(format!(
                                        "Error while rebuilding secondary index for {:?} table: {:?}",
                                        self.table, e
                                                )))
                                }

                            }
                        }
                        Err(e) => Err(e),
                    };

                    return Some(result);
                };
            } else {
                return None;
            }
        }
    }
}

#[derive(Debug)]
pub struct SecondaryIndexValueScanIterItem {
    pub row_id: u64,
    pub key_hash: SecondaryKeyHash,
    pub ttl: Option<DateTime<Utc>>,
    pub extended: Option<RocksSecondaryIndexValueTTLExtended>,
}

pub struct SecondaryIndexValueScanIter<'a> {
    index_id: u32,
    index_version: RocksSecondaryIndexValueVersion,
    iter: DBIterator<'a>,
}

impl<'a> Iterator for SecondaryIndexValueScanIter<'a> {
    type Item = Result<SecondaryIndexValueScanIterItem, CubeError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let option = self.iter.next();
            if let Some(res) = option {
                let (key, value) = res.unwrap();

                if let RowKey::SecondaryIndex(index_id, key_hash, row_id) = RowKey::from_bytes(&key)
                {
                    if index_id != self.index_id {
                        return None;
                    }

                    let secondary_index_value =
                        match RocksSecondaryIndexValue::from_bytes(&value, self.index_version) {
                            Ok(r) => r,
                            Err(err) => return Some(Err(err)),
                        };

                    let (ttl, extended) = match secondary_index_value {
                        RocksSecondaryIndexValue::Hash(_) => (None, None),
                        RocksSecondaryIndexValue::HashAndTTL(_, ttl) => (ttl, None),
                        RocksSecondaryIndexValue::HashAndTTLExtended(_, ttl, extended) => {
                            (ttl, Some(extended))
                        }
                    };

                    return Some(Ok(SecondaryIndexValueScanIterItem {
                        key_hash,
                        row_id,
                        ttl,
                        extended,
                    }));
                };
            } else {
                return None;
            }
        }
    }
}

pub trait RocksEntity {
    // Version of data/table fields which is used for data migration
    fn version() -> u32 {
        1
    }

    // Version of the serialization format
    fn value_version() -> u32 {
        1
    }
}

pub trait BaseRocksTable {
    fn migrate_table(&self, batch: &mut WriteBatch, table_info: TableInfo)
        -> Result<(), CubeError>;

    fn enable_update_event(&self) -> bool {
        true
    }

    fn enable_delete_event(&self) -> bool {
        true
    }
}

pub trait RocksTable: BaseRocksTable + Debug + Send + Sync {
    type T: Serialize + Clone + Debug + Send + RocksEntity;
    fn delete_event(&self, row: IdRow<Self::T>) -> MetaStoreEvent;
    fn update_event(&self, old_row: IdRow<Self::T>, new_row: IdRow<Self::T>) -> MetaStoreEvent;
    fn db(&self) -> &DB;
    fn table_ref(&self) -> &DbTableRef<'_>;
    fn snapshot(&self) -> &Snapshot<'_>;
    fn mem_seq(&self) -> &MemorySequence;
    fn index_id(index_num: IndexId) -> IndexId;
    fn table_id() -> TableId;
    fn deserialize_row<'de, D>(&self, deserializer: D) -> Result<Self::T, D::Error>
    where
        D: Deserializer<'de>;
    fn indexes() -> Vec<Box<dyn BaseRocksSecondaryIndex<Self::T>>>;

    fn migrate_table_by_truncate(&self, mut batch: &mut WriteBatch) -> Result<(), CubeError> {
        log::info!("Migrating by truncating rows from {:?} table", self);
        let total_rows = self.delete_all_rows_from_table(Self::table_id(), &mut batch)?;

        for index in Self::indexes() {
            log::trace!("Migrating by truncating rows from {:?} index", index);
            let total = self.delete_all_rows_from_index(index.get_id(), &mut batch)?;
            log::trace!(
                "Migrating by truncating rows from {:?} index: done ({} rows)",
                index,
                total
            );
        }

        log::info!(
            "Migrating by truncating rows from {:?} table: done ({} rows)",
            self,
            total_rows
        );

        Ok(())
    }

    /// @internal Do not use this method directly, please use insert or insert_with_pk
    fn do_insert(
        &self,
        row_id: Option<u64>,
        row: Self::T,
        batch_pipe: &mut BatchPipe,
    ) -> Result<IdRow<Self::T>, CubeError> {
        let mut ser = flexbuffers::FlexbufferSerializer::new();
        row.serialize(&mut ser).unwrap();
        let serialized_row = ser.take_buffer();

        for index in Self::indexes().iter() {
            if index.is_unique() {
                let hash = index.key_hash(&row);
                let index_val = index.index_key_by(&row);
                let existing_keys =
                    self.get_row_ids_from_index(index.get_id(), &index_val, hash.to_be_bytes())?;
                if existing_keys.len() > 0 {
                    return Err(CubeError::user(
                        format!(
                            "Unique constraint violation: row {:?} has a key that already exists in {:?} index",
                            &row,
                            index
                        )
                    ));
                }
            }
        }

        let row_id = if let Some(row_id) = row_id {
            row_id
        } else {
            self.next_table_seq()?
        };
        let inserted_row = self.insert_row_kv(row_id, serialized_row)?;

        batch_pipe.add_event(MetaStoreEvent::Insert(Self::table_id(), row_id));
        if self.snapshot().get_pinned(&inserted_row.key)?.is_some() {
            return Err(CubeError::internal(format!("Primary key constraint violation. Primary key already exists for a row id {}: {:?}", row_id, &row)));
        }
        batch_pipe.batch().put(inserted_row.key, inserted_row.val);

        let index_row = self.insert_index_row(&row, row_id)?;
        for to_insert in index_row {
            if self.snapshot().get_pinned(&to_insert.key)?.is_some() {
                return Err(CubeError::internal(format!("Primary key constraint violation in secondary index. Primary key already exists for a row id {}: {:?}", row_id, &row)));
            }
            batch_pipe.batch().put(to_insert.key, to_insert.val);
        }

        Ok(IdRow::new(row_id, row))
    }

    fn insert_with_pk(
        &self,
        row_id: u64,
        row: Self::T,
        batch_pipe: &mut BatchPipe,
    ) -> Result<IdRow<Self::T>, CubeError> {
        self.do_insert(Some(row_id), row, batch_pipe)
    }

    fn insert(
        &self,
        row: Self::T,
        batch_pipe: &mut BatchPipe,
    ) -> Result<IdRow<Self::T>, CubeError> {
        self.do_insert(None, row, batch_pipe)
    }

    fn migrate(&self) -> Result<(), CubeError> {
        self.migration_check_table().map_err(|err| {
            CubeError::internal(format!(
                "Error during table (table_id: {:?}) migration: {}",
                Self::table_id(),
                err
            ))
        })?;
        self.migration_check_indexes().map_err(|err| {
            CubeError::internal(format!(
                "Error during indexes (table_id: {:?}) migration: {}",
                Self::table_id(),
                err
            ))
        })?;

        Ok(())
    }

    fn migration_check_table(&self) -> Result<(), CubeError> {
        let snapshot = self.snapshot();

        let table_info = snapshot.get(
            &RowKey::TableInfo {
                table_id: Self::table_id(),
            }
            .to_bytes(),
        )?;

        if let Some(table_info) = table_info {
            let table_info = self.deserialize_table_info(&table_info)?;

            if table_info.version != Self::T::version()
                || table_info.value_version != Self::T::value_version()
            {
                let mut batch = WriteBatch::default();

                log::trace!(
                    "Migrating table {:?} from [{}, {}] to [{}, {}]",
                    Self::table_id(),
                    table_info.version,
                    table_info.value_version,
                    Self::T::version(),
                    Self::T::value_version(),
                );

                self.migrate_table(&mut batch, table_info)?;

                batch.put(
                    &RowKey::TableInfo {
                        table_id: Self::table_id(),
                    }
                    .to_bytes(),
                    self.serialize_table_info(TableInfo {
                        version: Self::T::version(),
                        value_version: Self::T::value_version(),
                    })?
                    .as_slice(),
                );

                self.db().write(batch)?;
            }
        } else {
            self.db().put(
                &RowKey::TableInfo {
                    table_id: Self::table_id(),
                }
                .to_bytes(),
                self.serialize_table_info(TableInfo {
                    version: Self::T::version(),
                    value_version: Self::T::value_version(),
                })?
                .as_slice(),
            )?;
        };

        Ok(())
    }

    fn migration_check_indexes(&self) -> Result<(), CubeError> {
        let snapshot = self.snapshot();
        for index in Self::indexes().into_iter() {
            let index_info = snapshot.get(
                &RowKey::SecondaryIndexInfo {
                    index_id: Self::index_id(index.get_id()),
                }
                .to_bytes(),
            )?;
            if let Some(index_info) = index_info {
                let index_info = self.deserialize_index_info(&index_info)?;
                if index_info.version != index.version()
                    || index_info.value_version != index.value_version()
                {
                    log::trace!(
                        "Migrating index {:?} from [{}, {:?}] to [{}, {:?}]",
                        index,
                        index_info.version,
                        index_info.value_version,
                        index.version(),
                        index.value_version(),
                    );

                    self.rebuild_index(&index)?;
                }
            } else {
                self.rebuild_index(&index)?;
            }
        }
        Ok(())
    }

    fn deserialize_table_info(&self, buffer: &[u8]) -> Result<TableInfo, CubeError> {
        let r = flexbuffers::Reader::get_root(&buffer).unwrap();

        TableInfo::deserialize(r).map_err(|err| {
            CubeError::internal(format!("Deserialization error for TableInfo: {}", err))
        })
    }

    fn serialize_table_info(&self, index_info: TableInfo) -> Result<Vec<u8>, CubeError> {
        let mut ser = flexbuffers::FlexbufferSerializer::new();
        index_info.serialize(&mut ser).unwrap();
        let serialized_row = ser.take_buffer();
        Ok(serialized_row)
    }

    fn deserialize_index_info(&self, buffer: &[u8]) -> Result<SecondaryIndexInfo, CubeError> {
        let r = flexbuffers::Reader::get_root(&buffer).unwrap();

        SecondaryIndexInfo::deserialize(r).map_err(|err| {
            CubeError::internal(format!(
                "Deserialization error for SecondaryIndexInfo: {}",
                err
            ))
        })
    }

    fn serialize_index_info(&self, index_info: SecondaryIndexInfo) -> Result<Vec<u8>, CubeError> {
        let mut ser = flexbuffers::FlexbufferSerializer::new();
        index_info.serialize(&mut ser).unwrap();
        let serialized_row = ser.take_buffer();
        Ok(serialized_row)
    }

    fn rebuild_index(
        &self,
        index: &Box<dyn BaseRocksSecondaryIndex<Self::T>>,
    ) -> Result<(), CubeError> {
        let time = SystemTime::now();
        let mut batch = WriteBatch::default();
        self.delete_all_rows_from_index(index.get_id(), &mut batch)?;

        let all_rows = self.scan_all_rows()?;

        let mut log_shown = false;

        for row in all_rows {
            if !log_shown {
                log::info!(
                    "Rebuilding metastore index {:?} for table {:?}",
                    index,
                    self
                );
                log_shown = true;
            }
            let row = row?;
            let index_row = self.index_key_val(row.get_row(), row.get_id(), index);
            batch.put(index_row.key, index_row.val);
        }
        batch.put(
            &RowKey::SecondaryIndexInfo {
                index_id: Self::index_id(index.get_id()),
            }
            .to_bytes(),
            self.serialize_index_info(SecondaryIndexInfo {
                version: index.version(),
                value_version: index.value_version(),
            })?
            .as_slice(),
        );
        self.db().write(batch)?;
        if log_shown {
            log::info!(
                "Rebuilding metastore index {:?} for table {:?} complete ({:?})",
                index,
                self,
                time.elapsed()?
            );
        }
        Ok(())
    }

    fn get_row_ids_by_index<K: Debug>(
        &self,
        row_key: &K,
        secondary_index: &impl RocksSecondaryIndex<Self::T, K>,
    ) -> Result<Vec<u64>, CubeError>
    where
        K: Hash,
    {
        let hash = secondary_index.typed_key_hash(&row_key);
        let index_val = secondary_index.key_to_bytes(&row_key);
        let existing_keys = self.get_row_ids_from_index(
            RocksSecondaryIndex::get_id(secondary_index),
            &index_val,
            hash.to_be_bytes(),
        )?;

        Ok(existing_keys)
    }

    fn count_rows_by_index<K: Debug + Hash>(
        &self,
        row_key: &K,
        secondary_index: &impl RocksSecondaryIndex<Self::T, K>,
    ) -> Result<u64, CubeError> {
        #[cfg(debug_assertions)]
        if RocksSecondaryIndex::is_unique(secondary_index) {
            return Err(CubeError::internal(format!(
                "Wrong usage of count_rows_by_index, called on unique index for {:?} table",
                self
            )));
        }

        let rows_ids = self.get_row_ids_by_index(row_key, secondary_index)?;
        Ok(rows_ids.len() as u64)
    }

    fn get_row_by_index_opt<K: Debug>(
        &self,
        row_key: &K,
        secondary_index: &impl RocksSecondaryIndex<Self::T, K>,
        reverse: bool,
    ) -> Result<Option<IdRow<Self::T>>, CubeError>
    where
        K: Hash,
    {
        let row_ids = self.get_row_ids_by_index(row_key, secondary_index)?;

        if RocksSecondaryIndex::is_unique(secondary_index) && row_ids.len() > 1 {
            return Err(CubeError::internal(format!(
                "Unique index expected but found multiple values in {:?} table: {:?}",
                self, row_ids
            )));
        }

        let id = if let Some(id) = if reverse {
            row_ids.last()
        } else {
            row_ids.first()
        } {
            id.clone()
        } else {
            return Ok(None);
        };

        if let Some(row) = self.get_row(id)? {
            Ok(Some(row))
        } else {
            let index = self.get_index_by_id(BaseRocksSecondaryIndex::get_id(secondary_index));
            self.rebuild_index(&index)?;

            Err(CubeError::internal(format!(
                "Row exists in secondary index however missing in {:?} table: {}. Repairing index.",
                self, id
            )))
        }
    }

    fn get_rows_by_index<K: Debug>(
        &self,
        row_key: &K,
        secondary_index: &impl RocksSecondaryIndex<Self::T, K>,
    ) -> Result<Vec<IdRow<Self::T>>, CubeError>
    where
        K: Hash,
    {
        let row_ids = self.get_row_ids_by_index(row_key, secondary_index)?;
        let mut res = Vec::with_capacity(row_ids.len());

        for id in row_ids {
            if let Some(row) = self.get_row(id)? {
                res.push(row);
            } else {
                let index = self.get_index_by_id(BaseRocksSecondaryIndex::get_id(secondary_index));
                self.rebuild_index(&index)?;
                return Err(CubeError::internal(format!(
                    "Row exists in secondary index however missing in {:?} table: {}. Repairing index.",
                    self, id
                )));
            }
        }

        if RocksSecondaryIndex::is_unique(secondary_index) && res.len() > 1 {
            return Err(CubeError::internal(format!(
                "Unique index expected but found multiple values in {:?} table: {:?}",
                self, res
            )));
        }

        Ok(res)
    }

    fn get_single_row_by_index<K: Debug>(
        &self,
        row_key: &K,
        secondary_index: &impl RocksSecondaryIndex<Self::T, K>,
    ) -> Result<IdRow<Self::T>, CubeError>
    where
        K: Hash,
    {
        let row = self.get_row_by_index_opt(row_key, secondary_index, false)?;
        row.ok_or(CubeError::internal(format!(
            "One value expected in {:?} for {:?} but nothing found",
            self, row_key
        )))
    }

    fn get_single_opt_row_by_index<K: Debug>(
        &self,
        row_key: &K,
        secondary_index: &impl RocksSecondaryIndex<Self::T, K>,
    ) -> Result<Option<IdRow<Self::T>>, CubeError>
    where
        K: Hash,
    {
        self.get_row_by_index_opt(row_key, secondary_index, false)
    }

    fn get_single_opt_row_by_index_reverse<K: Debug>(
        &self,
        row_key: &K,
        secondary_index: &impl RocksSecondaryIndex<Self::T, K>,
    ) -> Result<Option<IdRow<Self::T>>, CubeError>
    where
        K: Hash,
    {
        self.get_row_by_index_opt(row_key, secondary_index, true)
    }

    fn update_with_fn(
        &self,
        row_id: u64,
        update_fn: impl FnOnce(&Self::T) -> Self::T,
        batch_pipe: &mut BatchPipe,
    ) -> Result<IdRow<Self::T>, CubeError> {
        let row = self.get_row_or_not_found(row_id)?;
        let new_row = update_fn(&row.get_row());
        self.update(row_id, new_row, &row.get_row(), batch_pipe)
    }

    fn update_with_res_fn(
        &self,
        row_id: u64,
        update_fn: impl FnOnce(&Self::T) -> Result<Self::T, CubeError>,
        batch_pipe: &mut BatchPipe,
    ) -> Result<IdRow<Self::T>, CubeError> {
        let row = self.get_row_or_not_found(row_id)?;
        let new_row = update_fn(&row.get_row())?;
        self.update(row_id, new_row, &row.get_row(), batch_pipe)
    }

    fn update(
        &self,
        row_id: u64,
        new_row: Self::T,
        old_row: &Self::T,
        batch_pipe: &mut BatchPipe,
    ) -> Result<IdRow<Self::T>, CubeError> {
        let deleted_row = self.delete_index_row(&old_row, row_id)?;
        for row in deleted_row {
            batch_pipe.batch().delete(row.key);
        }

        let mut ser = flexbuffers::FlexbufferSerializer::new();
        new_row.serialize(&mut ser).unwrap();
        let serialized_row = ser.take_buffer();

        let updated_row = self.update_row_kv(row_id, serialized_row)?;
        batch_pipe.add_event(MetaStoreEvent::Update(Self::table_id(), row_id));

        if self.enable_update_event() {
            batch_pipe.add_event(self.update_event(
                IdRow::new(row_id, old_row.clone()),
                IdRow::new(row_id, new_row.clone()),
            ));
        }

        batch_pipe.batch().put(updated_row.key, updated_row.val);

        let index_row = self.insert_index_row(&new_row, row_id)?;
        for row in index_row {
            batch_pipe.batch().put(row.key, row.val);
        }

        Ok(IdRow::new(row_id, new_row))
    }

    fn truncate(&self, batch_pipe: &mut BatchPipe) -> Result<(), CubeError> {
        let iter = self.table_scan(self.snapshot())?;

        for item in iter {
            let item = item?;

            self.delete_row(item, batch_pipe)?;
        }

        Ok(())
    }

    fn update_extended_ttl_secondary_index<'a, K: Debug>(
        &self,
        row_id: u64,
        secondary_index: &'a impl RocksSecondaryIndex<Self::T, K>,
        secondary_key_hash: SecondaryKeyHash,
        extended: RocksSecondaryIndexValueTTLExtended,
        batch_pipe: &mut BatchPipe,
    ) -> Result<bool, CubeError>
    where
        K: Hash,
    {
        let index_id = RocksSecondaryIndex::get_id(secondary_index);
        let secondary_index_row_key =
            RowKey::SecondaryIndex(Self::index_id(index_id), secondary_key_hash, row_id);
        let secondary_index_key = secondary_index_row_key.to_bytes();

        if let Some(secondary_key_bytes) = self.db().get(&secondary_index_key)? {
            let index_value_version = RocksSecondaryIndex::value_version(secondary_index);
            let new_value = match RocksSecondaryIndexValue::from_bytes(
                &secondary_key_bytes,
                index_value_version,
            )? {
                RocksSecondaryIndexValue::Hash(hash) => {
                    RocksSecondaryIndexValue::HashAndTTLExtended(hash, None, extended)
                }
                RocksSecondaryIndexValue::HashAndTTL(hash, ttl) => {
                    RocksSecondaryIndexValue::HashAndTTLExtended(hash, ttl, extended)
                }
                RocksSecondaryIndexValue::HashAndTTLExtended(hash, ttl, _) => {
                    RocksSecondaryIndexValue::HashAndTTLExtended(hash, ttl, extended)
                }
            };

            batch_pipe.batch().put(
                secondary_index_key,
                new_value.to_bytes(index_value_version)?,
            );

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn delete(&self, row_id: u64, batch_pipe: &mut BatchPipe) -> Result<IdRow<Self::T>, CubeError> {
        let row = self.get_row_or_not_found(row_id)?;
        self.delete_row(row, batch_pipe)
    }

    fn try_delete(
        &self,
        row_id: u64,
        batch_pipe: &mut BatchPipe,
    ) -> Result<Option<IdRow<Self::T>>, CubeError> {
        if let Some(row) = self.get_row(row_id)? {
            Ok(Some(self.delete_row(row, batch_pipe)?))
        } else {
            Ok(None)
        }
    }

    fn delete_row(
        &self,
        row: IdRow<Self::T>,
        batch_pipe: &mut BatchPipe,
    ) -> Result<IdRow<Self::T>, CubeError> {
        let deleted_row = self.delete_index_row(row.get_row(), row.get_id())?;
        batch_pipe.add_event(MetaStoreEvent::Delete(Self::table_id(), row.get_id()));

        if self.enable_delete_event() {
            batch_pipe.add_event(self.delete_event(row.clone()));
        }

        for row in deleted_row {
            batch_pipe.batch().delete(row.key);
        }

        batch_pipe
            .batch()
            .delete(self.delete_row_kv(row.get_id())?.key);

        Ok(row)
    }

    fn next_table_seq(&self) -> Result<u64, CubeError> {
        let ref db = self.db();
        let seq_key = RowKey::Sequence(Self::table_id());
        let before_merge = self
            .snapshot()
            .get(seq_key.to_bytes())?
            .map(|v| Cursor::new(v).read_u64::<BigEndian>().unwrap());

        // TODO revert back merge operator if locking works
        let next_seq = self
            .mem_seq()
            .next_seq(Self::table_id(), before_merge.unwrap_or(0))?;

        let mut to_write = vec![];
        to_write.write_u64::<BigEndian>(next_seq)?;
        db.put(seq_key.to_bytes(), to_write)?;

        Ok(next_seq)
    }

    fn insert_row_kv(&self, row_id: u64, row: Vec<u8>) -> Result<KeyVal, CubeError> {
        let t = RowKey::Table(Self::table_id(), row_id);
        let res = KeyVal {
            key: t.to_bytes(),
            val: row,
        };

        Ok(res)
    }

    fn update_row_kv(&self, row_id: u64, row: Vec<u8>) -> Result<KeyVal, CubeError> {
        let t = RowKey::Table(Self::table_id(), row_id);
        let res = KeyVal {
            key: t.to_bytes(),
            val: row,
        };
        Ok(res)
    }

    fn delete_row_kv(&self, row_id: u64) -> Result<KeyVal, CubeError> {
        let t = RowKey::Table(Self::table_id(), row_id);
        let res = KeyVal {
            key: t.to_bytes(),
            val: vec![],
        };
        Ok(res)
    }

    fn get_row_or_not_found(&self, row_id: u64) -> Result<IdRow<Self::T>, CubeError> {
        self.get_row(row_id)?.ok_or(CubeError::user(format!(
            "Row with id {} is not found for {:?}",
            row_id, self
        )))
    }

    fn get_row(&self, row_id: u64) -> Result<Option<IdRow<Self::T>>, CubeError> {
        let ref db = self.snapshot();
        // Use pinned access to avoid double copying. While zero-copy deserialization would be ideal, but
        // we're using flex buffers with serde, which copies String values during deserialization. There is a way
        // to solve it by using &[u8] types, but it's not worth the effort right now.
        //
        // Let's avoid copying on lookup row, but doing copy on deserialization.
        let res = db.get_pinned(RowKey::Table(Self::table_id(), row_id).to_bytes())?;

        if let Some(buffer) = res {
            let row = self.deserialize_id_row(row_id, &buffer)?;
            return Ok(Some(row));
        }

        Ok(None)
    }

    fn deserialize_id_row(&self, row_id: u64, buffer: &[u8]) -> Result<IdRow<Self::T>, CubeError> {
        let r = flexbuffers::Reader::get_root(&buffer).unwrap();
        let row = self.deserialize_row(r)?;
        return Ok(IdRow::new(row_id, row));
    }

    fn insert_index_row(&self, row: &Self::T, row_id: u64) -> Result<Vec<KeyVal>, CubeError> {
        let indexes = Self::indexes();
        let mut res = Vec::with_capacity(indexes.len());

        for index in indexes.iter() {
            res.push(self.index_key_val(row, row_id, index));
        }

        Ok(res)
    }

    fn index_key_val(
        &self,
        row: &Self::T,
        row_id: u64,
        index: &Box<dyn BaseRocksSecondaryIndex<Self::T>>,
    ) -> KeyVal {
        let hash = index.key_hash(row);
        let index_val = index.index_value(row);
        let key =
            RowKey::SecondaryIndex(Self::index_id(index.get_id()), hash.to_be_bytes(), row_id);

        KeyVal {
            key: key.to_bytes(),
            val: index_val,
        }
    }

    fn delete_index_row(&self, row: &Self::T, row_id: u64) -> Result<Vec<KeyVal>, CubeError> {
        let mut res = Vec::new();
        for index in Self::indexes().iter() {
            let hash = index.key_hash(&row);
            let key =
                RowKey::SecondaryIndex(Self::index_id(index.get_id()), hash.to_be_bytes(), row_id);
            res.push(KeyVal {
                key: key.to_bytes(),
                val: vec![],
            });
        }

        Ok(res)
    }

    fn get_index_by_id(&self, secondary_index: u32) -> Box<dyn BaseRocksSecondaryIndex<Self::T>> {
        Self::indexes()
            .into_iter()
            .find(|i| i.get_id() == secondary_index)
            .unwrap()
    }

    fn collect_table_stats_by_extended_index<'a, K: Debug>(
        &'a self,
        secondary_index: &'a impl RocksSecondaryIndex<Self::T, K>,
        default_size: u64,
    ) -> Result<RocksTableStats, CubeError>
    where
        K: Hash,
    {
        let mut keys_total: u32 = 0;
        let mut expired_keys_total: u32 = 0;
        let mut size_total: u64 = 0;
        let mut expired_size_total: u64 = 0;
        let mut min_row_size: u64 = u64::MAX;
        let mut avg_row_size: u64 = 0;
        let mut max_row_size: u64 = u64::MIN;

        let now = Utc::now();

        for item in self.scan_index_values(secondary_index)? {
            let item = item?;

            let raw_size = if let Some(extended) = item.extended {
                extended.raw_size as u64
            } else {
                default_size
            };

            keys_total += 1;
            size_total += raw_size;

            if max_row_size < raw_size {
                max_row_size = raw_size;
            }

            if min_row_size > raw_size {
                min_row_size = raw_size;
            }

            avg_row_size = (avg_row_size + raw_size) / 2;

            if let Some(ttl) = item.ttl {
                if ttl < now {
                    expired_keys_total += 1;
                    expired_size_total += raw_size;
                }
            }
        }

        if keys_total == 0 {
            max_row_size = 0;
            min_row_size = 0;
        }

        Ok(RocksTableStats {
            table_name: format!("{:?}", Self::table_id()),
            keys_total,
            size_total,
            expired_keys_total,
            expired_size_total,
            min_row_size,
            max_row_size,
            avg_row_size,
        })
    }

    fn get_row_ids_from_index(
        &self,
        secondary_id: u32,
        secondary_key_val: &Vec<u8>,
        secondary_key_hash: SecondaryKeyHash,
    ) -> Result<Vec<u64>, CubeError> {
        let ref db = self.snapshot();
        let key_len = secondary_key_hash.len();
        let key_min = RowKey::SecondaryIndex(Self::index_id(secondary_id), secondary_key_hash, 0);

        let mut res: Vec<u64> = Vec::new();

        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);

        let iter = db.iterator_opt(
            IteratorMode::From(&key_min.to_bytes()[0..(key_len + 5)], Direction::Forward),
            opts,
        );
        let index = self.get_index_by_id(secondary_id);

        for kv_res in iter {
            let (key, value) = kv_res?;
            if let RowKey::SecondaryIndex(_, secondary_index_hash, row_id) =
                RowKey::from_bytes(&key)
            {
                if secondary_index_hash.len() != secondary_key_hash.len()
                    || secondary_index_hash != secondary_key_hash
                {
                    break;
                }

                let (hash, expire) =
                    match RocksSecondaryIndexValue::from_bytes(&*value, index.value_version())? {
                        RocksSecondaryIndexValue::Hash(h) => (h, None),
                        RocksSecondaryIndexValue::HashAndTTL(h, expire) => (h, expire),
                        RocksSecondaryIndexValue::HashAndTTLExtended(h, expire, _) => (h, expire),
                    };

                if hash.len() != secondary_key_val.len() || hash != secondary_key_val.as_slice() {
                    continue;
                }

                if let Some(expire) = expire {
                    if expire > self.table_ref().start_time {
                        res.push(row_id);
                    }
                } else {
                    res.push(row_id);
                }
            };
        }
        Ok(res)
    }

    fn delete_all_rows_from_table(
        &self,
        table_id: TableId,
        batch: &mut WriteBatch,
    ) -> Result<u64, CubeError> {
        let mut total = 0;

        let ref db = self.snapshot();
        let row_key = RowKey::Table(table_id, 0);

        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(false);
        opts.set_iterate_range(row_key.to_iterate_range());

        let iter = db.iterator_opt(IteratorMode::Start, opts);

        for kv_res in iter {
            let (key, _) = kv_res?;

            let row_key = RowKey::try_from_bytes(&key)?;
            if let RowKey::Table(row_table_id, _) = row_key {
                if row_table_id == table_id {
                    total += 1;
                    batch.delete(key);
                } else {
                    return Ok(total);
                }
            }
        }

        Ok(total)
    }

    fn delete_all_rows_from_index(
        &self,
        secondary_id: u32,
        batch: &mut WriteBatch,
    ) -> Result<u64, CubeError> {
        let ref db = self.snapshot();

        let zero_vec = [0 as u8; 8];
        let key_min = RowKey::SecondaryIndex(Self::index_id(secondary_id), zero_vec, 0);

        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(false);
        opts.set_iterate_range(key_min.to_iterate_range());

        let iter = db.iterator_opt(IteratorMode::Start, opts);
        let mut total = 0;

        for kv_res in iter {
            let (key, _) = kv_res?;

            let row_key = RowKey::try_from_bytes(&key)?;
            if let RowKey::SecondaryIndex(index_id, _, _) = row_key {
                if index_id == Self::index_id(secondary_id) {
                    total += 1;
                    batch.delete(key);
                } else {
                    return Ok(total);
                }
            }
        }

        Ok(total)
    }

    fn scan_rows(&self, limit: Option<usize>) -> Result<Vec<IdRow<Self::T>>, CubeError> {
        let iter = self.table_scan(self.snapshot())?;

        let mut res = Vec::new();

        if let Some(limit) = limit {
            for row in iter.take(limit) {
                res.push(row?);
            }
        } else {
            for row in iter {
                res.push(row?);
            }
        };

        Ok(res)
    }

    fn all_rows(&self) -> Result<Vec<IdRow<Self::T>>, CubeError> {
        let mut res = Vec::new();
        for row in self.table_scan(self.snapshot())? {
            res.push(row?);
        }
        Ok(res)
    }

    fn scan_all_rows<'a>(&'a self) -> Result<TableScanIter<'a, Self>, CubeError> {
        self.table_scan(self.snapshot())
    }

    fn scan_index_values<'a, K: Debug>(
        &'a self,
        secondary_index: &'a impl RocksSecondaryIndex<Self::T, K>,
    ) -> Result<SecondaryIndexValueScanIter<'a>, CubeError>
    where
        K: Hash,
    {
        let ref db = self.snapshot();

        let index_id = RocksSecondaryIndex::get_id(secondary_index);
        let zero_vec = [0 as u8; 8];
        let row_key = RowKey::SecondaryIndex(Self::index_id(index_id), zero_vec, 0);

        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(false);
        opts.set_iterate_range(row_key.to_iterate_range());

        let iter = db.iterator_opt(IteratorMode::Start, opts);

        Ok(SecondaryIndexValueScanIter {
            index_id: Self::index_id(index_id),
            index_version: RocksSecondaryIndex::value_version(secondary_index),
            iter,
        })
    }

    fn scan_rows_by_index<'a, K: Debug>(
        &'a self,
        row_key: &'a K,
        secondary_index: &'a impl RocksSecondaryIndex<Self::T, K>,
    ) -> Result<IndexScanIter<'a, Self>, CubeError>
    where
        K: Hash,
    {
        let ref db = self.snapshot();

        let secondary_key_hash = secondary_index.typed_key_hash(&row_key).to_be_bytes() as [u8; 8];
        let secondary_key_val = secondary_index.key_to_bytes(&row_key);

        let index_id = RocksSecondaryIndex::get_id(secondary_index);
        let key_len = secondary_key_hash.len();
        let key_min = RowKey::SecondaryIndex(Self::index_id(index_id), secondary_key_hash, 0);

        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        let iter = db.iterator_opt(
            IteratorMode::From(&key_min.to_bytes()[0..(key_len + 5)], Direction::Forward),
            opts,
        );

        Ok(IndexScanIter {
            table: self,
            index_id,
            secondary_key_val,
            secondary_key_hash,
            iter,
        })
    }

    fn table_scan<'a>(&'a self, db: &'a Snapshot) -> Result<TableScanIter<'a, Self>, CubeError> {
        let my_table_id = Self::table_id();
        let key_min = RowKey::Table(my_table_id, 0);

        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        let iterator = db.iterator_opt(
            IteratorMode::From(
                &key_min.to_bytes()[0..get_fixed_prefix()],
                Direction::Forward,
            ),
            opts,
        );

        Ok(TableScanIter {
            table_id: my_table_id,
            iter: iterator,
            table: self,
        })
    }

    fn build_path_rows<C: Clone, P>(
        &self,
        children: Vec<IdRow<C>>,
        mut parent_id_fn: impl FnMut(&IdRow<C>) -> u64,
        mut path_fn: impl FnMut(IdRow<C>, Arc<IdRow<Self::T>>) -> P,
    ) -> Result<Vec<P>, CubeError> {
        let id_to_child = children
            .into_iter()
            .map(|c| (parent_id_fn(&c), c))
            .collect::<Vec<_>>();
        let ids = id_to_child
            .iter()
            .map(|(id, _)| *id)
            .unique()
            .collect::<Vec<_>>();
        let rows = ids
            .into_iter()
            .map(|id| -> Result<(u64, Arc<IdRow<Self::T>>), CubeError> {
                Ok((id, Arc::new(self.get_row_or_not_found(id)?)))
            })
            .collect::<Result<HashMap<_, _>, _>>()?;
        Ok(id_to_child
            .into_iter()
            .map(|(id, c)| path_fn(c, rows.get(&id).unwrap().clone()))
            .collect::<Vec<_>>())
    }
}
