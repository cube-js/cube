use crate::metastore::rocks_store::TableId;
use crate::metastore::{
    get_fixed_prefix, BatchPipe, IdRow, IndexId, KeyVal, MemorySequence, MetaStoreEvent, RowKey,
    SecondaryIndexInfo,
};
use crate::CubeError;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use itertools::Itertools;
use rocksdb::{DBIterator, Direction, IteratorMode, ReadOptions, Snapshot, WriteBatch, DB};
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
            pub fn new(db: crate::metastore::DbTableRef<'a>) -> $rocks_table {
                $rocks_table { db }
            }
        }

        impl<'a> crate::metastore::RocksTable for $rocks_table<'a> {
            type T = $table;

            fn db(&self) -> &rocksdb::DB {
                self.db.db
            }

            fn snapshot(&self) -> &rocksdb::Snapshot {
                self.db.snapshot
            }

            fn mem_seq(&self) -> &crate::metastore::MemorySequence {
                &self.db.mem_seq
            }

            fn table_id(&self) -> TableId {
                $table_id
            }

            fn index_id(&self, index_num: IndexId) -> IndexId {
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

    fn version(&self) -> u32;
}

pub trait RocksSecondaryIndex<T, K: Hash>: BaseRocksSecondaryIndex<T> {
    fn typed_key_by(&self, row: &T) -> K;

    fn key_to_bytes(&self, key: &K) -> Vec<u8>;

    fn typed_key_hash(&self, row_key: &K) -> u64 {
        let key_bytes = self.key_to_bytes(row_key);
        self.hash_bytes(&key_bytes)
    }

    fn index_key_by(&self, row: &T) -> Vec<u8> {
        self.key_to_bytes(&self.typed_key_by(row))
    }

    fn get_id(&self) -> u32;

    fn is_unique(&self) -> bool;

    fn version(&self) -> u32;
}

impl<T, I> BaseRocksSecondaryIndex<T> for I
where
    I: RocksSecondaryIndex<T, String>,
{
    fn index_key_by(&self, row: &T) -> Vec<u8> {
        self.key_to_bytes(&self.typed_key_by(row))
    }

    fn get_id(&self) -> u32 {
        RocksSecondaryIndex::get_id(self)
    }

    fn is_unique(&self) -> bool {
        RocksSecondaryIndex::is_unique(self)
    }

    fn version(&self) -> u32 {
        RocksSecondaryIndex::version(self)
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
        if let Some((key, value)) = option {
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
    secondary_key_val: Vec<u8>,
    secondary_key_hash: Vec<u8>,
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
            if let Some((key, value)) = option {
                if let RowKey::SecondaryIndex(_, secondary_index_hash, row_id) =
                    RowKey::from_bytes(&key)
                {
                    if &secondary_index_hash != self.secondary_key_hash.as_slice() {
                        return None;
                    }

                    if self.secondary_key_val.as_slice() != value.as_ref() {
                        continue;
                    }

                    return Some(self.table.get_row_or_not_found(row_id));
                };
            } else {
                return None;
            }
        }
    }
}

pub trait RocksTable: Debug + Send + Sync {
    type T: Serialize + Clone + Debug + Send;
    fn delete_event(&self, row: IdRow<Self::T>) -> MetaStoreEvent;
    fn update_event(&self, old_row: IdRow<Self::T>, new_row: IdRow<Self::T>) -> MetaStoreEvent;
    fn db(&self) -> &DB;
    fn snapshot(&self) -> &Snapshot;
    fn mem_seq(&self) -> &MemorySequence;
    fn index_id(&self, index_num: IndexId) -> IndexId;
    fn table_id(&self) -> TableId;
    fn deserialize_row<'de, D>(&self, deserializer: D) -> Result<Self::T, D::Error>
    where
        D: Deserializer<'de>;
    fn indexes() -> Vec<Box<dyn BaseRocksSecondaryIndex<Self::T>>>;

    fn insert(
        &self,
        row: Self::T,
        batch_pipe: &mut BatchPipe,
    ) -> Result<IdRow<Self::T>, CubeError> {
        let mut ser = flexbuffers::FlexbufferSerializer::new();
        row.serialize(&mut ser).unwrap();
        let serialized_row = ser.take_buffer();

        for index in Self::indexes().iter() {
            let hash = index.key_hash(&row);
            let index_val = index.index_key_by(&row);
            let existing_keys =
                self.get_row_from_index(index.get_id(), &index_val, &hash.to_be_bytes().to_vec())?;
            if index.is_unique() && existing_keys.len() > 0 {
                return Err(CubeError::user(
                    format!(
                        "Unique constraint violation: row {:?} has a key that already exists in {:?} index",
                        &row,
                        index
                    )
                ));
            }
        }

        let (row_id, inserted_row) = self.insert_row(serialized_row)?;
        batch_pipe.add_event(MetaStoreEvent::Insert(self.table_id(), row_id));
        if self.snapshot().get(&inserted_row.key)?.is_some() {
            return Err(CubeError::internal(format!("Primary key constraint violation. Primary key already exists for a row id {}: {:?}", row_id, &row)));
        }
        batch_pipe.batch().put(inserted_row.key, inserted_row.val);

        let index_row = self.insert_index_row(&row, row_id)?;
        for to_insert in index_row {
            if self.snapshot().get(&to_insert.key)?.is_some() {
                return Err(CubeError::internal(format!("Primary key constraint violation in secondary index. Primary key already exists for a row id {}: {:?}", row_id, &row)));
            }
            batch_pipe.batch().put(to_insert.key, to_insert.val);
        }

        Ok(IdRow::new(row_id, row))
    }

    fn check_indexes(&self) -> Result<(), CubeError> {
        let snapshot = self.snapshot();
        for index in Self::indexes().into_iter() {
            let index_info = snapshot.get(
                &RowKey::SecondaryIndexInfo {
                    index_id: self.index_id(index.get_id()),
                }
                .to_bytes(),
            )?;
            if let Some(index_info) = index_info {
                let index_info = self.deserialize_index_info(index_info.as_slice())?;
                if index_info.version != index.version() {
                    self.rebuild_index(&index)?;
                }
            } else {
                self.rebuild_index(&index)?;
            }
        }
        Ok(())
    }

    fn deserialize_index_info(&self, buffer: &[u8]) -> Result<SecondaryIndexInfo, CubeError> {
        let r = flexbuffers::Reader::get_root(&buffer).unwrap();
        let row = SecondaryIndexInfo::deserialize(r)?;
        Ok(row)
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
                index_id: self.index_id(index.get_id()),
            }
            .to_bytes(),
            self.serialize_index_info(SecondaryIndexInfo {
                version: index.version(),
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
        let existing_keys = self.get_row_from_index(
            RocksSecondaryIndex::get_id(secondary_index),
            &index_val,
            &hash.to_be_bytes().to_vec(),
        )?;

        Ok(existing_keys)
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

        let mut res = Vec::new();

        for id in row_ids {
            if let Some(row) = self.get_row(id)? {
                res.push(row);
            } else {
                let index = Self::indexes()
                    .into_iter()
                    .find(|i| i.get_id() == BaseRocksSecondaryIndex::get_id(secondary_index))
                    .unwrap();
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
        let rows = self.get_rows_by_index(row_key, secondary_index)?;
        Ok(rows.into_iter().nth(0).ok_or(CubeError::internal(format!(
            "One value expected in {:?} for {:?} but nothing found",
            self, row_key
        )))?)
    }

    fn get_single_opt_row_by_index<K: Debug>(
        &self,
        row_key: &K,
        secondary_index: &impl RocksSecondaryIndex<Self::T, K>,
    ) -> Result<Option<IdRow<Self::T>>, CubeError>
    where
        K: Hash,
    {
        let rows = self.get_rows_by_index(row_key, secondary_index)?;
        Ok(rows.into_iter().nth(0))
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

        let updated_row = self.update_row(row_id, serialized_row)?;
        batch_pipe.add_event(MetaStoreEvent::Update(self.table_id(), row_id));
        batch_pipe.add_event(self.update_event(
            IdRow::new(row_id, old_row.clone()),
            IdRow::new(row_id, new_row.clone()),
        ));
        batch_pipe.batch().put(updated_row.key, updated_row.val);

        let index_row = self.insert_index_row(&new_row, row_id)?;
        for row in index_row {
            batch_pipe.batch().put(row.key, row.val);
        }
        Ok(IdRow::new(row_id, new_row))
    }

    fn delete(&self, row_id: u64, batch_pipe: &mut BatchPipe) -> Result<IdRow<Self::T>, CubeError> {
        let row = self.get_row_or_not_found(row_id)?;
        let deleted_row = self.delete_index_row(row.get_row(), row_id)?;
        batch_pipe.add_event(MetaStoreEvent::Delete(self.table_id(), row_id));
        batch_pipe.add_event(self.delete_event(row.clone()));
        for row in deleted_row {
            batch_pipe.batch().delete(row.key);
        }

        batch_pipe.batch().delete(self.delete_row(row_id)?.key);

        Ok(row)
    }

    fn next_table_seq(&self) -> Result<u64, CubeError> {
        let ref db = self.db();
        let seq_key = RowKey::Sequence(self.table_id());
        let before_merge = self
            .snapshot()
            .get(seq_key.to_bytes())?
            .map(|v| Cursor::new(v).read_u64::<BigEndian>().unwrap());

        // TODO revert back merge operator if locking works
        let next_seq = self
            .mem_seq()
            .next_seq(self.table_id(), before_merge.unwrap_or(0))?;

        let mut to_write = vec![];
        to_write.write_u64::<BigEndian>(next_seq)?;
        db.put(seq_key.to_bytes(), to_write)?;

        Ok(next_seq)
    }

    fn insert_row(&self, row: Vec<u8>) -> Result<(u64, KeyVal), CubeError> {
        let next_seq = self.next_table_seq()?;
        let t = RowKey::Table(self.table_id(), next_seq);
        let res = KeyVal {
            key: t.to_bytes(),
            val: row,
        };
        Ok((next_seq, res))
    }

    fn update_row(&self, row_id: u64, row: Vec<u8>) -> Result<KeyVal, CubeError> {
        let t = RowKey::Table(self.table_id(), row_id);
        let res = KeyVal {
            key: t.to_bytes(),
            val: row,
        };
        Ok(res)
    }

    fn delete_row(&self, row_id: u64) -> Result<KeyVal, CubeError> {
        let t = RowKey::Table(self.table_id(), row_id);
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
        let res = db.get(RowKey::Table(self.table_id(), row_id).to_bytes())?;

        if let Some(buffer) = res {
            let row = self.deserialize_id_row(row_id, buffer.as_slice())?;
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
        let mut res = Vec::new();
        for index in Self::indexes().iter() {
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
        let index_val = index.index_key_by(row);
        let key = RowKey::SecondaryIndex(
            self.index_id(index.get_id()),
            hash.to_be_bytes().to_vec(),
            row_id,
        );
        KeyVal {
            key: key.to_bytes(),
            val: index_val,
        }
    }

    fn delete_index_row(&self, row: &Self::T, row_id: u64) -> Result<Vec<KeyVal>, CubeError> {
        let mut res = Vec::new();
        for index in Self::indexes().iter() {
            let hash = index.key_hash(&row);
            let key = RowKey::SecondaryIndex(
                self.index_id(index.get_id()),
                hash.to_be_bytes().to_vec(),
                row_id,
            );
            res.push(KeyVal {
                key: key.to_bytes(),
                val: vec![],
            });
        }

        Ok(res)
    }

    fn get_row_from_index(
        &self,
        secondary_id: u32,
        secondary_key_val: &Vec<u8>,
        secondary_key_hash: &Vec<u8>,
    ) -> Result<Vec<u64>, CubeError> {
        let ref db = self.snapshot();
        let key_len = secondary_key_hash.len();
        let key_min =
            RowKey::SecondaryIndex(self.index_id(secondary_id), secondary_key_hash.clone(), 0);

        let mut res: Vec<u64> = Vec::new();

        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        let iter = db.iterator_opt(
            IteratorMode::From(&key_min.to_bytes()[0..(key_len + 5)], Direction::Forward),
            opts,
        );

        for (key, value) in iter {
            if let RowKey::SecondaryIndex(_, secondary_index_hash, row_id) =
                RowKey::from_bytes(&key)
            {
                if !secondary_index_hash
                    .iter()
                    .zip(secondary_key_hash)
                    .all(|(a, b)| a == b)
                {
                    break;
                }

                if secondary_key_val.len() != value.len()
                    || !value.iter().zip(secondary_key_val).all(|(a, b)| a == b)
                {
                    continue;
                }
                res.push(row_id);
            };
        }
        Ok(res)
    }

    fn delete_all_rows_from_index(
        &self,
        secondary_id: u32,
        batch: &mut WriteBatch,
    ) -> Result<(), CubeError> {
        let ref db = self.snapshot();
        let zero_vec = vec![0 as u8; 8];
        let key_len = zero_vec.len();
        let key_min = RowKey::SecondaryIndex(self.index_id(secondary_id), zero_vec.clone(), 0);

        let iter = db.iterator(IteratorMode::From(
            &key_min.to_bytes()[0..(key_len + 5)],
            Direction::Forward,
        ));

        for (key, _) in iter {
            let row_key = RowKey::from_bytes(&key);
            if let RowKey::SecondaryIndex(index_id, _, _) = row_key {
                if index_id == self.index_id(secondary_id) {
                    batch.delete(key);
                } else {
                    return Ok(());
                }
            }
        }
        Ok(())
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

    fn scan_rows_by_index<'a, K: Debug>(
        &'a self,
        row_key: &'a K,
        secondary_index: &'a impl RocksSecondaryIndex<Self::T, K>,
    ) -> Result<IndexScanIter<'a, Self>, CubeError>
    where
        K: Hash,
    {
        let ref db = self.snapshot();

        let secondary_key_hash = secondary_index
            .typed_key_hash(&row_key)
            .to_be_bytes()
            .to_vec();
        let secondary_key_val = secondary_index.key_to_bytes(&row_key);

        let key_len = secondary_key_hash.len();
        let key_min = RowKey::SecondaryIndex(
            self.index_id(RocksSecondaryIndex::get_id(secondary_index)),
            secondary_key_hash.clone(),
            0,
        );

        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        let iter = db.iterator_opt(
            IteratorMode::From(&key_min.to_bytes()[0..(key_len + 5)], Direction::Forward),
            opts,
        );

        Ok(IndexScanIter {
            table: self,
            secondary_key_val,
            secondary_key_hash,
            iter,
        })
    }

    fn table_scan<'a>(&'a self, db: &'a Snapshot) -> Result<TableScanIter<'a, Self>, CubeError> {
        let my_table_id = self.table_id();
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
