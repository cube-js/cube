use super::{IndexId, RocksSecondaryIndex, TableId};
use crate::metastore::table::Table;
use crate::metastore::{IdRow, RocksEntity};
use crate::rocks_table_impl;
use crate::{base_rocks_secondary_index, CubeError};
use byteorder::{BigEndian, WriteBytesExt};
use chrono::{DateTime, Utc};
use itertools::Itertools;

use serde::{Deserialize, Deserializer, Serialize};
use std::fmt::Debug;

crate::data_frame_from! {
#[derive(Clone, Serialize, Deserialize, Debug, Hash)]
pub struct ReplayHandle {
    table_id: u64,
    created_at: DateTime<Utc>,
    seq_pointers_by_location: Option<Vec<Option<SeqPointer>>>,
    has_failed_to_persist_chunks: bool
}
}

impl RocksEntity for ReplayHandle {}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct SeqPointer {
    start_seq: Option<i64>,
    end_seq: Option<i64>,
}

impl SeqPointer {
    pub fn new(start_seq: Option<i64>, end_seq: Option<i64>) -> Self {
        SeqPointer { start_seq, end_seq }
    }

    pub fn empty() -> Self {
        Self::new(None, None)
    }

    pub fn is_empty(&self) -> bool {
        self.start_seq.is_none() && self.end_seq.is_none()
    }

    pub fn start_seq(&self) -> &Option<i64> {
        &self.start_seq
    }

    pub fn end_seq(&self) -> &Option<i64> {
        &self.end_seq
    }

    /// Subtract interval from right.
    /// Used to determine which part of rows persisted for sure by subtracting failed in memory part.
    /// ```md
    /// self   |----------------|
    /// other         |------|
    /// result |------|
    /// ```
    pub fn subtract_from_right(&mut self, other: &Self) {
        if let Some((other_start_seq, end_seq)) =
            other.start_seq.as_ref().zip(self.end_seq.as_ref())
        {
            self.end_seq = Some((*end_seq).min(*other_start_seq));
        }

        if let Some((start_seq, end_seq)) = self.start_seq.as_ref().zip(self.end_seq.as_ref()) {
            if end_seq <= start_seq {
                self.start_seq = None;
                self.end_seq = None;
            }
        }
    }

    /// Subtract interval from left.
    /// Similar to `subtract_from_right` but opposite direction.
    /// ```md
    /// self          |--|
    /// other         |------|
    /// result None
    /// ```
    pub fn subtract_if_covers(&mut self, other: &Self) {
        if let Some((other_start, other_end)) = other.start_seq.as_ref().zip(other.end_seq.as_ref())
        {
            if let Some((start, end)) = self.start_seq.as_ref().zip(self.end_seq.as_ref()) {
                if other_start <= start && end <= other_end {
                    self.start_seq = None;
                    self.end_seq = None;
                }
            }
        }
    }

    pub fn union(&mut self, other: &Self) {
        if let Some(other_start_seq) = other.start_seq {
            if let Some(start_seq) = self.start_seq {
                self.start_seq = Some(other_start_seq.min(start_seq));
            } else {
                self.start_seq = Some(other_start_seq);
            }
        }

        if let Some(other_end_seq) = other.end_seq {
            if let Some(end_seq) = self.end_seq {
                self.end_seq = Some(other_end_seq.max(end_seq));
            } else {
                self.end_seq = Some(other_end_seq);
            }
        }
    }
}

impl ReplayHandle {
    pub fn new_from_seq_pointers(
        table_id: u64,
        seq_pointers_by_location: Option<Vec<Option<SeqPointer>>>,
    ) -> Self {
        Self {
            table_id,
            created_at: Utc::now(),
            has_failed_to_persist_chunks: false,
            seq_pointers_by_location,
        }
    }

    pub fn new(
        table: &IdRow<Table>,
        location_index: usize,
        seq_pointer: SeqPointer,
    ) -> Result<Self, CubeError> {
        let mut seq_pointers_by_location = vec![
            None;
            table
                .get_row()
                .locations()
                .ok_or_else(|| CubeError::internal(format!(
                    "Table without locations used to create ReplayHandle: {:?}",
                    table
                )))?
                .len()
        ];
        seq_pointers_by_location[location_index] = Some(seq_pointer);
        Ok(Self {
            table_id: table.get_id(),
            created_at: Utc::now(),
            has_failed_to_persist_chunks: false,
            seq_pointers_by_location: Some(seq_pointers_by_location),
        })
    }

    pub fn table_id(&self) -> u64 {
        self.table_id
    }

    pub fn created_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }

    pub fn has_failed_to_persist_chunks(&self) -> bool {
        self.has_failed_to_persist_chunks
    }

    pub fn set_failed_to_persist_chunks(&self, failed: bool) -> Self {
        let mut handle = self.clone();
        handle.has_failed_to_persist_chunks = failed;
        handle
    }
}

impl SeqPointerForLocation for ReplayHandle {
    fn seq_pointers_by_location(&self) -> &Option<Vec<Option<SeqPointer>>> {
        &self.seq_pointers_by_location
    }
}

pub trait SeqPointerForLocation: Debug + Clone {
    fn seq_pointers_by_location(&self) -> &Option<Vec<Option<SeqPointer>>>;

    fn seq_pointer_for_location(
        &self,
        table: &IdRow<Table>,
        location: &str,
    ) -> Result<&Option<SeqPointer>, CubeError> {
        let seq_pointers_by_location =
            self.seq_pointers_by_location().as_ref().ok_or_else(|| {
                CubeError::internal(format!(
                    "Seq pointers are not defined but expected for: {:?}",
                    self
                ))
            })?;
        seq_pointer_for_location(seq_pointers_by_location, table, location)
    }
}

pub fn seq_pointer_for_location<'a>(
    seq_pointers_by_location: &'a Vec<Option<SeqPointer>>,
    table: &IdRow<Table>,
    location: &str,
) -> Result<&'a Option<SeqPointer>, CubeError> {
    let locations = table.get_row().locations().ok_or_else(|| {
        CubeError::internal(format!(
            "Locations are not defined but expected for: {:?}",
            table
        ))
    })?;
    if locations.len() != seq_pointers_by_location.len() {
        return Err(CubeError::internal(format!(
            "Location array size mismatch during accessing seq pointers: {:?} and {:?}",
            table.get_row().locations(),
            seq_pointers_by_location
        )));
    }
    let pos = location_position(table, location)?;
    Ok(&seq_pointers_by_location[pos])
}

pub fn location_position(table: &IdRow<Table>, location: &str) -> Result<usize, CubeError> {
    let locations = table.get_row().locations().ok_or_else(|| {
        CubeError::internal(format!(
            "Locations are not defined but expected for: {:?}",
            table
        ))
    })?;
    let (pos, _) = locations
        .iter()
        .find_position(|l| l.as_str() == location)
        .ok_or_else(|| {
            CubeError::internal(format!(
                "Location '{}' is not found in table: {:?}",
                location, table
            ))
        })?;
    Ok(pos)
}

pub fn union_seq_pointer_by_location(
    seq_pointer_by_location: &mut Option<Vec<Option<SeqPointer>>>,
    other_seq_pointer_by_location: &Option<Vec<Option<SeqPointer>>>,
) -> Result<(), CubeError> {
    merge_seq_pointer_by_location(
        seq_pointer_by_location,
        other_seq_pointer_by_location,
        |a, b| {
            if let Some((a, b)) = a.as_mut().zip(b.as_ref()) {
                a.union(b);
            } else if let Some(b) = b.as_ref() {
                *a = Some(b.clone());
            }
        },
    )
}

pub fn subtract_from_right_seq_pointer_by_location(
    seq_pointer_by_location: &mut Option<Vec<Option<SeqPointer>>>,
    other_seq_pointer_by_location: &Option<Vec<Option<SeqPointer>>>,
) -> Result<(), CubeError> {
    merge_seq_pointer_by_location(
        seq_pointer_by_location,
        other_seq_pointer_by_location,
        |a, b| {
            if let Some((a, b)) = a.as_mut().zip(b.as_ref()) {
                a.subtract_from_right(b);
            }
        },
    )
}

pub fn subtract_if_covers_seq_pointer_by_location(
    seq_pointer_by_location: &mut Option<Vec<Option<SeqPointer>>>,
    other_seq_pointer_by_location: &Option<Vec<Option<SeqPointer>>>,
) -> Result<(), CubeError> {
    merge_seq_pointer_by_location(
        seq_pointer_by_location,
        other_seq_pointer_by_location,
        |a, b| {
            if let Some((a, b)) = a.as_mut().zip(b.as_ref()) {
                a.subtract_if_covers(b);
            }
        },
    )
}

pub fn merge_seq_pointer_by_location(
    seq_pointer_by_location: &mut Option<Vec<Option<SeqPointer>>>,
    other_seq_pointer_by_location: &Option<Vec<Option<SeqPointer>>>,
    merge_operation: fn(&mut Option<SeqPointer>, &Option<SeqPointer>),
) -> Result<(), CubeError> {
    if let Some(seq_pointers) = other_seq_pointer_by_location {
        match seq_pointer_by_location {
            Some(seq_pointers_by_location) => {
                if seq_pointers_by_location.len() != seq_pointers.len() {
                    return Err(CubeError::internal(format!(
                        "Location array size mismatch during updating seq pointers: {:?} and {:?}",
                        seq_pointers_by_location, other_seq_pointer_by_location
                    )));
                }
                for (to_update, from) in
                    seq_pointers_by_location.iter_mut().zip(seq_pointers.iter())
                {
                    merge_operation(to_update, from);
                }
            }
            None => {
                let mut new_seq_pointers = vec![None; seq_pointers.len()];

                for (to_update, from) in new_seq_pointers.iter_mut().zip(seq_pointers.iter()) {
                    merge_operation(to_update, from);
                }

                *seq_pointer_by_location = Some(new_seq_pointers);
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub enum ReplayHandleRocksIndex {
    ByTableId = 1,
}

base_rocks_secondary_index!(ReplayHandle, ReplayHandleRocksIndex);

rocks_table_impl!(
    ReplayHandle,
    ReplayHandleRocksTable,
    TableId::ReplayHandles,
    { vec![Box::new(ReplayHandleRocksIndex::ByTableId),] }
);

#[derive(Hash, Clone, Debug)]
pub enum ReplayHandleIndexKey {
    ByTableId(u64),
}

impl RocksSecondaryIndex<ReplayHandle, ReplayHandleIndexKey> for ReplayHandleRocksIndex {
    fn typed_key_by(&self, row: &ReplayHandle) -> ReplayHandleIndexKey {
        match self {
            ReplayHandleRocksIndex::ByTableId => ReplayHandleIndexKey::ByTableId(row.table_id),
        }
    }

    fn key_to_bytes(&self, key: &ReplayHandleIndexKey) -> Vec<u8> {
        match key {
            ReplayHandleIndexKey::ByTableId(table_id) => {
                let mut buf = Vec::with_capacity(8);
                buf.write_u64::<BigEndian>(*table_id).unwrap();
                buf
            }
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            ReplayHandleRocksIndex::ByTableId => false,
        }
    }

    fn version(&self) -> u32 {
        match self {
            ReplayHandleRocksIndex::ByTableId => 1,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
