use super::{
    BaseRocksSecondaryIndex, Column, ColumnType, IndexId, RocksSecondaryIndex, RocksTable, TableId,
};
use super::{DataFrameValue, TableValue};
use crate::base_rocks_secondary_index;
use crate::data_frame_from;
use crate::format_table_value;
use crate::metastore::{IdRow, ImportFormat, MetaStoreEvent, Schema};
use crate::rocks_table_impl;
use crate::store::DataFrame;
use crate::table::Row;
use byteorder::{BigEndian, WriteBytesExt};
use rocksdb::DB;
use serde::{Deserialize, Deserializer, Serialize};
use std::io::Write;
use std::sync::Arc;

data_frame_from! {
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct Table {
    table_name: String,
    schema_id: u64,
    columns: Vec<Column>,
    location: Option<String>,
    import_format: Option<ImportFormat>,
    #[serde(default)]
    has_data: bool
}
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TablePath {
    pub table: IdRow<Table>,
    pub schema: Arc<IdRow<Schema>>,
}

impl TablePath {
    pub fn table_name(&self) -> String {
        let schema_name = self.schema.get_row().get_name();
        let table_name = self.table.get_row().get_table_name();
        format!("{}.{}", schema_name, table_name)
    }
}

impl Table {
    pub fn new(
        table_name: String,
        schema_id: u64,
        columns: Vec<Column>,
        location: Option<String>,
        import_format: Option<ImportFormat>,
    ) -> Table {
        Table {
            table_name,
            schema_id,
            columns,
            location,
            import_format,
            has_data: false,
        }
    }
    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn get_schema_id(&self) -> u64 {
        self.schema_id
    }

    pub fn import_format(&self) -> &Option<ImportFormat> {
        &self.import_format
    }

    pub fn location(&self) -> &Option<String> {
        &self.location
    }

    pub fn get_table_name(&self) -> &String {
        &self.table_name
    }

    pub fn has_data(&self) -> &bool {
        &self.has_data
    }

    pub fn update_has_data(&self, has_data: bool) -> Self {
        Self {
            table_name: self.table_name.clone(),
            schema_id: self.schema_id,
            columns: self.columns.clone(),
            location: self.location.clone(),
            import_format: self.import_format.clone(),
            has_data,
        }
    }
}

impl Column {
    pub fn new(name: String, column_type: ColumnType, column_index: usize) -> Column {
        Column {
            name,
            column_type,
            column_index,
        }
    }
    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_column_type(&self) -> &ColumnType {
        &self.column_type
    }

    pub fn get_index(&self) -> usize {
        self.column_index
    }

    pub fn replace_index(&self, column_index: usize) -> Column {
        Column {
            name: self.name.clone(),
            column_type: self.column_type.clone(),
            column_index,
        }
    }
}

rocks_table_impl!(
    Table,
    TableRocksTable,
    TableId::Tables,
    { vec![Box::new(TableRocksIndex::Name)] },
    DeleteTable
);

#[derive(Clone, Copy, Debug)]
pub(crate) enum TableRocksIndex {
    Name = 1,
}

#[derive(Hash, Clone, Debug)]
pub enum TableIndexKey {
    ByName(u64, String),
}

base_rocks_secondary_index!(Table, TableRocksIndex);

impl RocksSecondaryIndex<Table, TableIndexKey> for TableRocksIndex {
    fn typed_key_by(&self, row: &Table) -> TableIndexKey {
        match self {
            TableRocksIndex::Name => {
                TableIndexKey::ByName(row.schema_id, row.table_name.to_string())
            }
        }
    }

    fn key_to_bytes(&self, key: &TableIndexKey) -> Vec<u8> {
        match key {
            TableIndexKey::ByName(schema_id, table_name) => {
                let mut buf = Vec::new();
                buf.write_u64::<BigEndian>(*schema_id).unwrap();
                buf.write_all(table_name.as_bytes()).unwrap();
                buf
            }
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            TableRocksIndex::Name => true,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
