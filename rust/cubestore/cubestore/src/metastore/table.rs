use super::{
    AggregateFunction, BaseRocksSecondaryIndex, Column, ColumnType, DataFrameValue, IndexId,
    RocksSecondaryIndex, RocksTable, TableId,
};
use crate::data_frame_from;
use crate::metastore::{IdRow, ImportFormat, MetaStoreEvent, Schema};
use crate::queryplanner::udfs::aggregate_udf_by_kind;
use crate::queryplanner::udfs::CubeAggregateUDFKind;
use crate::rocks_table_impl;
use crate::{base_rocks_secondary_index, CubeError};
use arrow::datatypes::Schema as ArrowSchema;
use byteorder::{BigEndian, WriteBytesExt};
use chrono::DateTime;
use chrono::Utc;
use datafusion::physical_plan::expressions::{Column as FusionColumn, Max, Min, Sum};
use datafusion::physical_plan::{udaf, AggregateExpr, PhysicalExpr};
use itertools::Itertools;
use rocksdb::DB;
use serde::{Deserialize, Deserializer, Serialize};
use std::io::Write;
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct AggregateColumnIndex {
    index: u64,
    function: AggregateFunction,
}

impl AggregateColumnIndex {
    pub fn new(index: u64, function: AggregateFunction) -> Self {
        Self { index, function }
    }

    pub fn index(&self) -> u64 {
        self.index
    }

    pub fn function(&self) -> &AggregateFunction {
        &self.function
    }
}

impl DataFrameValue<String> for Vec<AggregateColumnIndex> {
    fn value(v: &Self) -> String {
        v.iter()
            .map(|v| format!("{}({})", v.function, v.index))
            .join(", ")
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct AggregateColumn {
    column: Column,
    function: AggregateFunction,
}

impl AggregateColumn {
    pub fn new(column: Column, function: AggregateFunction) -> Self {
        Self { column, function }
    }

    pub fn column(&self) -> &Column {
        &self.column
    }

    pub fn function(&self) -> &AggregateFunction {
        &self.function
    }

    pub fn aggregate_expr(
        &self,
        schema: &ArrowSchema,
    ) -> Result<Arc<dyn AggregateExpr>, CubeError> {
        let col = Arc::new(FusionColumn::new_with_schema(
            self.column.get_name().as_str(),
            &schema,
        )?);
        let res: Arc<dyn AggregateExpr> = match self.function {
            AggregateFunction::SUM => {
                Arc::new(Sum::new(col.clone(), col.name(), col.data_type(schema)?))
            }
            AggregateFunction::MAX => {
                Arc::new(Max::new(col.clone(), col.name(), col.data_type(schema)?))
            }
            AggregateFunction::MIN => {
                Arc::new(Min::new(col.clone(), col.name(), col.data_type(schema)?))
            }
            AggregateFunction::MERGE => {
                let fun = aggregate_udf_by_kind(CubeAggregateUDFKind::MergeHll).descriptor();
                udaf::create_aggregate_expr(&fun, &[col.clone()], schema, col.name())?
            }
        };
        Ok(res)
    }
}

impl core::fmt::Display for AggregateColumn {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_fmt(format_args!(
            "{}({})",
            self.function,
            self.column.get_name()
        ))
    }
}

data_frame_from! {
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct Table {
    table_name: String,
    schema_id: u64,
    columns: Vec<Column>,
    #[serde(default)]
    locations: Option<Vec<String>>,
    import_format: Option<ImportFormat>,
    #[serde(default)]
    has_data: bool,
    #[serde(default="Table::is_ready_default")]
    is_ready: bool,
    #[serde(default)]
    created_at: Option<DateTime<Utc>>,
    #[serde(default)]
    build_range_end: Option<DateTime<Utc>>,
    #[serde(default)]
    unique_key_column_indices: Option<Vec<u64>>,
    #[serde(default)]
    aggregate_column_indices: Vec<AggregateColumnIndex>,
    #[serde(default)]
    seq_column_index: Option<u64>,
    #[serde(default)]
    location_download_sizes: Option<Vec<u64>>,
    #[serde(default)]
    partition_split_threshold: Option<u64>
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
        locations: Option<Vec<String>>,
        import_format: Option<ImportFormat>,
        is_ready: bool,
        build_range_end: Option<DateTime<Utc>>,
        unique_key_column_indices: Option<Vec<u64>>,
        aggregate_column_indices: Vec<AggregateColumnIndex>,
        seq_column_index: Option<u64>,
        partition_split_threshold: Option<u64>,
    ) -> Table {
        let location_download_sizes = locations.as_ref().map(|locations| vec![0; locations.len()]);
        Table {
            table_name,
            schema_id,
            columns,
            locations,
            import_format,
            has_data: false,
            is_ready,
            created_at: Some(Utc::now()),
            build_range_end,
            unique_key_column_indices,
            aggregate_column_indices,
            seq_column_index,
            location_download_sizes,
            partition_split_threshold,
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

    pub fn locations(&self) -> Option<Vec<&String>> {
        self.locations.as_ref().map(|l| l.iter().collect())
    }

    pub fn get_table_name(&self) -> &String {
        &self.table_name
    }

    pub fn has_data(&self) -> &bool {
        &self.has_data
    }

    pub fn update_has_data(&self, has_data: bool) -> Self {
        let mut table = self.clone();
        table.has_data = has_data;
        table
    }

    pub fn is_ready(&self) -> bool {
        self.is_ready
    }

    pub fn update_is_ready(&self, is_ready: bool) -> Self {
        let mut table = self.clone();
        table.is_ready = is_ready;
        table
    }

    pub fn update_location_download_size(
        &self,
        location: &str,
        download_size: u64,
    ) -> Result<Self, CubeError> {
        let mut table = self.clone();
        let locations = table.locations.as_ref().ok_or(CubeError::internal(format!(
            "Can't update location size for table without locations: {:?}",
            self
        )))?;
        let (pos, _) =
            locations
                .iter()
                .find_position(|l| l == &location)
                .ok_or(CubeError::internal(format!(
                    "Can't update location size: location '{}' not found in {:?}",
                    location, locations
                )))?;
        if table.location_download_sizes.is_none() {
            table.location_download_sizes = Some(vec![0; locations.len()]);
        }
        table.location_download_sizes.as_mut().unwrap()[pos] = download_size;
        Ok(table)
    }

    pub fn total_download_size(&self) -> u64 {
        self.location_download_sizes
            .as_ref()
            .map(|sizes| sizes.iter().sum::<u64>())
            .unwrap_or(0)
    }

    pub fn is_ready_default() -> bool {
        true
    }

    pub fn created_at(&self) -> &Option<DateTime<Utc>> {
        &self.created_at
    }

    pub fn build_range_end(&self) -> &Option<DateTime<Utc>> {
        &self.build_range_end
    }

    pub fn unique_key_columns(&self) -> Option<Vec<&Column>> {
        self.unique_key_column_indices
            .as_ref()
            .map(|indices| indices.iter().map(|i| &self.columns[*i as usize]).collect())
    }

    pub fn aggregate_columns(&self) -> Vec<AggregateColumn> {
        self.aggregate_column_indices
            .iter()
            .map(|v| {
                AggregateColumn::new(self.columns[v.index as usize].clone(), v.function.clone())
            })
            .collect()
    }

    pub fn aggregate_column_indices(&self) -> &Vec<AggregateColumnIndex> {
        &self.aggregate_column_indices
    }

    pub fn seq_column(&self) -> Option<&Column> {
        self.seq_column_index
            .as_ref()
            .map(|c| &self.columns[*c as usize])
    }

    pub fn in_memory_ingest(&self) -> bool {
        self.seq_column_index.is_some()
    }

    pub fn is_stream_location(location: &str) -> bool {
        location.starts_with("stream:")
    }

    pub fn partition_split_threshold(&self) -> &Option<u64> {
        &self.partition_split_threshold
    }

    pub fn partition_split_threshold_or_default(
        &self,
        config_partition_split_threshold: u64,
    ) -> u64 {
        self.partition_split_threshold
            .as_ref()
            .map(|v| *v)
            .unwrap_or(config_partition_split_threshold)
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

rocks_table_impl!(Table, TableRocksTable, TableId::Tables, {
    vec![Box::new(TableRocksIndex::Name)]
});

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

    fn version(&self) -> u32 {
        match self {
            TableRocksIndex::Name => 1,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
