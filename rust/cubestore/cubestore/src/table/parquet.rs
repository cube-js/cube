use crate::config::injection::DIService;
use crate::metastore::table::Table;
use crate::metastore::{IdRow, Index};
use crate::queryplanner::metadata_cache::MetadataCacheFactory;
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::physical_plan::ParquetFileReaderFactory;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::{
    WriterProperties, WriterPropertiesBuilder, WriterVersion,
};
use std::fs::File;
use std::sync::Arc;

pub trait CubestoreParquetMetadataCache: DIService + Send + Sync {
    fn cache(self: &Self) -> Arc<dyn ParquetFileReaderFactory>;
}

#[derive(Debug)]
pub struct CubestoreParquetMetadataCacheImpl {
    cache: Arc<dyn ParquetFileReaderFactory>,
}

crate::di_service!(
    CubestoreParquetMetadataCacheImpl,
    [CubestoreParquetMetadataCache]
);

impl CubestoreParquetMetadataCacheImpl {
    pub fn new(cache: Arc<dyn ParquetFileReaderFactory>) -> Arc<CubestoreParquetMetadataCacheImpl> {
        Arc::new(CubestoreParquetMetadataCacheImpl { cache })
    }
}

impl CubestoreParquetMetadataCache for CubestoreParquetMetadataCacheImpl {
    fn cache(self: &Self) -> Arc<dyn ParquetFileReaderFactory> {
        self.cache.clone()
    }
}

#[async_trait]
pub trait CubestoreMetadataCacheFactory: DIService + Send + Sync {
    // Once we use a Rust that supports trait upcasting as a stable feature, we could make
    // CubestoreMetadataCacheFactory inherit from the MetadataCacheFactory trait and use trait
    // upcasting.
    fn cache_factory(&self) -> &Arc<dyn MetadataCacheFactory>;
    async fn build_writer_props(
        &self,
        _table: &IdRow<Table>,
        builder: WriterPropertiesBuilder,
    ) -> Result<WriterProperties, CubeError> {
        Ok(builder.build())
    }
}

pub struct CubestoreMetadataCacheFactoryImpl {
    metadata_cache_factory: Arc<dyn MetadataCacheFactory>,
}

crate::di_service!(
    CubestoreMetadataCacheFactoryImpl,
    [CubestoreMetadataCacheFactory]
);

impl CubestoreMetadataCacheFactoryImpl {
    pub fn new(
        metadata_cache_factory: Arc<dyn MetadataCacheFactory>,
    ) -> Arc<CubestoreMetadataCacheFactoryImpl> {
        Arc::new(CubestoreMetadataCacheFactoryImpl {
            metadata_cache_factory,
        })
    }
}

impl CubestoreMetadataCacheFactory for CubestoreMetadataCacheFactoryImpl {
    fn cache_factory(&self) -> &Arc<dyn MetadataCacheFactory> {
        &self.metadata_cache_factory
    }
}

pub struct ParquetTableStore {
    table: Index,
    row_group_size: usize,
    metadata_cache_factory: Arc<dyn CubestoreMetadataCacheFactory>,
}

impl ParquetTableStore {
    pub fn read_columns(&self, path: &str) -> Result<Vec<RecordBatch>, CubeError> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::create_new(path)?)?;
        let mut r = builder.with_batch_size(self.row_group_size).build()?;
        let mut batches = Vec::new();
        for b in r {
            batches.push(b?)
        }
        Ok(batches)
    }
}

impl ParquetTableStore {
    pub fn new(
        table: Index,
        row_group_size: usize,
        metadata_cache_factory: Arc<dyn CubestoreMetadataCacheFactory>,
    ) -> ParquetTableStore {
        ParquetTableStore {
            table,
            row_group_size,
            metadata_cache_factory,
        }
    }

    pub fn key_size(&self) -> u64 {
        self.table.sort_key_size()
    }

    pub fn partition_split_key_size(&self) -> u64 {
        self.table
            .partition_split_key_size()
            .unwrap_or(self.key_size())
    }

    pub fn arrow_schema(&self) -> Schema {
        arrow_schema(&self.table)
    }

    pub async fn writer_props(&self, table: &IdRow<Table>) -> Result<WriterProperties, CubeError> {
        self.metadata_cache_factory
            .build_writer_props(
                table,
                WriterProperties::builder()
                    .set_max_row_group_size(self.row_group_size)
                    .set_writer_version(WriterVersion::PARQUET_2_0),
            )
            .await
            .map_err(CubeError::from)
    }

    pub async fn write_data(
        &self,
        dest_file: &str,
        columns: Vec<ArrayRef>,
        table: &IdRow<Table>,
    ) -> Result<(), CubeError> {
        self.write_data_given_props(dest_file, columns, self.writer_props(table).await?)
    }

    pub fn write_data_given_props(
        &self,
        dest_file: &str,
        columns: Vec<ArrayRef>,
        props: WriterProperties,
    ) -> Result<(), CubeError> {
        let schema = Arc::new(arrow_schema(&self.table));
        let batch = RecordBatch::try_new(schema.clone(), columns.to_vec())?;

        let mut w = ArrowWriter::try_new(File::create(dest_file)?, schema, Some(props))?;
        w.write(&batch)?;
        w.close()?;

        Ok(())
    }
}

pub fn arrow_schema(i: &Index) -> Schema {
    Schema::new(i.columns().iter().map(|c| c.into()).collect::<Vec<Field>>())
}

#[cfg(test)]
mod tests {
    use crate::assert_eq_columns;
    use crate::metastore::table::Table;
    use crate::metastore::{Column, ColumnType, IdRow, Index};
    use crate::queryplanner::metadata_cache::BasicMetadataCacheFactory;
    use crate::store::{compaction, ROW_GROUP_SIZE};
    use crate::table::data::{cmp_row_key_heap, concat_record_batches, rows_to_columns, to_stream};
    use crate::table::parquet::{
        arrow_schema, CubestoreMetadataCacheFactoryImpl, ParquetTableStore,
    };
    use crate::table::{Row, TableValue};
    use crate::util::decimal::Decimal;
    use datafusion::arrow::array::{
        ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };
    use datafusion::arrow::datatypes::{Int32Type, Int64Type};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet;
    use datafusion::parquet::data_type::{BoolType, DataType};
    use datafusion::parquet::file::reader::FileReader;
    use datafusion::parquet::file::reader::SerializedFileReader;
    use datafusion::parquet::file::statistics::{Statistics, TypedStatistics};
    use itertools::Itertools;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn column_statistics() {
        let index = Index::try_new(
            "table".to_string(),
            1,
            vec![
                Column::new("str".to_string(), ColumnType::String, 0),
                Column::new("int".to_string(), ColumnType::Int, 1),
                Column::new("time".to_string(), ColumnType::Timestamp, 2),
                Column::new(
                    "decimal".to_string(),
                    ColumnType::Decimal {
                        scale: 4,
                        precision: 5,
                    },
                    3,
                ),
                Column::new("float".to_string(), ColumnType::Float, 4),
                Column::new("bool".to_string(), ColumnType::Boolean, 5),
            ],
            6,
            None,
            None,
            Index::index_type_default(),
        )
        .unwrap();
        let table = dummy_table_row(index.table_id(), index.get_name());

        let dest_file = NamedTempFile::new().unwrap();
        let store = ParquetTableStore::new(
            index,
            ROW_GROUP_SIZE,
            CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
        );

        let data: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                Some("b"),
                None,
                Some("ab"),
                Some("abc"),
            ])),
            Arc::new(Int64Array::from(vec![None, Some(3), Some(1), Some(2)])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(6),
                Some(4),
                None,
                Some(5),
            ])),
            Arc::new(Decimal128Array::from(vec![Some(9), Some(7), Some(8), None])),
            Arc::new(Float64Array::from(vec![
                Some(3.3),
                None,
                Some(1.1),
                Some(2.2),
            ])),
            Arc::new(BooleanArray::from(vec![
                None,
                Some(true),
                Some(false),
                Some(true),
            ])),
        ];
        // TODO: check floats use total_cmp.

        store
            .write_data(dest_file.path().to_str().unwrap(), data, &table)
            .await
            .unwrap();

        let r = SerializedFileReader::new(dest_file.into_file()).unwrap();

        assert_eq!(r.num_row_groups(), 1);
        let metadata = r.metadata();
        let columns = metadata.row_group(0).columns();
        let columns = columns
            .iter()
            .map(|c| print_min_max(c.statistics()))
            .join("\n");

        assert_eq!(
            columns,
            // strings shown as byte arrays. 97, 98, 99 are codes for 'a', 'b', 'c'.
            "min: [97, 98], max: [98]\
           \nmin: 1, max: 3\
           \nmin: 4, max: 6\
           \nmin: 7, max: 9\
           \nmin: 1.1, max: 3.3\
           \nmin: false, max: true"
        );
    }

    fn dummy_table_row(table_id: u64, table_name: &str) -> IdRow<Table> {
        IdRow::<Table>::new(
            table_id,
            Table::new(
                table_name.to_string(),
                table_id,
                vec![],
                None,
                None,
                true,
                None,
                None,
                None,
                None,
                None,
                None,
                vec![],
                None,
                None,
                None,
            ),
        )
    }

    #[tokio::test]
    async fn gutter() {
        let store = ParquetTableStore {
            table: Index::try_new(
                "foo".to_string(),
                1,
                vec![
                    Column::new("foo_int".to_string(), ColumnType::Int, 0),
                    Column::new("foo".to_string(), ColumnType::String, 1),
                    Column::new("boo".to_string(), ColumnType::String, 2),
                    Column::new("bool".to_string(), ColumnType::Boolean, 3),
                    Column::new(
                        "dec".to_string(),
                        ColumnType::Decimal {
                            scale: 5,
                            precision: 18,
                        },
                        4,
                    ),
                ],
                3,
                None,
                None,
                Index::index_type_default(),
            )
            .unwrap(),
            row_group_size: 10,
            metadata_cache_factory: CubestoreMetadataCacheFactoryImpl::new(Arc::new(
                BasicMetadataCacheFactory::new(),
            )),
        };
        let table = dummy_table_row(store.table.table_id(), store.table.get_name());
        let file = NamedTempFile::new().unwrap();
        let file_name = file.path().to_str().unwrap();

        let mut first_rows = (0..40)
            .map(|i| {
                Row::new(vec![
                    if i % 5 != 0 {
                        TableValue::Int(i % 20)
                    } else {
                        TableValue::Null
                    },
                    TableValue::String(format!("Foo {}", i)),
                    if i % 7 == 0 {
                        TableValue::Null
                    } else {
                        TableValue::String(format!("Boo {}", i))
                    },
                    TableValue::Boolean(i % 5 == 0),
                    if i % 5 != 0 {
                        TableValue::Decimal(Decimal::new((i * 10000) as i128))
                    } else {
                        TableValue::Null
                    },
                ])
            })
            .collect::<Vec<_>>();
        first_rows.sort_by(|a, b| cmp_row_key_heap(3, &a.values(), &b.values()));
        let first_cols = rows_to_columns(&store.table.columns(), &first_rows);
        store
            .write_data(file_name, first_cols.clone(), &table)
            .await
            .unwrap();

        let read_rows = concat_record_batches(&store.read_columns(file_name).unwrap());
        assert_eq_columns!(&first_cols, read_rows.columns());

        // Split
        let split_1 = NamedTempFile::new().unwrap();
        let split_1 = split_1.path().to_str().unwrap();

        let split_2 = NamedTempFile::new().unwrap();
        let split_2 = split_2.path().to_str().unwrap();

        let mut to_split = first_rows;
        for i in 40..150 {
            to_split.push(Row::new(vec![
                TableValue::Int(i),
                TableValue::String(format!("Foo {}", i)),
                TableValue::String(format!("Boo {}", i)),
                TableValue::Boolean(false),
                TableValue::Decimal(Decimal::new((i * 10000) as i128)),
            ]));
        }
        to_split.sort_by(|a, b| cmp_row_key_heap(3, &a.values(), &b.values()));

        let to_split_cols = rows_to_columns(&store.table.columns(), &to_split);
        let schema = Arc::new(arrow_schema(&store.table));
        let to_split_batch = RecordBatch::try_new(schema.clone(), to_split_cols.clone()).unwrap();
        let count_min = compaction::write_to_files(
            to_stream(to_split_batch),
            to_split.len(),
            ParquetTableStore::new(
                store.table.clone(),
                store.row_group_size,
                CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
            ),
            &dummy_table_row(store.table.table_id(), store.table.get_name()),
            vec![split_1.to_string(), split_2.to_string()],
        )
        .await
        .unwrap();

        let read_1 = concat_record_batches(&store.read_columns(split_1).unwrap());
        let read_2 = concat_record_batches(&store.read_columns(split_2).unwrap());
        assert_eq!(read_1.num_rows() + read_2.num_rows(), to_split.len());
        let read = concat_record_batches(&[read_1, read_2]);

        assert_eq_columns!(read.columns(), &to_split_cols);

        assert_eq!(
            count_min,
            vec![
                (
                    75,
                    vec![
                        TableValue::Null,
                        TableValue::String(format!("Foo {}", 0)),
                        TableValue::Null,
                    ],
                    vec![
                        TableValue::Int(74),
                        TableValue::String(format!("Foo {}", 74)),
                        TableValue::String(format!("Boo {}", 74)),
                    ]
                ),
                (
                    75,
                    vec![
                        TableValue::Int(75),
                        TableValue::String(format!("Foo {}", 75)),
                        TableValue::String(format!("Boo {}", 75)),
                    ],
                    vec![
                        TableValue::Int(149),
                        TableValue::String(format!("Foo {}", 149)),
                        TableValue::String(format!("Boo {}", 149)),
                    ],
                )
            ]
        );
    }

    #[tokio::test]
    async fn failed_rle_run_bools() {
        const NUM_ROWS: usize = ROW_GROUP_SIZE;

        let check_bools = async |bools: Vec<bool>| {
            let index = Index::try_new(
                "test".to_string(),
                0,
                vec![Column::new("b".to_string(), ColumnType::Boolean, 0)],
                1,
                None,
                None,
                Index::index_type_default(),
            )
            .unwrap();
            let table = dummy_table_row(index.table_id(), index.get_name());
            let tmp_file = NamedTempFile::new().unwrap();
            let store = ParquetTableStore::new(
                index.clone(),
                NUM_ROWS,
                CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
            );
            store
                .write_data(
                    tmp_file.path().to_str().unwrap(),
                    vec![Arc::new(BooleanArray::from(bools))],
                    &table,
                )
                .await
                .unwrap();
        };

        // Maximize the data to write with RLE encoding.
        // First, in bit-packed encoding.
        let mut bools = Vec::with_capacity(NUM_ROWS);
        for _ in 0..NUM_ROWS / 2 {
            bools.push(true);
            bools.push(false);
        }
        check_bools(bools).await;

        // Second, in RLE encoding.
        let mut bools = Vec::with_capacity(NUM_ROWS);
        for _ in 0..NUM_ROWS / 16 {
            for _ in 0..8 {
                bools.push(true);
            }
            for _ in 0..8 {
                bools.push(false);
            }
        }
        check_bools(bools).await;
    }

    #[tokio::test]
    async fn read_bytes() {
        const NUM_ROWS: usize = 8;
        let index = Index::try_new(
            "index".into(),
            0,
            vec![
                Column::new("id".into(), ColumnType::Int, 0),
                Column::new("bytes".into(), ColumnType::Bytes, 1),
            ],
            1,
            None,
            None,
            Index::index_type_default(),
        )
        .unwrap();
        let table = dummy_table_row(index.table_id(), index.get_name());

        let file = NamedTempFile::new().unwrap();
        let file = file.path().to_str().unwrap();
        let rows = vec![
            Row::new(vec![TableValue::Int(1), TableValue::Bytes(vec![1, 2, 3])]),
            Row::new(vec![TableValue::Int(2), TableValue::Bytes(vec![5, 6, 7])]),
        ];

        let data = rows_to_columns(&index.columns(), &rows);

        let w = ParquetTableStore::new(
            index.clone(),
            NUM_ROWS,
            CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
        );
        w.write_data(file, data.clone(), &table).await.unwrap();
        let r = concat_record_batches(&w.read_columns(file).unwrap());
        assert_eq_columns!(r.columns(), &data);
    }

    fn print_min_max_typed<T: DataType>(s: &TypedStatistics<T>) -> String {
        format!(
            "min: {}, max: {}",
            s.min_opt()
                .map(|v| v.to_string())
                .unwrap_or("NULL".to_string()),
            s.max_opt()
                .map(|v| v.to_string())
                .unwrap_or("NULL".to_string())
        )
    }

    fn print_min_max(s: Option<&Statistics>) -> String {
        let s = match s {
            Some(s) => s,
            None => return "<null>".to_string(),
        };
        match s {
            Statistics::Boolean(t) => print_min_max_typed::<parquet::data_type::BoolType>(t),
            Statistics::Int32(t) => print_min_max_typed::<parquet::data_type::Int32Type>(t),
            Statistics::Int64(t) => print_min_max_typed::<parquet::data_type::Int64Type>(t),
            Statistics::Int96(t) => print_min_max_typed::<parquet::data_type::Int96Type>(t),
            Statistics::Float(t) => print_min_max_typed::<parquet::data_type::FloatType>(t),
            Statistics::Double(t) => print_min_max_typed::<parquet::data_type::DoubleType>(t),
            Statistics::ByteArray(t) => print_min_max_typed::<parquet::data_type::ByteArrayType>(t),
            Statistics::FixedLenByteArray(t) => {
                print_min_max_typed::<parquet::data_type::FixedLenByteArrayType>(t)
            }
        }
    }
}
