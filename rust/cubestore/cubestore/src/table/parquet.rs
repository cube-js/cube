use crate::config::injection::DIService;
use crate::metastore::Index;
use crate::CubeError;
use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::parquet::{NoopParquetMetadataCache, ParquetMetadataCache};
use parquet::arrow::{ArrowReader, ArrowWriter, ParquetFileArrowReader};
use parquet::file::properties::{WriterProperties, WriterVersion};
use std::fs::File;
use std::sync::Arc;

pub trait CubestoreParquetMetadataCache: DIService + Send + Sync {
    fn cache(self: &Self) -> Arc<dyn ParquetMetadataCache>;
}

#[derive(Debug)]
pub struct CubestoreParquetMetadataCacheImpl {
    cache: Arc<dyn ParquetMetadataCache>,
}

crate::di_service!(
    CubestoreParquetMetadataCacheImpl,
    [CubestoreParquetMetadataCache]
);

impl CubestoreParquetMetadataCacheImpl {
    pub fn new(cache: Arc<dyn ParquetMetadataCache>) -> Arc<CubestoreParquetMetadataCacheImpl> {
        Arc::new(CubestoreParquetMetadataCacheImpl { cache })
    }
}

impl CubestoreParquetMetadataCache for CubestoreParquetMetadataCacheImpl {
    fn cache(self: &Self) -> Arc<dyn ParquetMetadataCache> {
        self.cache.clone()
    }
}

pub struct ParquetTableStore {
    table: Index,
    row_group_size: usize,
}

impl ParquetTableStore {
    pub fn read_columns(&self, path: &str) -> Result<Vec<RecordBatch>, CubeError> {
        let mut r = ParquetFileArrowReader::new(Arc::new(
            NoopParquetMetadataCache::new().file_reader(path)?,
        ));
        let mut batches = Vec::new();
        for b in r.get_record_reader(self.row_group_size)? {
            batches.push(b?)
        }
        Ok(batches)
    }
}

impl ParquetTableStore {
    pub fn new(table: Index, row_group_size: usize) -> ParquetTableStore {
        ParquetTableStore {
            table,
            row_group_size,
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

    pub fn writer_props(&self) -> WriterProperties {
        WriterProperties::builder()
            .set_max_row_group_size(self.row_group_size)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .build()
    }

    pub fn write_data(&self, dest_file: &str, columns: Vec<ArrayRef>) -> Result<(), CubeError> {
        let schema = Arc::new(arrow_schema(&self.table));
        let batch = RecordBatch::try_new(schema.clone(), columns.to_vec())?;

        let mut w =
            ArrowWriter::try_new(File::create(dest_file)?, schema, Some(self.writer_props()))?;
        w.write(&batch)?;
        w.close()?;

        Ok(())
    }
}

pub fn arrow_schema(i: &Index) -> Schema {
    Schema::new(i.columns().iter().map(|c| c.into()).collect())
}

#[cfg(test)]
mod tests {
    extern crate test;

    use crate::assert_eq_columns;
    use crate::metastore::{Column, ColumnType, Index};
    use crate::store::{compaction, ROW_GROUP_SIZE};
    use crate::table::data::{cmp_row_key_heap, concat_record_batches, rows_to_columns, to_stream};
    use crate::table::parquet::{arrow_schema, ParquetTableStore};
    use crate::table::{Row, TableValue};
    use crate::util::decimal::Decimal;
    use arrow::array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, Int64Decimal4Array, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow::record_batch::RecordBatch;
    use itertools::Itertools;
    use parquet::data_type::DataType;
    use parquet::file::reader::FileReader;
    use parquet::file::reader::SerializedFileReader;
    use parquet::file::statistics::{Statistics, TypedStatistics};
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    #[test]
    fn column_statistics() {
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

        let dest_file = NamedTempFile::new().unwrap();
        let store = ParquetTableStore::new(index, ROW_GROUP_SIZE);

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
            Arc::new(Int64Decimal4Array::from(vec![
                Some(9),
                Some(7),
                Some(8),
                None,
            ])),
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
            .write_data(dest_file.path().to_str().unwrap(), data)
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
        };
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
                        TableValue::Decimal(Decimal::new(i * 10000))
                    } else {
                        TableValue::Null
                    },
                ])
            })
            .collect::<Vec<_>>();
        first_rows.sort_by(|a, b| cmp_row_key_heap(3, &a.values(), &b.values()));
        let first_cols = rows_to_columns(&store.table.columns(), &first_rows);
        store.write_data(file_name, first_cols.clone()).unwrap();

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
                TableValue::Decimal(Decimal::new(i * 10000)),
            ]));
        }
        to_split.sort_by(|a, b| cmp_row_key_heap(3, &a.values(), &b.values()));

        let to_split_cols = rows_to_columns(&store.table.columns(), &to_split);
        let schema = Arc::new(arrow_schema(&store.table));
        let to_split_batch = RecordBatch::try_new(schema.clone(), to_split_cols.clone()).unwrap();
        let count_min = compaction::write_to_files(
            to_stream(to_split_batch).await,
            to_split.len(),
            ParquetTableStore::new(store.table.clone(), store.row_group_size),
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

    #[test]
    fn failed_rle_run_bools() {
        const NUM_ROWS: usize = ROW_GROUP_SIZE;

        let check_bools = |bools: Vec<bool>| {
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
            let tmp_file = NamedTempFile::new().unwrap();
            let store = ParquetTableStore::new(index.clone(), NUM_ROWS);
            store
                .write_data(
                    tmp_file.path().to_str().unwrap(),
                    vec![Arc::new(BooleanArray::from(bools))],
                )
                .unwrap();
        };

        // Maximize the data to write with RLE encoding.
        // First, in bit-packed encoding.
        let mut bools = Vec::with_capacity(NUM_ROWS);
        for _ in 0..NUM_ROWS / 2 {
            bools.push(true);
            bools.push(false);
        }
        check_bools(bools);

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
        check_bools(bools);
    }

    #[test]
    fn read_bytes() {
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

        let file = NamedTempFile::new().unwrap();
        let file = file.path().to_str().unwrap();
        let rows = vec![
            Row::new(vec![TableValue::Int(1), TableValue::Bytes(vec![1, 2, 3])]),
            Row::new(vec![TableValue::Int(2), TableValue::Bytes(vec![5, 6, 7])]),
        ];

        let data = rows_to_columns(&index.columns(), &rows);

        let w = ParquetTableStore::new(index.clone(), NUM_ROWS);
        w.write_data(file, data.clone()).unwrap();
        let r = concat_record_batches(&w.read_columns(file).unwrap());
        assert_eq_columns!(r.columns(), &data);
    }

    fn print_min_max_typed<T: DataType>(s: &TypedStatistics<T>) -> String {
        format!("min: {}, max: {}", s.min(), s.max())
    }

    fn print_min_max(s: Option<&Statistics>) -> String {
        let s = match s {
            Some(s) => s,
            None => return "<null>".to_string(),
        };
        match s {
            Statistics::Boolean(t) => print_min_max_typed(t),
            Statistics::Int32(t) => print_min_max_typed(t),
            Statistics::Int64(t) => print_min_max_typed(t),
            Statistics::Int96(t) => print_min_max_typed(t),
            Statistics::Float(t) => print_min_max_typed(t),
            Statistics::Double(t) => print_min_max_typed(t),
            Statistics::ByteArray(t) => print_min_max_typed(t),
            Statistics::FixedLenByteArray(t) => print_min_max_typed(t),
        }
    }
}
