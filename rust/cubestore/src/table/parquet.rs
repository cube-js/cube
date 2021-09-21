use crate::metastore::Index;
use crate::CubeError;
use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::{ArrowReader, ArrowWriter, ParquetFileArrowReader};
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::file::reader::SerializedFileReader;
use std::convert::TryFrom;
use std::fs::File;
use std::sync::Arc;

pub struct ParquetTableStore {
    table: Index,
    row_group_size: usize,
}

impl ParquetTableStore {
    pub fn read_columns(&self, file: &str) -> Result<Vec<RecordBatch>, CubeError> {
        let mut r = ParquetFileArrowReader::new(Arc::new(SerializedFileReader::try_from(file)?));
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
    use crate::table::data::{concat_record_batches, rows_to_columns, to_stream};
    use crate::table::parquet::{arrow_schema, ParquetTableStore};
    use crate::table::{Row, TableValue};
    use crate::util::decimal::Decimal;
    use arrow::array::BooleanArray;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

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
        first_rows.sort_by(|a, b| a.sort_key(3).cmp(&b.sort_key(3)));
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
        to_split.sort_by(|a, b| a.sort_key(3).cmp(&b.sort_key(3)));

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
                    ]
                ),
                (
                    75,
                    vec![
                        TableValue::Int(75),
                        TableValue::String(format!("Foo {}", 75)),
                        TableValue::String(format!("Boo {}", 75)),
                    ]
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

        let s = ParquetTableStore::new(index, NUM_ROWS);
        let r = concat_record_batches(&s.read_columns(file).unwrap());
        assert_eq_columns!(r.columns(), &data);
    }
}
