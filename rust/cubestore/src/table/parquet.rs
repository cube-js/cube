use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::parquet::ParquetExec;
use crate::table::{TableStore, Row, TableValue};
use crate::CubeError;
use std::fs::File;
use parquet::file::reader::{SerializedFileReader, FileReader};
use parquet::data_type::*;
use parquet::column::reader::ColumnReader;
use crate::metastore::{ColumnType, Column, Index};
use std::rc::Rc;
use parquet::schema::types;
use parquet::file::writer::{SerializedFileWriter, FileWriter};
use std::cmp::{min, max};
use parquet::column::writer::ColumnWriter;
use parquet::file::properties::WriterProperties;
use super::TimestampValue;

use std::sync::Arc;
use parquet::file::metadata::RowGroupMetaData;
use num::integer::div_ceil;

pub struct ParquetTableStore {
    table: Index,
    row_group_size: usize,
}

pub struct RowParquetWriter {
    parquet_writer: SerializedFileWriter<File>,
    buffer: Vec<Row>,
    row_group_size: usize,
    sort_key_size: u64,
}

enum ColumnAccessor {
    Bytes(Vec<ByteArray>),
    Int(Vec<i64>),
    Boolean(Vec<bool>),
    // Decimal(Vec<i64>),
}

pub struct RowParquetReader<'a> {
    pub parquet_reader: SerializedFileReader<File>,
    column_with_buffer: Vec<(&'a Column, usize, ColumnAccessor, Option<Vec<i16>>)>,
}

impl TableStore for ParquetTableStore {
    fn merge_rows<'a>(&'a self, source_file: Option<&'a str>, dest_files: Vec<String>, rows: Vec<Row>, sort_key_size: u64) -> Result<Vec<(u64, (Row, Row))>, CubeError> {
        let mut writers = Vec::new();
        for f in dest_files.iter() {
            writers.push(RowParquetWriter::open(&self.table, f, self.row_group_size, sort_key_size)?);
        }
        if source_file.is_none() {
            let mut split_writer = SplitRowParquetWriter::new(writers, rows.len());
            split_writer.write_rows(rows.as_slice())?;
            return Ok(split_writer.close()?);
        }

        let mut reader = RowParquetReader::open(&self.table, source_file.unwrap(), None)?;
        let mut right_position = 0;
        let total_row_number = reader.parquet_reader.metadata().file_metadata().num_rows() as usize + rows.len();
        let mut split_writer = SplitRowParquetWriter::new(writers, total_row_number);

        for row_group_index in 0..reader.parquet_reader.num_row_groups() {
            let read_rows = reader.read_rows(row_group_index)?;
            let (new_pos, to_write) = ParquetTableStore::merge_sort(
                read_rows,
                &rows,
                right_position,
                sort_key_size,
            );
            split_writer.write_rows(to_write.as_slice())?;
            right_position = new_pos;
        }

        if right_position < rows.len() {
            split_writer.write_rows(&rows[right_position..rows.len()])?;
        }

        Ok(split_writer.close()?)
    }

    fn read_rows(&self, file: &str) -> Result<Vec<Row>, CubeError> {
        let mut result = Vec::<Row>::new();
        let mut reader = RowParquetReader::open(&self.table, file, None)?;
        for row_group_index in 0..reader.parquet_reader.num_row_groups() {
            let mut rows = reader.read_rows(row_group_index)?;
            result.append(&mut rows);
        }
        Ok(result)
    }

    fn read_filtered_rows(&self, file: &str, columns: &Vec<Column>, limit: usize) -> Result<Vec<Row>, CubeError> {
        let mut result = Vec::<Row>::new();
        let mut reader = RowParquetReader::open(&self.table, file, Some(columns))?;
        'outer: for row_group_index in 0..reader.parquet_reader.num_row_groups() {
            let row_group = reader.read_rows(row_group_index)?;
            for row in &row_group {
                if result.len() >= limit { break 'outer; }
                result.push(row.clone());
            }
        }
        Ok(result)
    }

    fn scan_node(&self, file: &str, columns: &Vec<Column>, row_group_filter: Option<Arc<dyn Fn(&RowGroupMetaData) -> bool + Send + Sync>>) -> Result<Arc<dyn ExecutionPlan + Send + Sync>, CubeError> {
        Ok(Arc::new(ParquetExec::try_new_with_filter(
            file,
            Some(columns.iter().map(|c| c.get_index()).collect::<Vec<_>>()),
            self.row_group_size,
            row_group_filter,
        )?))
    }
}

impl ParquetTableStore {
    pub fn new(table: Index, row_group_size: usize) -> ParquetTableStore {
        ParquetTableStore {
            table,
            row_group_size,
        }
    }

    fn merge_sort(left: Vec<Row>, right: &Vec<Row>, initial_right_pos: usize, sort_key_size: u64) -> (usize, Vec<Row>) {
        if right.len() == initial_right_pos || left[left.len() - 1].sort_key(sort_key_size) <= right[initial_right_pos].sort_key(sort_key_size) {
            return (initial_right_pos, left);
        }
        let mut result = Vec::with_capacity(left.len());
        let mut left_position = 0;
        let mut right_position = initial_right_pos;
        while left_position < left.len() {
            let left_key = Some(left[left_position].sort_key(sort_key_size));
            let right_key = if right_position < right.len() {
                Some(right[right_position].sort_key(sort_key_size))
            } else {
                None
            };
            let option = left_key.as_ref().zip(right_key.as_ref());
            if right_key.is_none() || option.map(|(l, r)| l <= r).unwrap_or(false) {
                result.push(left[left_position].clone()); // TODO clone
                left_position += 1;
            } else if option.map(|(l, r)| l > r).unwrap_or(false) {
                result.push(right[right_position].clone()); // TODO clone
                right_position += 1;
            } else {
                panic!("Shouldn't get here");
            }
        }
        (right_position, result)
    }
}

impl<'a> RowParquetReader<'a> {
    fn open(table: &'a Index, file: &'a str, columns_to_read: Option<&'a Vec<Column>>) -> Result<RowParquetReader<'a>, CubeError> {
        let file = File::open(file)?;
        let parquet_reader = SerializedFileReader::new(file)?;

        let column_with_buffer = columns_to_read.unwrap_or(table.get_columns()).iter()
            .map(|c| (
                c,
                c.get_index(),
                match c.get_column_type() {
                    ColumnType::String => ColumnAccessor::Bytes(vec![ByteArray::new(); 16384]),
                    ColumnType::Bytes => ColumnAccessor::Bytes(vec![ByteArray::new(); 16384]),
                    ColumnType::Int => ColumnAccessor::Int(vec![0; 16384]),
                    ColumnType::Timestamp => ColumnAccessor::Int(vec![0; 16384]),
                    ColumnType::Boolean => ColumnAccessor::Boolean(vec![false; 16384]),
                    x => panic!("Column type is not supported: {:?}", x)
                },
                Some(vec![0; 16384])
            )).collect::<Vec<_>>();

        Ok(RowParquetReader {
            parquet_reader,
            column_with_buffer,
        })
    }

    fn load_row_group(&mut self, row_group_index: usize) -> Result<usize, CubeError> {
        let row_group = self.parquet_reader.get_row_group(row_group_index)?;
        let mut values_read = 0;
        for (_, index, column_accessor, def_levels) in &mut self.column_with_buffer {
            let mut col_reader = row_group.get_column_reader(*index).unwrap();
            match column_accessor {
                ColumnAccessor::Bytes(buffer) => {
                    if let ColumnReader::ByteArrayColumnReader(ref mut reader) = col_reader {
                        values_read = max(values_read, reader.read_batch(buffer.len(), def_levels.as_mut().map(|l| l.as_mut_slice()), None, buffer.as_mut_slice())?.1);
                    }
                }
                ColumnAccessor::Int(buffer) => {
                    if let ColumnReader::Int64ColumnReader(ref mut reader) = col_reader {
                        values_read = max(values_read, reader.read_batch(buffer.len(), def_levels.as_mut().map(|l| l.as_mut_slice()), None, buffer.as_mut_slice())?.1);
                    }
                }
                ColumnAccessor::Boolean(buffer) => {
                    if let ColumnReader::BoolColumnReader(ref mut reader) = col_reader {
                        values_read = max(values_read, reader.read_batch(buffer.len(), def_levels.as_mut().map(|l| l.as_mut_slice()), None, buffer.as_mut_slice())?.1);
                    }
                }
            };
        }
        Ok(values_read)
    }

    fn read_rows(&mut self, row_group_index: usize) -> Result<Vec<Row>, CubeError> {
        let values_read = self.load_row_group(row_group_index)?;
        let mut vec_result = Vec::<Row>::with_capacity(values_read);
        for _ in 0..values_read {
            vec_result.push(Row::new(Vec::with_capacity(self.column_with_buffer.len())))
        }
        for (col, _, column_accessor, def_levels) in &self.column_with_buffer {
            let mut cur_value_index = 0;
            match def_levels {
                Some(levels) => {
                    match col.get_column_type() {
                        ColumnType::String => {
                            if let ColumnAccessor::Bytes(buffer) = &column_accessor {
                                for i in 0..values_read {
                                    if levels[i] == 1 {
                                        let value = buffer[cur_value_index].as_utf8()?;
                                        vec_result[i].push(TableValue::String(value.to_string()));
                                        cur_value_index += 1;
                                    } else {
                                        vec_result[i].push(TableValue::Null);
                                    }
                                }
                            }
                        }
                        ColumnType::Bytes => {
                            if let ColumnAccessor::Bytes(buffer) = &column_accessor {
                                for i in 0..values_read {
                                    if levels[i] == 1 {
                                        let value = buffer[cur_value_index].as_bytes();
                                        vec_result[i].push(TableValue::Bytes(value.to_vec()));
                                        cur_value_index += 1;
                                    } else {
                                        vec_result[i].push(TableValue::Null);
                                    }
                                }
                            }
                        }
                        ColumnType::Int => {
                            if let ColumnAccessor::Int(buffer) = &column_accessor {
                                for i in 0..values_read {
                                    if levels[i] == 1 {
                                        let value = buffer[cur_value_index];
                                        vec_result[i].push(TableValue::Int(value));
                                        cur_value_index += 1;
                                    } else {
                                        vec_result[i].push(TableValue::Null);
                                    }
                                }
                            }
                        }
                        ColumnType::Timestamp => {
                            if let ColumnAccessor::Int(buffer) = &column_accessor {
                                for i in 0..values_read {
                                    if levels[i] == 1 {
                                        let value = buffer[cur_value_index];
                                        vec_result[i].push(TableValue::Timestamp(TimestampValue::new(value * 1000 as i64)));
                                        cur_value_index += 1;
                                    } else {
                                        vec_result[i].push(TableValue::Null);
                                    }
                                }
                            }
                        }
                        ColumnType::Boolean => {
                            if let ColumnAccessor::Boolean(buffer) = &column_accessor {
                                for i in 0..values_read {
                                    if levels[i] == 1 {
                                        let value = buffer[cur_value_index];
                                        vec_result[i].push(TableValue::Boolean(value));
                                        cur_value_index += 1;
                                    } else {
                                        vec_result[i].push(TableValue::Null);
                                    }
                                }
                            }
                        }
                        x => panic!("Unsupported value: {:?}", x)
                    };
                }
                x => panic!("Unsupported value: {:?}", x)
            }
        }
        Ok(vec_result)
    }
}

pub struct SplitRowParquetWriter {
    writers: Vec<RowParquetWriter>,
    current_writer: usize,
    rows_written: usize,
    rows_written_current_file: u64,
    chunk_size: usize,
    min_max_rows: Vec<(u64, (Row, Row))>,
    first_row: Option<Row>,
    last_row: Option<Row>,
}

impl SplitRowParquetWriter {
    pub fn new(writers: Vec<RowParquetWriter>, total_row_number: usize) -> SplitRowParquetWriter {
        let chunk_size = div_ceil(total_row_number, writers.len());
        SplitRowParquetWriter {
            writers,
            current_writer: 0,
            rows_written: 0,
            rows_written_current_file: 0,
            chunk_size,
            min_max_rows: Vec::new(),
            first_row: None,
            last_row: None,
        }
    }

    fn write_rows(&mut self, rows: &[Row]) -> Result<(), CubeError> {
        if rows.len() == 0 {
            return Ok(());
        }
        if self.first_row.is_none() {
            self.first_row = Some(rows[0].clone());
        }
        let mut remaining_slice = rows;
        while remaining_slice.len() + self.rows_written > (self.current_writer + 1) * self.chunk_size {
            let split_at = (self.current_writer + 1) * self.chunk_size - self.rows_written;
            self.writers[self.current_writer].write_rows(&remaining_slice[0..split_at])?;
            self.rows_written += split_at;
            self.rows_written_current_file += split_at as u64;
            self.min_max_rows.push((self.rows_written_current_file, (self.first_row.as_ref().unwrap_or(&remaining_slice[0]).clone(), remaining_slice[split_at - 1].clone())));
            self.rows_written_current_file = 0;
            self.first_row = None;
            self.current_writer += 1;
            remaining_slice = &remaining_slice[split_at..];
        }
        if self.first_row.is_none() {
            self.first_row = Some(remaining_slice[0].clone());
        }
        self.writers[self.current_writer].write_rows(remaining_slice)?;
        self.last_row = Some(remaining_slice[remaining_slice.len() - 1].clone());
        self.rows_written += remaining_slice.len();
        self.rows_written_current_file += remaining_slice.len() as u64;
        Ok(())
    }

    fn close(mut self) -> Result<Vec<(u64, (Row, Row))>, CubeError> {
        if self.first_row.is_some() && self.last_row.is_some() {
            self.min_max_rows.push((self.rows_written_current_file, (self.first_row.as_ref().unwrap().clone(), self.last_row.as_ref().unwrap().clone())));
            self.rows_written_current_file = 0;
        }
        for w in self.writers.into_iter() {
            w.close()?;
        }
        Ok(self.min_max_rows)
    }
}

impl RowParquetWriter {
    fn open(table: &'a Index, file: &'a str, row_group_size: usize, sort_key_size: u64) -> Result<RowParquetWriter, CubeError> {
        let file = File::create(file)?;

        let mut fields = table.get_columns().iter().map(|column| {
            // TODO pass nullable columns
            Rc::new(parquet::schema::types::Type::from(column))
        }
        ).collect();

        let schema = Rc::new(
            types::Type::group_type_builder("schema")
                .with_fields(&mut fields)
                .build().unwrap(),
        );

        let props = Self::writer_props();
        let parquet_writer =
            SerializedFileWriter::new(file.try_clone()?, schema, props)?;

        Ok(RowParquetWriter {
            parquet_writer,
            row_group_size,
            buffer: Vec::with_capacity(row_group_size as usize),
            sort_key_size,
        })
    }

    fn write_rows(&mut self, rows: &[Row]) -> Result<(), CubeError> {
        self.buffer.extend_from_slice(rows); // TODO optimize

        if self.buffer.len() >= self.row_group_size {
            self.write_buffer()?;
        }

        Ok(())
    }

    fn write_buffer(&mut self) -> Result<(), CubeError> {
        let batch_size = self.row_group_size;
        let row_group_count = self.buffer.len() / self.row_group_size;
        for row_batch_index in 0..max(row_group_count, 1) {
            let mut row_group_writer = self.parquet_writer.next_row_group()?;

            let mut column_index = 0;

            let rows_in_group = min(self.row_group_size, self.buffer.len());

            while let Some(mut col_writer) = row_group_writer.next_column()? {
                // TODO types
                match col_writer {
                    ColumnWriter::Int64ColumnWriter(ref mut typed) => {
                        let column_values = (0..rows_in_group).filter(|row_index| &self.buffer[row_batch_index * batch_size + row_index].values[column_index] != &TableValue::Null).map(
                            |row_index| {
                                // TODO types
                                match &self.buffer[row_batch_index * batch_size + row_index].values[column_index] {
                                    TableValue::Int(val) => i64::from(val.clone()),
                                    TableValue::Timestamp(t) => i64::from(t.clone().get_time_stamp() / 1000),
                                    x => panic!("Unsupported value: {:?}", x)
                                }
                            }
                        ).collect::<Vec<i64>>();
                        let min = if self.sort_key_size >= column_index as u64 && column_values.len() > 0 {
                            Some(column_values[0].clone())
                        } else {
                            None
                        };
                        let max = if self.sort_key_size >= column_index as u64 && column_values.len() > 0 {
                            Some(column_values[column_values.len() - 1].clone())
                        } else {
                            None
                        };
                        let def_levels = self.get_def_levels(batch_size, row_batch_index, column_index, rows_in_group, column_values.len());
                        typed.write_batch_with_statistics(&column_values, def_levels.as_ref().map(|b| b.as_slice()), None, &min, &max, None, None)?;
                    }
                    ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
                        let column_values = (0..rows_in_group).filter(|row_index| &self.buffer[row_batch_index * batch_size + row_index].values[column_index] != &TableValue::Null).map(
                            |row_index| {
                                // TODO types
                                match &self.buffer[row_batch_index * batch_size + row_index].values[column_index] {
                                    TableValue::String(str) => ByteArray::from(str.as_str()),
                                    TableValue::Bytes(bytes) => ByteArray::from(bytes.clone()),
                                    x => panic!("Unsupported value: {:?}", x)
                                }
                            }
                        ).collect::<Vec<ByteArray>>();
                        let min = if self.sort_key_size >= column_index as u64 && column_values.len() > 0 {
                            Some(column_values[0].clone())
                        } else {
                            None
                        };
                        let max = if self.sort_key_size >= column_index as u64 && column_values.len() > 0 {
                            Some(column_values[column_values.len() - 1].clone())
                        } else {
                            None
                        };
                        let def_levels = self.get_def_levels(batch_size, row_batch_index, column_index, rows_in_group, column_values.len());
                        typed.write_batch_with_statistics(&column_values, def_levels.as_ref().map(|b| b.as_slice()), None, &min, &max, None, None)?;
                    }
                    ColumnWriter::BoolColumnWriter(ref mut typed) => {
                        let column_values = (0..rows_in_group).filter(|row_index| &self.buffer[row_batch_index * batch_size + row_index].values[column_index] != &TableValue::Null).map(
                            |row_index| {
                                // TODO types
                                match &self.buffer[row_batch_index * batch_size + row_index].values[column_index] {
                                    TableValue::Boolean(b) => *b,
                                    x => panic!("Unsupported value: {:?}", x)
                                }
                            }
                        ).collect::<Vec<bool>>();
                        let min = if self.sort_key_size >= column_index as u64 && column_values.len() > 0 {
                            Some(column_values[0].clone())
                        } else {
                            None
                        };
                        let max = if self.sort_key_size >= column_index as u64 && column_values.len() > 0 {
                            Some(column_values[column_values.len() - 1].clone())
                        } else {
                            None
                        };
                        let def_levels = self.get_def_levels(batch_size, row_batch_index, column_index, rows_in_group, column_values.len());
                        typed.write_batch_with_statistics(&column_values, def_levels.as_ref().map(|b| b.as_slice()), None, &min, &max, None, None)?;
                    }
                    _ => panic!("Unsupported writer")
                };

                row_group_writer.close_column(col_writer)?;
                column_index += 1;
            }

            self.parquet_writer.close_row_group(row_group_writer)?;
        }

        let target_size = self.buffer.len() - row_group_count * self.row_group_size;
        for i in (0..target_size).rev() {
            self.buffer.swap_remove(i);
        }
        self.buffer.truncate(target_size);
        Ok(())
    }

    fn get_def_levels(&self, batch_size: usize, row_batch_index: usize, column_index: usize, rows_in_group: usize, _column_values_len: usize) -> Option<Vec<i16>> {
        Some((0..rows_in_group).map(
            |row_index| {
                // TODO types
                match &self.buffer[row_batch_index * batch_size + row_index].values[column_index] {
                    TableValue::Null => 0,
                    _ => 1
                }
            }
        ).collect::<Vec<i16>>())
    }

    fn close(mut self) -> Result<(), CubeError> {
        self.write_buffer()?;
        self.parquet_writer.close()?;
        Ok(())
    }

    fn writer_props() -> Rc<WriterProperties> {
        Rc::new(
            WriterProperties::builder()
                // .set_key_value_metadata(Some(vec![KeyValue::new(
                //     "key".to_string(),
                //     "value".to_string(),
                // )]))
                .set_statistics_enabled(true)
                // .set_column_dictionary_enabled(ColumnPath::new(vec!["col0".to_string()]), true)
                // .set_column_encoding(ColumnPath::new(vec!["col1".to_string()]), Encoding::RLE)
                .build(),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::table::parquet::{ParquetTableStore, RowParquetReader, ColumnAccessor};
    use crate::metastore::{Index, Column, ColumnType};
    use crate::table::{TableStore, Row, TableValue};
    use std::{fs, io};

    extern crate test;

    use test::Bencher;
    use csv::ReaderBuilder;
    use std::fs::File;
    use std::mem::swap;
    use parquet::file::reader::FileReader;
    use std::io::BufReader;
    use std::time::SystemTime;
    use parquet::file::statistics::Statistics;
    use datafusion::physical_plan::hash_aggregate::{HashAggregateExec, AggregateMode};
    use datafusion::physical_plan::expressions::{Count, binary, Literal};
    use datafusion::physical_plan::{expressions, ExecutionPlan};
    use arrow::datatypes::DataType;
    use std::sync::Arc;
    use arrow::array::{PrimitiveArrayOps, UInt64Array};
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::logical_plan::Operator;
    use datafusion::scalar::ScalarValue;
    use futures::executor::block_on;

    #[test]
    fn gutter() {
        let store = ParquetTableStore {
            table: Index::new("foo".to_string(), 1, vec![
                Column::new("foo_int".to_string(), ColumnType::Int, 0),
                Column::new("foo".to_string(), ColumnType::String, 1),
                Column::new("boo".to_string(), ColumnType::String, 2),
                Column::new("bool".to_string(), ColumnType::Boolean, 3),
            ], 3),
            row_group_size: 7,
        };
        let file_name = "foo.parquet";

        let mut first_rows = (0..40).map(|i| Row::new(vec![
            TableValue::Int(i),
            TableValue::String(format!("Foo {}", i)),
            if i % 7 == 0 { TableValue::Null } else { TableValue::String(format!("Boo {}", i)) },
            TableValue::Boolean(i % 5 == 0)
        ])).collect::<Vec<_>>();
        first_rows.sort_by(|a, b| a.sort_key(3).cmp(&b.sort_key(3)));
        store.merge_rows(None, vec![file_name.to_string()], first_rows.clone(), 3).unwrap();
        let read_rows = store.read_rows(file_name).unwrap();
        assert_eq!(read_rows.len(), first_rows.len());
        for (read, expected) in read_rows.iter().zip(first_rows.clone()) {
            assert_eq!(read, &expected);
        }

        let next_file = "foo-2.parquet";
        let mut next_rows = (40..100).map(|i| Row::new(
            vec![
                TableValue::Int(i),
                TableValue::String(format!("Foo {}", i)),
                TableValue::String(format!("Boo {}", i)),
                TableValue::Boolean(false)
            ])).collect::<Vec<_>>();
        next_rows.sort_by(|a, b| a.sort_key(3).cmp(&b.sort_key(3)));
        store.merge_rows(Some(file_name), vec![next_file.to_string()], next_rows.clone(), 3).unwrap();

        let mut resulting = first_rows.clone();
        resulting.append(&mut next_rows);
        resulting.sort_by(|a, b| a.sort_key(3).cmp(&b.sort_key(3)));

        let read_rows = store.read_rows(next_file).unwrap();
        assert_eq!(read_rows.len(), resulting.len());
        for ((read, expected), _) in read_rows.iter().zip(resulting.iter()).zip(0..) {
            // println!("{}", i);
            // println!("{:?} - {:?}", read, expected);
            assert_eq!(read, expected);
        }

        // Split

        let split_1 = "foo-3-1.parquet";
        let split_2 = "foo-3-2.parquet";
        let mut next_rows = (100..150).map(|i| Row::new(vec![
            TableValue::Int(i),
            TableValue::String(format!("Foo {}", i)),
            TableValue::String(format!("Boo {}", i)),
            TableValue::Boolean(false)
        ])).collect::<Vec<_>>();
        next_rows.sort_by(|a, b| a.sort_key(3).cmp(&b.sort_key(3)));
        let min_max = store.merge_rows(Some(next_file), vec![split_1.to_string(), split_2.to_string()], next_rows.clone(), 3).unwrap();

        resulting.append(&mut next_rows);
        resulting.sort_by(|a, b| a.sort_key(3).cmp(&b.sort_key(3)));

        let read_rows_1 = store.read_rows(split_1).unwrap();
        let read_rows_2 = store.read_rows(split_2).unwrap();
        assert_eq!(read_rows_1.len() + read_rows_2.len(), resulting.len());
        let read_rows = read_rows_1.iter().chain(read_rows_2.iter());
        for (read, expected) in read_rows.zip(resulting.iter()) {
            // println!("{}", i);
            // println!("{:?} - {:?}", read, expected);
            assert_eq!(read, expected);
        }

        assert_eq!(min_max, vec![
            (75, (
                Row::new(vec![TableValue::Int(0), TableValue::String(format!("Foo {}", 0)), TableValue::Null, TableValue::Boolean(true)]),
                Row::new(vec![TableValue::Int(74), TableValue::String(format!("Foo {}", 74)), TableValue::String(format!("Boo {}", 74)), TableValue::Boolean(false)])
            )),
            (75, (
                Row::new(vec![TableValue::Int(75), TableValue::String(format!("Foo {}", 75)), TableValue::String(format!("Boo {}", 75)), TableValue::Boolean(false)]),
                Row::new(vec![TableValue::Int(149), TableValue::String(format!("Foo {}", 149)), TableValue::String(format!("Boo {}", 149)), TableValue::Boolean(false)])
            ))
        ]);

        fs::remove_file(file_name).unwrap();
        fs::remove_file(next_file).unwrap();
        fs::remove_file(split_1).unwrap();
        fs::remove_file(split_2).unwrap();
    }

    #[bench]
    fn filter_count(b: &mut Bencher) {
        if let Ok((store, columns_to_read)) = prepare_donors() {
            let mut reader = RowParquetReader::open(&store.table, "Donors.parquet", Some(&columns_to_read)).unwrap();

            b.iter(|| {
                let start = SystemTime::now();
                let mut counter = 0;
                for row_group in 0..reader.parquet_reader.num_row_groups() {
                    {
                        let (_, index, _, _) = &reader.column_with_buffer[0];
                        if let Some(Statistics::ByteArray(stats)) = reader.parquet_reader.get_row_group(row_group).unwrap().metadata().column(*index).statistics() {
                            let min = stats.min().as_utf8().unwrap();
                            let max = stats.max().as_utf8().unwrap();
                            println!("Min: {}, Max: {}", min, max);
                            if !(min <= "San Francisco" && "San Francisco" <= max) {
                                continue;
                            }
                        }
                    }
                    let values_read = reader.load_row_group(row_group).unwrap();
                    let (_, _, accessor, _) = &reader.column_with_buffer[0];
                    if let ColumnAccessor::Bytes(buffer) = accessor {
                        for i in 0..values_read {
                            if buffer[i].as_utf8().unwrap() == "San Francisco" {
                                counter += 1;
                            }
                        }
                    }
                }
                println!("San Francisco count ({:?}): {}", start.elapsed().unwrap(), counter);
            });
        }
    }

    #[bench]
    fn filter_count_using_scan(b: &mut Bencher) {
        if let Ok((store, columns_to_read)) = prepare_donors() {
            b.iter(|| {
                let start = SystemTime::now();
                let reader = store.scan_node(
                    "Donors.parquet",
                    &columns_to_read,
                    Some(Arc::new(|group| {
                        if let Some(Statistics::ByteArray(stats)) = group.column(0).statistics() {
                            let min = stats.min().as_utf8().unwrap();
                            let max = stats.max().as_utf8().unwrap();
                            println!("Min: {}, Max: {}", min, max);
                            min <= "San Francisco" && "San Francisco" <= max
                        } else {
                            false
                        }
                    })),
                ).unwrap();
                let filter_expr = binary(
                    Arc::new(expressions::Column::new("Donor City")),
                    Operator::Eq,
                    Arc::new(Literal::new(ScalarValue::Utf8(Some("San Francisco".to_string())))),
                    reader.schema().as_ref(),
                ).unwrap();
                let filter = Arc::new(FilterExec::try_new(filter_expr, reader).unwrap());
                let aggregate = HashAggregateExec::try_new(
                    AggregateMode::Partial,
                    vec![],
                    vec![Arc::new(Count::new(
                        Arc::new(expressions::Column::new("Donor City")), "count".to_string(), DataType::UInt64)
                    )],
                    filter,
                ).unwrap();
                let mut res = block_on(async { aggregate.execute(0).await }).unwrap();
                let batch = res.next().unwrap().unwrap();
                let result = batch.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
                println!("San Francisco count ({:?}): {}", start.elapsed().unwrap(), result.value(0));
            });
        }
    }

    fn prepare_donors() -> Result<(ParquetTableStore, Vec<Column>), io::Error> {
        let store = ParquetTableStore {
            table: Index::new("donors".to_string(), 1, vec![
                Column::new("Donor City".to_string(), ColumnType::String, 0),
                Column::new("Donor ID".to_string(), ColumnType::String, 1),
                Column::new("Donor State".to_string(), ColumnType::String, 2),
                Column::new("Donor Is Teacher".to_string(), ColumnType::String, 3),
                Column::new("Donor Zip".to_string(), ColumnType::String, 4),
            ], 6),
            row_group_size: 16384,
        };

        let column_mapping = vec![1, 0, 2, 3, 4];

        let donors = File::open("Donors.csv")?;
        let mut rdr = ReaderBuilder::new()
            .from_reader(BufReader::new(donors));

        let mut index = 0;
        let mut to_merge = Vec::new();
        let column_count = store.table.get_columns().len();
        let mut current_file = None;

        if fs::metadata("Donors.parquet").is_err() {
            for record in rdr.records() {
                let r = record.unwrap();
                let mut values = Vec::with_capacity(column_count);
                for c in store.table.get_columns() {
                    values.push(TableValue::String(r[column_mapping[c.get_index()]].to_string()));
                }
                to_merge.push(Row::new(values));
                index += 1;
                if index % 500000 == 0 {
                    current_file = merge_for_bench(&store, &mut index, &mut to_merge, &current_file);
                }
            }

            merge_for_bench(&store, &mut index, &mut to_merge, &current_file);
        }

        let columns_to_read = vec![store.table.get_columns()[0].clone()];
        Ok((store, columns_to_read))
    }

    fn merge_for_bench(store: &ParquetTableStore, index: &mut i32, mut to_merge: &mut Vec<Row>, current_file: &Option<String>) -> Option<String> {
        println!("Merging {}", index);
        let dest_file = current_file.as_ref().map(|f| format!("{}.new", f)).unwrap_or("Donors.parquet".to_string());
        let mut tmp = Vec::new();
        swap(&mut tmp, &mut to_merge);
        tmp.sort_by(|a, b| a.sort_key(2).cmp(&b.sort_key(2)));
        store.merge_rows(current_file.as_ref().map(|s| s.as_str()), vec![dest_file], tmp, 2).unwrap();
        fs::rename("Donors.parquet.new", "Donors.parquet").unwrap();
        Some("Donors.parquet".to_string())
    }
}
