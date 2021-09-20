use super::TimestampValue;
use crate::metastore::{Column, ColumnType, Index};
use crate::table::{Row, TableStore};
use crate::CubeError;
use parquet::column::reader::ColumnReader;
use parquet::column::writer::ColumnWriter;
use parquet::data_type::*;
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::{FileWriter, SerializedFileWriter};
use parquet::schema::types;
use std::cmp::{max, min, Ordering};
use std::fs::File;

use crate::table::data::{
    cmp_row_key, cmp_row_key_heap, convert_row_to_heap_allocated, MutRows, Rows, RowsView,
    TableValueR,
};
use crate::util::decimal::Decimal;
use num::integer::div_ceil;
use std::sync::Arc;

pub struct ParquetTableStore {
    table: Index,
    row_group_size: usize,
}

pub struct RowParquetWriter {
    columns: Vec<Column>,
    parquet_writer: SerializedFileWriter<File>,
    buffer: MutRows,
    row_group_size: usize,
}

enum ColumnAccessor {
    Bytes(Vec<ByteArray>),
    Int(Vec<i64>),
    Boolean(Vec<bool>),
    Float(Vec<f64>),
}

pub struct RowParquetReader<'a> {
    pub parquet_reader: SerializedFileReader<File>,
    column_with_buffer: Vec<(&'a Column, usize, ColumnAccessor, Option<Vec<i16>>)>,
}

impl TableStore for ParquetTableStore {
    fn merge_rows<'a>(
        &'a self,
        source_file: Option<&'a str>,
        dest_files: Vec<String>,
        rows: RowsView<'a>,
        sort_key_size: usize,
    ) -> Result<Vec<(u64, (Row, Row))>, CubeError> {
        let mut writers = Vec::new();
        for f in dest_files.iter() {
            writers.push(RowParquetWriter::open(&self.table, f, self.row_group_size)?);
        }
        if source_file.is_none() {
            let mut split_writer = SplitRowParquetWriter::new(writers, rows.len(), sort_key_size);
            split_writer.write_rows(rows)?;
            return Ok(split_writer.close()?);
        }

        let mut reader = RowParquetReader::open(&self.table, source_file.unwrap(), None)?;
        let mut right_position = 0;
        let total_row_number =
            reader.parquet_reader.metadata().file_metadata().num_rows() as usize + rows.len();
        let mut split_writer = SplitRowParquetWriter::new(writers, total_row_number, sort_key_size);

        for row_group_index in 0..reader.parquet_reader.num_row_groups() {
            let mut read_rows = MutRows::new(reader.column_with_buffer.len());
            reader.read_rows(row_group_index, &mut read_rows)?;
            let (new_pos, to_write) = ParquetTableStore::merge_sort(
                read_rows.freeze(),
                rows,
                right_position,
                sort_key_size as usize,
            );
            split_writer.write_rows(to_write.view())?;
            right_position = new_pos;
        }

        if right_position < rows.len() {
            split_writer.write_rows(rows.slice(right_position, rows.len()))?;
        }

        Ok(split_writer.close()?)
    }

    fn read_rows(&self, file: &str) -> Result<Rows, CubeError> {
        let mut reader = RowParquetReader::open(&self.table, file, None)?;
        let mut result = MutRows::new(reader.column_with_buffer.len());
        for row_group_index in 0..reader.parquet_reader.num_row_groups() {
            reader.read_rows(row_group_index, &mut result)?;
        }
        Ok(result.freeze())
    }

    fn read_filtered_rows(
        &self,
        file: &str,
        columns: &Vec<Column>,
        limit: usize,
    ) -> Result<Rows, CubeError> {
        let mut reader = RowParquetReader::open(&self.table, file, Some(columns))?;
        let mut result = MutRows::new(reader.column_with_buffer.len());
        for row_group_index in 0..reader.parquet_reader.num_row_groups() {
            reader.read_rows(row_group_index, &mut result)?;
            if limit <= result.num_rows() {
                result.truncate(limit);
                break;
            }
        }
        Ok(result.freeze())
    }

    // fn scan_node(
    //     &self,
    //     file: &str,
    //     columns: &Vec<Column>,
    //     row_group_filter: Option<Arc<dyn Fn(&RowGroupMetaData) -> bool + Send + Sync>>,
    // ) -> Result<Arc<dyn ExecutionPlan + Send + Sync>, CubeError> {
    //     Ok(Arc::new(ParquetExec::try_new_with_filter(
    //         file,
    //         Some(columns.iter().map(|c| c.get_index()).collect::<Vec<_>>()),
    //         self.row_group_size,
    //         row_group_filter,
    //     )?))
    // }
}

impl ParquetTableStore {
    pub fn new(table: Index, row_group_size: usize) -> ParquetTableStore {
        ParquetTableStore {
            table,
            row_group_size,
        }
    }

    #[cfg(test)]
    pub fn merge_rows_from_heap<'a>(
        &'a self,
        source_file: Option<&'a str>,
        dest_files: Vec<String>,
        rows: Vec<Row>,
        sort_key_size: usize,
    ) -> Result<Vec<(u64, (Row, Row))>, CubeError> {
        let mut buffer = Vec::new();
        self.merge_rows(
            source_file,
            dest_files,
            RowsView::from_heap_allocated(&mut buffer, rows[0].len(), &rows),
            sort_key_size,
        )
    }

    fn merge_sort(
        left: Rows,
        right: RowsView,
        initial_right_pos: usize,
        sort_key_size: usize,
    ) -> (usize, Rows) {
        let leftv = left.view();
        if right.len() == initial_right_pos
            || cmp_row_key(
                sort_key_size,
                &leftv[leftv.len() - 1],
                &right[initial_right_pos],
            ) <= Ordering::Equal
        {
            return (initial_right_pos, left);
        }
        let mut result = MutRows::with_capacity(left.num_columns(), left.num_rows());
        let mut left_position = 0;
        let mut right_position = initial_right_pos;
        while left_position < left.num_rows() {
            if right.len() <= right_position
                || cmp_row_key(sort_key_size, &leftv[left_position], &right[right_position])
                    <= Ordering::Equal
            {
                result.add_row_copy(&leftv[left_position]); // TODO copy
                left_position += 1;
            } else {
                result.add_row_copy(&right[right_position]); // TODO copy
                right_position += 1;
            }
        }
        (right_position, result.freeze())
    }
}

impl<'a> RowParquetReader<'a> {
    fn open(
        table: &'a Index,
        file: &'a str,
        columns_to_read: Option<&'a Vec<Column>>,
    ) -> Result<RowParquetReader<'a>, CubeError> {
        let file = File::open(file)?;
        let parquet_reader = SerializedFileReader::new(file)?;

        let column_with_buffer = columns_to_read
            .unwrap_or(table.get_columns())
            .iter()
            .map(|c| {
                (
                    c,
                    c.get_index(),
                    match c.get_column_type() {
                        ColumnType::String => ColumnAccessor::Bytes(vec![ByteArray::new(); 16384]),
                        ColumnType::Bytes | ColumnType::HyperLogLog(_) => {
                            ColumnAccessor::Bytes(vec![ByteArray::new(); 16384])
                        }
                        ColumnType::Int => ColumnAccessor::Int(vec![0; 16384]),
                        ColumnType::Decimal { .. } => ColumnAccessor::Int(vec![0; 16384]),
                        ColumnType::Timestamp => ColumnAccessor::Int(vec![0; 16384]),
                        ColumnType::Boolean => ColumnAccessor::Boolean(vec![false; 16384]),
                        ColumnType::Float => ColumnAccessor::Float(vec![0.0; 16384]),
                    },
                    Some(vec![0; 16384]),
                )
            })
            .collect::<Vec<_>>();

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
                        values_read = max(
                            values_read,
                            reader
                                .read_batch(
                                    buffer.len(),
                                    def_levels.as_mut().map(|l| l.as_mut_slice()),
                                    None,
                                    buffer.as_mut_slice(),
                                )?
                                .1,
                        );
                    }
                }
                ColumnAccessor::Int(buffer) => {
                    if let ColumnReader::Int64ColumnReader(ref mut reader) = col_reader {
                        values_read = max(
                            values_read,
                            reader
                                .read_batch(
                                    buffer.len(),
                                    def_levels.as_mut().map(|l| l.as_mut_slice()),
                                    None,
                                    buffer.as_mut_slice(),
                                )?
                                .1,
                        );
                    }
                }
                ColumnAccessor::Boolean(buffer) => {
                    if let ColumnReader::BoolColumnReader(ref mut reader) = col_reader {
                        values_read = max(
                            values_read,
                            reader
                                .read_batch(
                                    buffer.len(),
                                    def_levels.as_mut().map(|l| l.as_mut_slice()),
                                    None,
                                    buffer.as_mut_slice(),
                                )?
                                .1,
                        );
                    }
                }
                ColumnAccessor::Float(buffer) => {
                    if let ColumnReader::DoubleColumnReader(ref mut reader) = col_reader {
                        values_read = max(
                            values_read,
                            reader
                                .read_batch(
                                    buffer.len(),
                                    def_levels.as_mut().map(|l| l.as_mut_slice()),
                                    None,
                                    buffer.as_mut_slice(),
                                )?
                                .1,
                        );
                    }
                }
            };
        }
        Ok(values_read)
    }

    fn read_rows(&mut self, row_group_index: usize, output: &mut MutRows) -> Result<(), CubeError> {
        assert_eq!(output.num_columns(), self.column_with_buffer.len());

        let values_read = self.load_row_group(row_group_index)?;
        let mut result = output.add_rows(values_read);
        for (col_i, (col, _, column_accessor, def_levels)) in
            self.column_with_buffer.iter().enumerate()
        {
            let mut cur_value_index = 0;
            match def_levels {
                Some(levels) => {
                    match col.get_column_type() {
                        ColumnType::String => {
                            if let ColumnAccessor::Bytes(buffer) = &column_accessor {
                                for i in 0..values_read {
                                    if levels[i] == 1 {
                                        let value = buffer[cur_value_index].as_utf8()?;
                                        result.set_interned(i, col_i, TableValueR::String(value));
                                        cur_value_index += 1;
                                    } else {
                                        result.set_interned(i, col_i, TableValueR::Null);
                                    }
                                }
                            }
                        }
                        ColumnType::Bytes | ColumnType::HyperLogLog(_) => {
                            if let ColumnAccessor::Bytes(buffer) = &column_accessor {
                                for i in 0..values_read {
                                    if levels[i] == 1 {
                                        let value = buffer[cur_value_index].as_bytes();
                                        result.set_interned(i, col_i, TableValueR::Bytes(value));
                                        cur_value_index += 1;
                                    } else {
                                        result.set_interned(i, col_i, TableValueR::Null);
                                    }
                                }
                            }
                        }
                        ColumnType::Int => {
                            if let ColumnAccessor::Int(buffer) = &column_accessor {
                                for i in 0..values_read {
                                    if levels[i] == 1 {
                                        let value = buffer[cur_value_index];
                                        result.set_interned(i, col_i, TableValueR::Int(value));
                                        cur_value_index += 1;
                                    } else {
                                        result.set_interned(i, col_i, TableValueR::Null);
                                    }
                                }
                            }
                        }
                        ColumnType::Decimal { .. } => {
                            if let ColumnAccessor::Int(buffer) = &column_accessor {
                                for i in 0..values_read {
                                    if levels[i] == 1 {
                                        let value = buffer[cur_value_index];

                                        result.set_interned(
                                            i,
                                            col_i,
                                            TableValueR::Decimal(Decimal::new(value)),
                                        );
                                        cur_value_index += 1;
                                    } else {
                                        result.set_interned(i, col_i, TableValueR::Null);
                                    }
                                }
                            }
                        }
                        ColumnType::Timestamp => {
                            if let ColumnAccessor::Int(buffer) = &column_accessor {
                                for i in 0..values_read {
                                    if levels[i] == 1 {
                                        let value = buffer[cur_value_index];

                                        result.set_interned(
                                            i,
                                            col_i,
                                            TableValueR::Timestamp(TimestampValue::new(
                                                value * 1000 as i64,
                                            )),
                                        );
                                        cur_value_index += 1;
                                    } else {
                                        result.set_interned(i, col_i, TableValueR::Null);
                                    }
                                }
                            }
                        }
                        ColumnType::Boolean => {
                            if let ColumnAccessor::Boolean(buffer) = &column_accessor {
                                for i in 0..values_read {
                                    if levels[i] == 1 {
                                        let value = buffer[cur_value_index];
                                        result.set_interned(i, col_i, TableValueR::Boolean(value));
                                        cur_value_index += 1;
                                    } else {
                                        result.set_interned(i, col_i, TableValueR::Null);
                                    }
                                }
                            }
                        }
                        ColumnType::Float => {
                            if let ColumnAccessor::Float(buffer) = &column_accessor {
                                for i in 0..values_read {
                                    if levels[i] == 1 {
                                        let value = buffer[cur_value_index];
                                        result.set_interned(
                                            i,
                                            col_i,
                                            TableValueR::Float(value.into()),
                                        );
                                        cur_value_index += 1;
                                    } else {
                                        result.set_interned(i, col_i, TableValueR::Null);
                                    }
                                }
                            }
                        }
                    };
                }
                x => panic!("Unsupported value: {:?}", x),
            }
        }
        Ok(())
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
    sort_key_size: usize,
}

impl SplitRowParquetWriter {
    pub fn new(
        writers: Vec<RowParquetWriter>,
        total_row_number: usize,
        sort_key_size: usize,
    ) -> SplitRowParquetWriter {
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
            sort_key_size,
        }
    }

    fn write_rows(&'a mut self, rows: RowsView<'a>) -> Result<(), CubeError> {
        if rows.len() == 0 {
            return Ok(());
        }
        if self.first_row.is_none() {
            self.first_row = Some(convert_row_to_heap_allocated(&rows[0]));
        }
        let mut remaining_slice = rows;
        while remaining_slice.len() + self.rows_written
            > (self.current_writer + 1) * self.chunk_size
        {
            let target_split_at = (self.current_writer + 1) * self.chunk_size;
            let mut split_at = if self.rows_written > target_split_at {
                0
            } else {
                target_split_at - self.rows_written
            };
            // move to the last position with a matching sort_key
            while split_at < remaining_slice.len()
                && self.is_current_key_equal(&remaining_slice, split_at)
            {
                split_at += 1;
            }
            if split_at == remaining_slice.len() - 1 {
                break;
            }
            if split_at == 0 {
                self.min_max_rows.push((
                    self.rows_written_current_file,
                    (
                        self.first_row.as_ref().unwrap().clone(),
                        self.last_row.as_ref().unwrap().clone(),
                    ),
                ));
            } else {
                self.writers[self.current_writer].write_rows(remaining_slice.slice(0, split_at))?;
                self.rows_written += split_at;
                self.rows_written_current_file += split_at as u64;
                self.min_max_rows.push((
                    self.rows_written_current_file,
                    (
                        self.first_row
                            .as_ref()
                            .map(|r| r.clone())
                            .unwrap_or_else(|| convert_row_to_heap_allocated(&remaining_slice[0])),
                        convert_row_to_heap_allocated(&remaining_slice[split_at - 1]),
                    ),
                ));
            }
            self.rows_written_current_file = 0;
            self.first_row = None;
            self.current_writer += 1;
            remaining_slice = remaining_slice.slice(split_at, remaining_slice.len());
        }
        if remaining_slice.len() > 0 {
            if self.first_row.is_none() {
                self.first_row = Some(convert_row_to_heap_allocated(&remaining_slice[0]));
            }
            self.writers[self.current_writer].write_rows(remaining_slice)?;
            self.last_row = Some(convert_row_to_heap_allocated(
                &remaining_slice[remaining_slice.len() - 1],
            ));
            self.rows_written += remaining_slice.len();
            self.rows_written_current_file += remaining_slice.len() as u64;
        }

        Ok(())
    }

    fn is_current_key_equal<'a>(
        &'a self,
        remaining_slice: &'a RowsView<'a>,
        split_at: usize,
    ) -> bool {
        if split_at == 0 {
            return self.last_row.is_some()
                && cmp_row_key_heap(
                    self.sort_key_size,
                    &self.last_row.as_ref().unwrap().values,
                    &remaining_slice[split_at],
                ) == Ordering::Equal;
        } else {
            return cmp_row_key(
                self.sort_key_size,
                &remaining_slice[split_at - 1],
                &remaining_slice[split_at],
            ) == Ordering::Equal;
        }
    }

    fn close(mut self) -> Result<Vec<(u64, (Row, Row))>, CubeError> {
        // TODO handle case if only one partition is written out of 3
        assert!(self.current_writer == self.writers.len() - 1);
        if self.first_row.is_some() && self.last_row.is_some() {
            self.min_max_rows.push((
                self.rows_written_current_file,
                (
                    self.first_row.as_ref().unwrap().clone(),
                    self.last_row.as_ref().unwrap().clone(),
                ),
            ));
            self.rows_written_current_file = 0;
        }
        for w in self.writers.into_iter() {
            w.close()?;
        }
        let sort_key_size = self.sort_key_size;
        Ok(self
            .min_max_rows
            .into_iter()
            .map(|(c, (min, max))| {
                (
                    c,
                    (
                        Row::new(min.values.into_iter().take(sort_key_size).collect()),
                        Row::new(max.values.into_iter().take(sort_key_size).collect()),
                    ),
                )
            })
            .collect())
    }
}

impl RowParquetWriter {
    fn open(
        table: &'a Index,
        file: &'a str,
        row_group_size: usize,
    ) -> Result<RowParquetWriter, CubeError> {
        let file = File::create(file)?;

        let mut fields = table
            .get_columns()
            .iter()
            .map(|column| {
                // TODO pass nullable columns
                Arc::new(parquet::schema::types::Type::from(column))
            })
            .collect();

        let schema = Arc::new(
            types::Type::group_type_builder("schema")
                .with_fields(&mut fields)
                .build()
                .unwrap(),
        );

        let props = Self::writer_props();
        let parquet_writer = SerializedFileWriter::new(file.try_clone()?, schema, props)?;

        Ok(RowParquetWriter {
            columns: table.get_columns().clone(),
            parquet_writer,
            row_group_size,
            buffer: MutRows::with_capacity(table.get_columns().len(), row_group_size as usize),
        })
    }

    fn write_rows(&'a mut self, rows: RowsView<'a>) -> Result<(), CubeError> {
        self.buffer.add_from_slice(rows); // TODO avoid copying data.

        if self.buffer.num_rows() >= self.row_group_size {
            self.write_buffer()?;
        }

        Ok(())
    }

    fn write_buffer(&'a mut self) -> Result<(), CubeError> {
        let batch_size = self.row_group_size;
        let row_group_count = div_ceil(self.buffer.num_rows(), self.row_group_size);
        for row_batch_index in 0..row_group_count {
            let mut row_group_writer = self.parquet_writer.next_row_group()?;

            let mut column_index = 0;

            let rows_in_group = min(
                batch_size,
                self.buffer.num_rows() - row_batch_index * batch_size,
            );

            while let Some(mut col_writer) = row_group_writer.next_column()? {
                // TODO types
                match col_writer {
                    ColumnWriter::Int64ColumnWriter(ref mut typed) => {
                        let column = &self.columns[column_index];
                        let mut min = None;
                        let mut max = None;
                        let column_values = (0..rows_in_group)
                            .filter(|row_index| {
                                &self.buffer.rows()[row_batch_index * batch_size + row_index]
                                    [column_index]
                                    != &TableValueR::Null
                            })
                            .map(|row_index| -> Result<_, CubeError> {
                                // TODO types
                                match &self.buffer.rows()[row_batch_index * batch_size + row_index]
                                    [column_index]
                                {
                                    TableValueR::Int(val) => Ok(*val),
                                    TableValueR::Decimal(val) => match column.get_column_type() {
                                        ColumnType::Decimal { .. } => Ok(val.raw_value()),
                                        x => panic!("Unexpected type: {:?}", x),
                                    },
                                    TableValueR::Timestamp(t) => {
                                        Ok(i64::from(t.clone().get_time_stamp() / 1000))
                                    }
                                    x => panic!("Unsupported value: {:?}", x),
                                }
                            })
                            .map(|res_val| {
                                if res_val.is_err() {
                                    return res_val;
                                }
                                let v = res_val.unwrap();
                                if min.is_none() || v < min.unwrap() {
                                    min = Some(v)
                                }
                                if max.is_none() || max.unwrap() < v {
                                    max = Some(v)
                                }
                                return Ok(v);
                            })
                            .collect::<Result<Vec<i64>, _>>()?;
                        let def_levels = self.get_def_levels(
                            batch_size,
                            row_batch_index,
                            column_index,
                            rows_in_group,
                            column_values.len(),
                        );
                        typed.write_batch_with_statistics(
                            &column_values,
                            def_levels.as_ref().map(|b| b.as_slice()),
                            None,
                            &min,
                            &max,
                            None,
                            None,
                        )?;
                    }
                    ColumnWriter::DoubleColumnWriter(ref mut typed) => {
                        let column = &self.columns[column_index];
                        let mut min = None;
                        let mut max = None;
                        let column_values = (0..rows_in_group)
                            .filter(|row_index| {
                                &self.buffer.rows()[row_batch_index * batch_size + row_index]
                                    [column_index]
                                    != &TableValueR::Null
                            })
                            .map(|row_index| -> Result<_, CubeError> {
                                // TODO types
                                match &self.buffer.rows()[row_batch_index * batch_size + row_index]
                                    [column_index]
                                {
                                    TableValueR::Float(val) => match column.get_column_type() {
                                        ColumnType::Float => Ok(*val),
                                        x => panic!("Unexpected type: {:?}", x),
                                    },
                                    x => panic!("Unsupported value: {:?}", x),
                                }
                            })
                            .map(|res_val| {
                                if let Err(e) = res_val {
                                    return Err(e);
                                }
                                // We must use OrdF64 here!
                                let v = res_val.unwrap();
                                if min.is_none() || v < min.unwrap() {
                                    min = Some(v)
                                }
                                if max.is_none() || max.unwrap() < v {
                                    max = Some(v)
                                }
                                return Ok(v.0);
                            })
                            .collect::<Result<Vec<f64>, _>>()?;
                        let def_levels = self.get_def_levels(
                            batch_size,
                            row_batch_index,
                            column_index,
                            rows_in_group,
                            column_values.len(),
                        );
                        typed.write_batch_with_statistics(
                            &column_values,
                            def_levels.as_ref().map(|b| b.as_slice()),
                            None,
                            &min.map(|f| f.0),
                            &max.map(|f| f.0),
                            None,
                            None,
                        )?;
                    }
                    ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
                        // Both vars store indicies into the `column_values`.
                        let mut min: Option<String> = None;
                        let mut max: Option<String> = None;
                        let mut use_min_max = true;
                        let mut update_stats = |v: &TableValueR| {
                            if !use_min_max {
                                return;
                            }
                            let s;
                            if let TableValueR::String(ss) = v {
                                s = ss;
                            } else {
                                use_min_max = false;
                                min = None;
                                max = None;
                                return;
                            }
                            if min.is_none() || s < &min.as_deref().unwrap() {
                                min = Some(s.to_string()) // TODO: remove allocations.
                            }
                            if max.is_none() || &max.as_deref().unwrap() < s {
                                max = Some(s.to_string()) // TODO: remove allocations
                            }
                        };
                        let column_values = (0..rows_in_group)
                            .filter(|row_index| {
                                &self.buffer.rows()[row_batch_index * batch_size + row_index]
                                    [column_index]
                                    != &TableValueR::Null
                            })
                            .map(|row_index| {
                                let v = &self.buffer.rows()
                                    [row_batch_index * batch_size + row_index][column_index];
                                update_stats(v);
                                // TODO types
                                match v {
                                    TableValueR::String(str) => ByteArray::from(*str),
                                    TableValueR::Bytes(bytes) => ByteArray::from(bytes.to_vec()),
                                    x => panic!("Unsupported value: {:?}", x),
                                }
                            })
                            .collect::<Vec<ByteArray>>();
                        let def_levels = self.get_def_levels(
                            batch_size,
                            row_batch_index,
                            column_index,
                            rows_in_group,
                            column_values.len(),
                        );
                        assert!(use_min_max || min.is_none() && max.is_none());
                        typed.write_batch_with_statistics(
                            &column_values,
                            def_levels.as_ref().map(|b| b.as_slice()),
                            None,
                            &min.map(|s| ByteArray::from(s.into_bytes())),
                            &max.map(|s| ByteArray::from(s.into_bytes())),
                            None,
                            None,
                        )?;
                    }
                    ColumnWriter::BoolColumnWriter(ref mut typed) => {
                        let mut min = None;
                        let mut max = None;
                        let column_values = (0..rows_in_group)
                            .filter(|row_index| {
                                &self.buffer.rows()[row_batch_index * batch_size + row_index]
                                    [column_index]
                                    != &TableValueR::Null
                            })
                            .map(|row_index| {
                                // TODO types
                                match &self.buffer.rows()[row_batch_index * batch_size + row_index]
                                    [column_index]
                                {
                                    TableValueR::Boolean(b) => *b,
                                    x => panic!("Unsupported value: {:?}", x),
                                }
                            })
                            .map(|res_val| {
                                let v = res_val;
                                if min.is_none() || v < min.unwrap() {
                                    min = Some(v)
                                }
                                if max.is_none() || max.unwrap() < v {
                                    max = Some(v)
                                }
                                return res_val;
                            })
                            .collect::<Vec<bool>>();
                        let def_levels = self.get_def_levels(
                            batch_size,
                            row_batch_index,
                            column_index,
                            rows_in_group,
                            column_values.len(),
                        );
                        typed.write_batch_with_statistics(
                            &column_values,
                            def_levels.as_ref().map(|b| b.as_slice()),
                            None,
                            &min,
                            &max,
                            None,
                            None,
                        )?;
                    }
                    _ => panic!("Unsupported writer"),
                };

                row_group_writer.close_column(col_writer)?;
                column_index += 1;
            }

            self.parquet_writer.close_row_group(row_group_writer)?;
        }

        let target_size = if row_group_count * self.row_group_size > self.buffer.num_rows() {
            0
        } else {
            self.buffer.num_rows() - row_group_count * self.row_group_size
        };
        let mut leftovers = MutRows::with_capacity(self.buffer.num_columns(), self.row_group_size);
        leftovers.add_from_slice(
            self.buffer
                .rows()
                .slice(self.buffer.num_rows() - target_size, self.buffer.num_rows()),
        );
        self.buffer = leftovers;
        Ok(())
    }

    fn get_def_levels(
        &self,
        batch_size: usize,
        row_batch_index: usize,
        column_index: usize,
        rows_in_group: usize,
        _column_values_len: usize,
    ) -> Option<Vec<i16>> {
        Some(
            (0..rows_in_group)
                .map(|row_index| {
                    // TODO types
                    match &self.buffer.rows()[row_batch_index * batch_size + row_index]
                        [column_index]
                    {
                        TableValueR::Null => 0,
                        _ => 1,
                    }
                })
                .collect::<Vec<i16>>(),
        )
    }

    fn close(mut self) -> Result<(), CubeError> {
        self.write_buffer()?;
        self.parquet_writer.close()?;
        Ok(())
    }

    fn writer_props() -> Arc<WriterProperties> {
        Arc::new(
            WriterProperties::builder()
                // .set_key_value_metadata(Some(vec![KeyValue::new(
                //     "key".to_string(),
                //     "value".to_string(),
                // )]))
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .set_statistics_enabled(true)
                // .set_column_dictionary_enabled(ColumnPath::new(vec!["col0".to_string()]), true)
                // .set_column_encoding(ColumnPath::new(vec!["col1".to_string()]), Encoding::RLE)
                .build(),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::metastore::{Column, ColumnType, Index};
    use crate::table::parquet::{
        ColumnAccessor, ParquetTableStore, RowParquetReader, RowParquetWriter,
    };
    use crate::table::{Row, TableStore, TableValue};
    use std::{fs, io};

    extern crate test;

    use crate::table::data::{convert_row_to_heap_allocated, RowsView, TableValueR};
    use crate::util::decimal::Decimal;
    use csv::ReaderBuilder;
    use itertools::Itertools;
    use parquet::file::reader::FileReader;
    use parquet::file::statistics::Statistics;
    use std::fs::File;
    use std::io::BufReader;
    use std::mem::swap;
    use std::time::SystemTime;
    use tempfile::NamedTempFile;
    use test::Bencher;

    #[test]
    fn gutter() {
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
                1,
            )
            .unwrap(),
            row_group_size: 10,
        };
        let file_name = "foo.parquet";

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
        first_rows.sort_by(|a, b| a.sort_key(1).cmp(&b.sort_key(1)));
        store
            .merge_rows_from_heap(None, vec![file_name.to_string()], first_rows.clone(), 3)
            .unwrap();
        let read_rows = store.read_rows(file_name).unwrap();
        assert_eq!(read_rows.len(), first_rows.len());
        for (read, expected) in read_rows.view().iter().zip(first_rows.clone()) {
            assert_eq!(&convert_row_to_heap_allocated(&read), &expected);
        }

        let next_file = "foo-2.parquet";
        let mut next_rows = (40..100)
            .map(|i| {
                Row::new(vec![
                    TableValue::Int(i),
                    TableValue::String(format!("Foo {}", i)),
                    TableValue::String(format!("Boo {}", i)),
                    TableValue::Boolean(false),
                    TableValue::Decimal(Decimal::new(i * 10000)),
                ])
            })
            .collect::<Vec<_>>();
        next_rows.sort_by(|a, b| a.sort_key(3).cmp(&b.sort_key(3)));
        store
            .merge_rows_from_heap(
                Some(file_name),
                vec![next_file.to_string()],
                next_rows.clone(),
                3,
            )
            .unwrap();

        let mut resulting = first_rows.clone();
        resulting.append(&mut next_rows);
        resulting.sort_by(|a, b| a.sort_key(1).cmp(&b.sort_key(1)));

        let read_rows = store.read_rows(next_file).unwrap();
        assert_eq!(read_rows.len(), resulting.len());
        for ((read, expected), _) in read_rows.view().iter().zip(resulting.iter()).zip(0..) {
            // println!("{}", i);
            // println!("{:?} - {:?}", read, expected);

            assert_eq!(&convert_row_to_heap_allocated(&read), expected);
        }

        // Split

        let split_1 = "foo-3-1.parquet";
        let split_2 = "foo-3-2.parquet";
        let mut next_rows = (100..150)
            .map(|i| {
                Row::new(vec![
                    TableValue::Int(i),
                    TableValue::String(format!("Foo {}", i)),
                    TableValue::String(format!("Boo {}", i)),
                    TableValue::Boolean(false),
                    TableValue::Decimal(Decimal::new(i * 10000)),
                ])
            })
            .collect::<Vec<_>>();
        next_rows.sort_by(|a, b| a.sort_key(1).cmp(&b.sort_key(1)));
        let min_max = store
            .merge_rows_from_heap(
                Some(next_file),
                vec![split_1.to_string(), split_2.to_string()],
                next_rows.clone(),
                3,
            )
            .unwrap();

        resulting.append(&mut next_rows);
        resulting.sort_by(|a, b| a.sort_key(1).cmp(&b.sort_key(1)));

        let read_rows_1 = store.read_rows(split_1).unwrap();
        let read_rows_2 = store.read_rows(split_2).unwrap();
        assert_eq!(read_rows_1.len() + read_rows_2.len(), resulting.len());
        let read_rows = read_rows_1
            .view()
            .iter()
            .chain(read_rows_2.view().iter())
            .map(|r| convert_row_to_heap_allocated(r))
            .collect_vec();
        for (read, expected) in read_rows.iter().zip(resulting.iter()) {
            // println!("{}", i);
            // println!("{:?} - {:?}", read, right);
            assert_eq!(read, expected);
        }

        assert_eq!(
            min_max,
            vec![
                (
                    75,
                    (
                        Row::new(vec![
                            TableValue::Null,
                            TableValue::String(format!("Foo {}", 0)),
                            TableValue::Null,
                        ]),
                        Row::new(vec![
                            TableValue::Int(74),
                            TableValue::String(format!("Foo {}", 74)),
                            TableValue::String(format!("Boo {}", 74)),
                        ])
                    )
                ),
                (
                    75,
                    (
                        Row::new(vec![
                            TableValue::Int(75),
                            TableValue::String(format!("Foo {}", 75)),
                            TableValue::String(format!("Boo {}", 75)),
                        ]),
                        Row::new(vec![
                            TableValue::Int(149),
                            TableValue::String(format!("Foo {}", 149)),
                            TableValue::String(format!("Boo {}", 149)),
                        ])
                    )
                )
            ]
        );

        fs::remove_file(file_name).unwrap();
        fs::remove_file(next_file).unwrap();
        fs::remove_file(split_1).unwrap();
        fs::remove_file(split_2).unwrap();
    }

    #[test]
    fn failed_rle_run_bools() {
        const NUM_ROWS: usize = 16384;

        let check_bools = |bools: &[TableValueR]| {
            let index = Index::try_new(
                "test".to_string(),
                0,
                vec![Column::new("b".to_string(), ColumnType::Boolean, 0)],
                1,
            )
            .unwrap();
            let tmp_file = NamedTempFile::new().unwrap();
            let mut w = RowParquetWriter::open(&index, tmp_file.path().to_str().unwrap(), NUM_ROWS)
                .unwrap();
            w.write_rows(RowsView::new(&bools, 1)).unwrap();
            w.close().unwrap()
        };

        // Maximize the data to write with RLE encoding.
        // First, in bit-packed encoding.
        let mut bools = Vec::with_capacity(NUM_ROWS);
        for _ in 0..NUM_ROWS / 2 {
            bools.push(TableValueR::Boolean(true));
            bools.push(TableValueR::Boolean(false));
        }
        check_bools(&bools);

        // Second, in RLE encoding.
        let mut bools = Vec::with_capacity(NUM_ROWS);
        for _ in 0..NUM_ROWS / 16 {
            for _ in 0..8 {
                bools.push(TableValueR::Boolean(true));
            }
            for _ in 0..8 {
                bools.push(TableValueR::Boolean(false));
            }
        }
        check_bools(&bools);
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
            TableValueR::Int(1),
            TableValueR::Bytes(&[1, 2, 3]),
            TableValueR::Int(2),
            TableValueR::Bytes(&[5, 6, 7]),
        ];

        let mut w = RowParquetWriter::open(&index, file, NUM_ROWS).unwrap();
        w.write_rows(RowsView::new(&rows, 2)).unwrap();
        w.close().unwrap();

        let s = ParquetTableStore::new(index, NUM_ROWS);
        let r = s.read_rows(file).unwrap();
        assert_eq!(r.all_values(), rows);
    }

    #[bench]
    fn filter_count(b: &mut Bencher) {
        if let Ok((store, columns_to_read)) = prepare_donors() {
            let mut reader =
                RowParquetReader::open(&store.table, "Donors.parquet", Some(&columns_to_read))
                    .unwrap();

            b.iter(|| {
                let start = SystemTime::now();
                let mut counter = 0;
                for row_group in 0..reader.parquet_reader.num_row_groups() {
                    {
                        let (_, index, _, _) = &reader.column_with_buffer[0];
                        if let Some(Statistics::ByteArray(stats)) = reader
                            .parquet_reader
                            .get_row_group(row_group)
                            .unwrap()
                            .metadata()
                            .column(*index)
                            .statistics()
                        {
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
                println!(
                    "San Francisco count ({:?}): {}",
                    start.elapsed().unwrap(),
                    counter
                );
            });
        }
    }

    /*
    #[bench]
    fn filter_count_using_scan(b: &mut Bencher) {
        if let Ok((store, columns_to_read)) = prepare_donors() {
            b.iter(|| {
                let start = SystemTime::now();
                let reader = store
                    .scan_node(
                        "Donors.parquet",
                        &columns_to_read,
                        Some(Arc::new(|group| {
                            if let Some(Statistics::ByteArray(stats)) = group.column(0).statistics()
                            {
                                let min = stats.min().as_utf8().unwrap();
                                let max = stats.max().as_utf8().unwrap();
                                println!("Min: {}, Max: {}", min, max);
                                min <= "San Francisco" && "San Francisco" <= max
                            } else {
                                false
                            }
                        })),
                    )
                    .unwrap();
                let filter_expr = binary(
                    Arc::new(expressions::Column::new("Donor City")),
                    Operator::Eq,
                    Arc::new(Literal::new(ScalarValue::Utf8(Some(
                        "San Francisco".to_string(),
                    )))),
                    reader.schema().as_ref(),
                )
                .unwrap();
                let filter = Arc::new(FilterExec::try_new(filter_expr, reader).unwrap());
                let aggregate = HashAggregateExec::try_new(
                    AggregateMode::Partial,
                    vec![],
                    vec![Arc::new(Count::new(
                        Arc::new(expressions::Column::new("Donor City")),
                        "count".to_string(),
                        DataType::UInt64,
                    ))],
                    filter,
                )
                .unwrap();
                let batch = block_on(async {
                    aggregate
                        .execute(0)
                        .await
                        .unwrap()
                        .next()
                        .await
                        .unwrap()
                        .unwrap()
                });
                let result = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                println!(
                    "San Francisco count ({:?}): {}",
                    start.elapsed().unwrap(),
                    result.value(0)
                );
            });
        }
    }
     */

    fn prepare_donors() -> Result<(ParquetTableStore, Vec<Column>), io::Error> {
        let store = ParquetTableStore {
            table: Index::try_new(
                "donors".to_string(),
                1,
                vec![
                    Column::new("Donor City".to_string(), ColumnType::String, 0),
                    Column::new("Donor ID".to_string(), ColumnType::String, 1),
                    Column::new("Donor State".to_string(), ColumnType::String, 2),
                    Column::new("Donor Is Teacher".to_string(), ColumnType::String, 3),
                    Column::new("Donor Zip".to_string(), ColumnType::String, 4),
                ],
                6,
            )
            .unwrap(),
            row_group_size: 16384,
        };

        let column_mapping = vec![1, 0, 2, 3, 4];

        let donors = File::open("Donors.csv")?;
        let mut rdr = ReaderBuilder::new().from_reader(BufReader::new(donors));

        let mut index = 0;
        let mut to_merge = Vec::new();
        let column_count = store.table.get_columns().len();
        let mut current_file = None;

        if fs::metadata("Donors.parquet").is_err() {
            for record in rdr.records() {
                let r = record.unwrap();
                let mut values = Vec::with_capacity(column_count);
                for c in store.table.get_columns() {
                    values.push(TableValue::String(
                        r[column_mapping[c.get_index()]].to_string(),
                    ));
                }
                to_merge.push(Row::new(values));
                index += 1;
                if index % 500000 == 0 {
                    current_file =
                        merge_for_bench(&store, &mut index, &mut to_merge, &current_file);
                }
            }

            merge_for_bench(&store, &mut index, &mut to_merge, &current_file);
        }

        let columns_to_read = vec![store.table.get_columns()[0].clone()];
        Ok((store, columns_to_read))
    }

    fn merge_for_bench(
        store: &ParquetTableStore,
        index: &mut i32,
        mut to_merge: &mut Vec<Row>,
        current_file: &Option<String>,
    ) -> Option<String> {
        println!("Merging {}", index);
        let dest_file = current_file
            .as_ref()
            .map(|f| format!("{}.new", f))
            .unwrap_or("Donors.parquet".to_string());
        let mut tmp = Vec::new();
        swap(&mut tmp, &mut to_merge);
        tmp.sort_by(|a, b| a.sort_key(2).cmp(&b.sort_key(2)));
        store
            .merge_rows_from_heap(
                current_file.as_ref().map(|s| s.as_str()),
                vec![dest_file],
                tmp,
                2,
            )
            .unwrap();
        fs::rename("Donors.parquet.new", "Donors.parquet").unwrap();
        Some("Donors.parquet".to_string())
    }
}
