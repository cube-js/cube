use std::fmt::{self, Debug, Formatter};

use chrono::{SecondsFormat, TimeZone, Utc};
use datafusion::arrow::array::{
    Array, Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    UInt32Array,
};
use datafusion::arrow::{
    array::{BooleanArray, TimestampNanosecondArray, UInt64Array},
    datatypes::{DataType, TimeUnit},
    record_batch::RecordBatch,
};
use log::{error, warn};
use msql_srv::ColumnType;

use crate::{compile::builder::CompiledQueryFieldMeta, CubeError};

#[derive(Clone, Debug)]
pub struct Column {
    name: String,
    column_type: ColumnType,
}

impl Column {
    pub fn new(name: String, column_type: ColumnType) -> Column {
        Column { name, column_type }
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_type(&self) -> ColumnType {
        self.column_type
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    values: Vec<TableValue>,
}

impl Row {
    pub fn new(values: Vec<TableValue>) -> Row {
        Row { values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn values(&self) -> &Vec<TableValue> {
        &self.values
    }

    pub fn push(&mut self, val: TableValue) {
        self.values.push(val);
    }

    pub fn hydrate_from_response(
        meta: &Vec<CompiledQueryFieldMeta>,
        record: &serde_json::Map<String, serde_json::Value>,
    ) -> Row {
        let mut values: Vec<TableValue> = vec![];

        for column_meta in meta {
            let value = record.get(column_meta.column_from.as_str()).unwrap();

            match column_meta.column_type {
                ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_LONGLONG => {
                    let column_value = match value {
                        serde_json::Value::Null => TableValue::Null,
                        serde_json::Value::Number(number) => match number.as_i64() {
                            Some(v) => TableValue::Int64(v),
                            None => TableValue::Null,
                        },
                        serde_json::Value::String(s) => match s.parse::<i64>() {
                            Ok(v) => TableValue::Int64(v),
                            Err(error) => {
                                warn!("Unable to parse value as i64: {}", error.to_string());

                                TableValue::Null
                            }
                        },
                        v => {
                            error!(
                                "Unable to map value {:?} to MYSQL_TYPE_LONG (returning null)",
                                v
                            );

                            TableValue::Null
                        }
                    };

                    values.push(column_value);
                }
                ColumnType::MYSQL_TYPE_DOUBLE => {
                    let column_value = match value {
                        serde_json::Value::Null => TableValue::Null,
                        serde_json::Value::Number(number) => match number.as_f64() {
                            Some(v) => TableValue::Float64(v),
                            None => TableValue::Null,
                        },
                        serde_json::Value::String(s) => match s.parse::<f64>() {
                            Ok(v) => TableValue::Float64(v),
                            Err(error) => {
                                warn!("Unable to parse value as f64: {}", error.to_string());

                                TableValue::Null
                            }
                        },
                        v => {
                            error!(
                                "Unable to map value {:?} to MYSQL_TYPE_DOUBLE (returning null)",
                                v
                            );

                            TableValue::Null
                        }
                    };

                    values.push(column_value);
                }
                ColumnType::MYSQL_TYPE_STRING => {
                    let column_value = match value {
                        serde_json::Value::Null => TableValue::Null,
                        serde_json::Value::String(v) => TableValue::String(v.clone()),
                        serde_json::Value::Bool(v) => TableValue::Boolean(*v),
                        serde_json::Value::Number(v) => TableValue::String(v.to_string()),
                        v => {
                            error!(
                                "Unable to map value {:?} to MYSQL_TYPE_STRING (returning null)",
                                v
                            );

                            TableValue::Null
                        }
                    };

                    values.push(column_value);
                }
                ColumnType::MYSQL_TYPE_TINY => {
                    let column_value = match value {
                        serde_json::Value::Null => TableValue::Null,
                        serde_json::Value::Bool(v) => TableValue::Boolean(*v),
                        v => {
                            error!(
                                "Unable to map value {:?} to MYSQL_TYPE_TINY (boolean) (returning null)",
                                v
                            );

                            TableValue::Null
                        }
                    };

                    values.push(column_value);
                }
                _ => panic!(
                    "Unsupported column_type in hydration: {:?}",
                    column_meta.column_type
                ),
            }
        }

        Self::new(values)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum TableValue {
    Null,
    String(String),
    Int64(i64),
    Boolean(bool),
    Float64(f64),
    Timestamp(TimestampValue),
}

pub struct DataFrame {
    columns: Vec<Column>,
    data: Vec<Row>,
}

impl DataFrame {
    pub fn new(columns: Vec<Column>, data: Vec<Row>) -> DataFrame {
        DataFrame { columns, data }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn get_rows(&self) -> &Vec<Row> {
        &self.data
    }

    pub fn mut_rows(&mut self) -> &mut Vec<Row> {
        &mut self.data
    }

    pub fn into_rows(self) -> Vec<Row> {
        self.data
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TimestampValue {
    unix_nano: i64,
}

impl TimestampValue {
    pub fn new(mut unix_nano: i64) -> TimestampValue {
        // This is a hack to workaround a mismatch between on-disk and in-memory representations.
        // We use millisecond precision on-disk.
        unix_nano -= unix_nano % 1000;
        TimestampValue { unix_nano }
    }

    pub fn get_time_stamp(&self) -> i64 {
        self.unix_nano
    }
}

impl Debug for TimestampValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimestampValue")
            .field("unix_nano", &self.unix_nano)
            .field("str", &self.to_string())
            .finish()
    }
}

impl ToString for TimestampValue {
    fn to_string(&self) -> String {
        Utc.timestamp_nanos(self.unix_nano)
            .to_rfc3339_opts(SecondsFormat::Millis, true)
    }
}

macro_rules! convert_array_cast_native {
    ($V: expr, (Vec<u8>)) => {{
        $V.to_vec()
    }};
    ($V: expr, $T: ty) => {{
        $V as $T
    }};
}

macro_rules! convert_array {
    ($ARRAY:expr, $NUM_ROWS:expr, $ROWS:expr, $ARRAY_TYPE: ident, $TABLE_TYPE: ident, $NATIVE: tt) => {{
        let a = $ARRAY.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        for i in 0..$NUM_ROWS {
            $ROWS[i].push(if a.is_null(i) {
                TableValue::Null
            } else {
                TableValue::$TABLE_TYPE(convert_array_cast_native!(a.value(i), $NATIVE))
            });
        }
    }};
}

pub fn arrow_to_column_type(arrow_type: DataType) -> Result<ColumnType, CubeError> {
    match arrow_type {
        DataType::Binary => Ok(ColumnType::MYSQL_TYPE_BLOB),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(ColumnType::MYSQL_TYPE_STRING),
        DataType::Timestamp(_, _) => Ok(ColumnType::MYSQL_TYPE_STRING),
        DataType::Float16 | DataType::Float64 => Ok(ColumnType::MYSQL_TYPE_DOUBLE),
        DataType::Boolean => Ok(ColumnType::MYSQL_TYPE_TINY),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Ok(ColumnType::MYSQL_TYPE_LONGLONG),
        x => Err(CubeError::internal(format!("unsupported type {:?}", x))),
    }
}

pub fn batch_to_dataframe(batches: &Vec<RecordBatch>) -> Result<DataFrame, CubeError> {
    let mut cols = vec![];
    let mut all_rows = vec![];

    for batch in batches.iter() {
        if cols.is_empty() {
            let schema = batch.schema().clone();
            for (_i, field) in schema.fields().iter().enumerate() {
                cols.push(Column::new(
                    field.name().clone(),
                    arrow_to_column_type(field.data_type().clone())?,
                ));
            }
        }
        if batch.num_rows() == 0 {
            continue;
        }
        let mut rows = vec![];

        for _ in 0..batch.num_rows() {
            rows.push(Row::new(Vec::with_capacity(batch.num_columns())));
        }

        for column_index in 0..batch.num_columns() {
            let array = batch.column(column_index);
            let num_rows = batch.num_rows();
            match array.data_type() {
                DataType::Int32 => convert_array!(array, num_rows, rows, Int32Array, Int64, i64),
                DataType::UInt32 => convert_array!(array, num_rows, rows, UInt32Array, Int64, i64),
                DataType::UInt64 => convert_array!(array, num_rows, rows, UInt64Array, Int64, i64),
                DataType::Int64 => convert_array!(array, num_rows, rows, Int64Array, Int64, i64),
                DataType::Float64 => {
                    let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            let decimal = a.value(i) as f64;
                            TableValue::Float64(decimal)
                        });
                    }
                }
                DataType::Utf8 => {
                    let a = array.as_any().downcast_ref::<StringArray>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::String(a.value(i).to_string())
                        });
                    }
                }
                DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::Timestamp(TimestampValue::new(a.value(i) * 1000_i64))
                        });
                    }
                }
                DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::Timestamp(TimestampValue::new(a.value(i)))
                        });
                    }
                }
                DataType::Boolean => {
                    let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::Boolean(a.value(i))
                        });
                    }
                }
                x => panic!("Unsupported data type: {:?}", x),
            }
        }
        all_rows.append(&mut rows);
    }

    Ok(DataFrame::new(cols, all_rows))
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn test_hydrate_from_response() {
        let meta = vec![
            CompiledQueryFieldMeta {
                column_from: "KibanaSampleDataEcommerce.count".to_string(),
                column_to: "count".to_string(),
                column_type: ColumnType::MYSQL_TYPE_LONGLONG,
            },
            CompiledQueryFieldMeta {
                column_from: "KibanaSampleDataEcommerce.maxPrice".to_string(),
                column_to: "maxPrice".to_string(),
                column_type: ColumnType::MYSQL_TYPE_DOUBLE,
            },
            CompiledQueryFieldMeta {
                column_from: "KibanaSampleDataEcommerce.isBool".to_string(),
                column_to: "isBool".to_string(),
                column_type: ColumnType::MYSQL_TYPE_TINY,
            },
        ];

        let response = r#"
            [
                {"KibanaSampleDataEcommerce.count": null, "KibanaSampleDataEcommerce.maxPrice": null, "KibanaSampleDataEcommerce.isBool": null},
                {"KibanaSampleDataEcommerce.count": 5, "KibanaSampleDataEcommerce.maxPrice": 5.05, "KibanaSampleDataEcommerce.isBool": true},
                {"KibanaSampleDataEcommerce.count": "5", "KibanaSampleDataEcommerce.maxPrice": "5.05", "KibanaSampleDataEcommerce.isBool": false}
            ]
        "#;
        let data = serde_json::from_str::<Vec<serde_json::Value>>(&response).unwrap();
        assert_eq!(
            Row::hydrate_from_response(&meta, data[0].as_object().unwrap()),
            Row::new(vec![TableValue::Null, TableValue::Null, TableValue::Null])
        );
        assert_eq!(
            Row::hydrate_from_response(&meta, data[1].as_object().unwrap()),
            Row::new(vec![
                TableValue::Int64(5),
                TableValue::Float64(5.05),
                TableValue::Boolean(true)
            ])
        );
        assert_eq!(
            Row::hydrate_from_response(&meta, data[2].as_object().unwrap()),
            Row::new(vec![
                TableValue::Int64(5),
                TableValue::Float64(5.05),
                TableValue::Boolean(false)
            ])
        );
    }
}
