use crate::{sql::ColumnType, transport::CubeColumn};

pub trait CubeColumnPostgresExt {
    fn get_data_type(&self) -> String;
    fn get_udt_name(&self) -> String;
    fn is_nullable(&self) -> String;
    fn udt_schema(&self) -> String;
    fn get_numeric_precision(&self) -> Option<u32>;
    fn numeric_precision_radix(&self) -> Option<u32>;
    fn numeric_scale(&self) -> Option<u32>;
    fn datetime_precision(&self) -> Option<u32>;
    fn char_octet_length(&self) -> Option<u32>;
}

impl CubeColumnPostgresExt for CubeColumn {
    fn get_data_type(&self) -> String {
        match self.get_column_type() {
            ColumnType::String => "text".to_string(),
            ColumnType::VarStr => "character varying".to_string(),
            ColumnType::Double => "double precision".to_string(),
            ColumnType::Boolean => "boolean".to_string(),
            ColumnType::Int8 => "smallint".to_string(),
            ColumnType::Int32 => "integer".to_string(),
            ColumnType::Int64 => "bigint".to_string(),
            ColumnType::Blob => "bytea".to_string(),
            ColumnType::Date(_) => "date".to_string(),
            ColumnType::Interval(_) => "interval".to_string(),
            ColumnType::Timestamp => "timestamp without time zone".to_string(),
            ColumnType::Decimal(_, _) => "numeric".to_string(),
            ColumnType::List(field) => {
                let base_type = match field.data_type() {
                    datafusion::arrow::datatypes::DataType::Binary => "bytea",
                    datafusion::arrow::datatypes::DataType::Boolean => "boolean",
                    datafusion::arrow::datatypes::DataType::Utf8 => "text",
                    datafusion::arrow::datatypes::DataType::Int16 => "smallint",
                    datafusion::arrow::datatypes::DataType::Int32 => "integer",
                    datafusion::arrow::datatypes::DataType::Int64 => "bigint",
                    datafusion::arrow::datatypes::DataType::UInt16 => "smallint",
                    datafusion::arrow::datatypes::DataType::UInt32 => "integer",
                    datafusion::arrow::datatypes::DataType::UInt64 => "bigint",
                    _ => "text",
                };
                format!("{}[]", base_type)
            }
        }
    }

    fn get_udt_name(&self) -> String {
        match self.get_column_type() {
            ColumnType::String => "text".to_string(),
            ColumnType::VarStr => "varchar".to_string(),
            ColumnType::Double => "float8".to_string(),
            ColumnType::Boolean => "bool".to_string(),
            ColumnType::Int8 => "int2".to_string(),
            ColumnType::Int32 => "int4".to_string(),
            ColumnType::Int64 => "int8".to_string(),
            ColumnType::Blob => "bytea".to_string(),
            ColumnType::Date(_) => "date".to_string(),
            ColumnType::Interval(_) => "interval".to_string(),
            ColumnType::Timestamp => "timestamp".to_string(),
            ColumnType::Decimal(_, _) => "numeric".to_string(),
            ColumnType::List(field) => {
                let base_type = match field.data_type() {
                    datafusion::arrow::datatypes::DataType::Binary => "bytea",
                    datafusion::arrow::datatypes::DataType::Boolean => "bool",
                    datafusion::arrow::datatypes::DataType::Utf8 => "text",
                    datafusion::arrow::datatypes::DataType::Int16 => "int2",
                    datafusion::arrow::datatypes::DataType::Int32 => "int4",
                    datafusion::arrow::datatypes::DataType::Int64 => "int8",
                    datafusion::arrow::datatypes::DataType::UInt16 => "int2",
                    datafusion::arrow::datatypes::DataType::UInt32 => "int4",
                    datafusion::arrow::datatypes::DataType::UInt64 => "int8",
                    _ => "text",
                };
                format!("_{}", base_type)
            }
        }
    }

    fn is_nullable(&self) -> String {
        if self.sql_can_be_null() {
            return "YES".to_string();
        } else {
            return "NO".to_string();
        }
    }

    fn udt_schema(&self) -> String {
        return "pg_catalog".to_string();
    }

    fn get_numeric_precision(&self) -> Option<u32> {
        match self.get_column_type() {
            // In PostgreSQL, integer types (INT2, INT4, INT8) don't have numeric_precision
            // Only NUMERIC/DECIMAL types do
            ColumnType::Decimal(precision, _) => Some(precision as u32),
            // Double precision (float8) has binary precision of 53 bits
            ColumnType::Double => Some(53),
            _ => None,
        }
    }

    fn numeric_precision_radix(&self) -> Option<u32> {
        match self.get_column_type() {
            // DECIMAL/NUMERIC types use base 10
            ColumnType::Decimal(_, _) => Some(10),
            // Float types use base 2 (binary)
            ColumnType::Double => Some(2),
            _ => None,
        }
    }

    fn numeric_scale(&self) -> Option<u32> {
        match self.get_column_type() {
            // Only DECIMAL/NUMERIC types have a scale
            ColumnType::Decimal(_, scale) => Some(scale as u32),
            _ => None,
        }
    }

    fn datetime_precision(&self) -> Option<u32> {
        match self.get_column_type() {
            ColumnType::Timestamp => Some(6),
            _ => None,
        }
    }

    fn char_octet_length(&self) -> Option<u32> {
        match self.get_column_type() {
            ColumnType::String => Some(1073741824),
            _ => None,
        }
    }
}
