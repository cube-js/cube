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
            ColumnType::Timestamp => "timestamp without time zone".to_string(),
            ColumnType::Int64 => "bigint".to_string(),
            ColumnType::Double => "numeric".to_string(),
            ColumnType::Boolean => "boolean".to_string(),
            _ => "text".to_string(),
        }
    }

    fn get_udt_name(&self) -> String {
        match self.get_column_type() {
            ColumnType::Timestamp => "timestamp".to_string(),
            ColumnType::Int64 => "int8".to_string(),
            ColumnType::Double => "numeric".to_string(),
            ColumnType::Boolean => "bool".to_string(),
            _ => "text".to_string(),
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
            ColumnType::Int64 => Some(64),
            _ => None,
        }
    }

    fn numeric_precision_radix(&self) -> Option<u32> {
        match self.get_column_type() {
            ColumnType::Int64 => Some(2),
            ColumnType::Double => Some(10),
            _ => None,
        }
    }

    fn numeric_scale(&self) -> Option<u32> {
        match self.get_column_type() {
            ColumnType::Int64 => Some(0),
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
