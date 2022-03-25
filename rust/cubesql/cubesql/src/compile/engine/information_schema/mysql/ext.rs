use crate::{sql::ColumnType, transport::CubeColumn};

pub trait CubeColumnMySqlExt {
    fn get_data_type(&self) -> String;
    fn get_mysql_column_type(&self) -> String;
}

impl CubeColumnMySqlExt for CubeColumn {
    fn get_data_type(&self) -> String {
        match self.get_column_type() {
            ColumnType::Timestamp => "datetime".to_string(),
            ColumnType::Int64 => "int".to_string(),
            ColumnType::Double => "numeric".to_string(),
            ColumnType::Blob => "boolean".to_string(),
            _ => "varchar".to_string(),
        }
    }

    fn get_mysql_column_type(&self) -> String {
        match self.get_column_type() {
            ColumnType::Timestamp => "datetime".to_string(),
            ColumnType::Int64 => "int".to_string(),
            ColumnType::Double => "numeric".to_string(),
            ColumnType::Blob => "boolean".to_string(),
            _ => "varchar(255)".to_string(),
        }
    }
}
