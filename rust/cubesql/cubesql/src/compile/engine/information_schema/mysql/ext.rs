use crate::{sql::ColumnType, transport::CubeColumn};

pub trait CubeColumnMySqlExt {
    fn get_data_type(&self) -> &'static str;
    fn get_mysql_column_type(&self) -> &'static str;
}

impl CubeColumnMySqlExt for CubeColumn {
    fn get_data_type(&self) -> &'static str {
        match self.get_column_type() {
            ColumnType::Timestamp => "datetime",
            ColumnType::Int64 => "int",
            ColumnType::Double => "numeric",
            // bool, boolean is an alias for tinyint(1)
            ColumnType::Boolean => "tinyint(1)",
            _ => "varchar",
        }
    }

    fn get_mysql_column_type(&self) -> &'static str {
        match self.get_column_type() {
            ColumnType::Timestamp => "datetime",
            ColumnType::Int64 => "int",
            ColumnType::Double => "numeric",
            // bool, boolean is an alias for tinyint(1)
            ColumnType::Boolean => "tinyint(1)",
            _ => "varchar(255)",
        }
    }
}
