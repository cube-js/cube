use cubeclient::models::{V1CubeMeta, V1CubeMetaDimension, V1CubeMetaMeasure, V1CubeMetaSegment};
use msql_srv::ColumnType as MySqlColumnType;

use crate::sql::ColumnType;

pub trait V1CubeMetaMeasureExt {
    fn get_real_name(&self) -> String;

    fn is_same_agg_type(&self, expect_agg_type: &String) -> bool;

    fn get_mysql_type(&self) -> MySqlColumnType;

    fn get_sql_type(&self) -> ColumnType;

    /// varchar(128)
    fn get_column_type(&self) -> String;

    /// varchar
    fn get_data_type(&self) -> String;
}

impl V1CubeMetaMeasureExt for V1CubeMetaMeasure {
    fn get_real_name(&self) -> String {
        let (_, dimension_name) = self.name.split_once('.').unwrap();

        dimension_name.to_string()
    }

    fn is_same_agg_type(&self, expect_agg_type: &String) -> bool {
        if self.agg_type.is_some() {
            if expect_agg_type.eq(&"countDistinct".to_string()) {
                let agg_type = self.agg_type.as_ref().unwrap();

                agg_type.eq(&"countDistinct".to_string())
                    || agg_type.eq(&"countDistinctApprox".to_string())
            } else {
                self.agg_type.as_ref().unwrap().eq(expect_agg_type)
            }
        } else {
            false
        }
    }

    fn get_mysql_type(&self) -> MySqlColumnType {
        let from_type = match &self._type.to_lowercase().as_str() {
            &"number" => MySqlColumnType::MYSQL_TYPE_DOUBLE,
            &"boolean" => MySqlColumnType::MYSQL_TYPE_TINY,
            _ => MySqlColumnType::MYSQL_TYPE_STRING,
        };

        match &self.agg_type {
            Some(agg_type) => match agg_type.as_str() {
                "count" => MySqlColumnType::MYSQL_TYPE_LONGLONG,
                _ => from_type,
            },
            _ => from_type,
        }
    }

    fn get_sql_type(&self) -> ColumnType {
        let from_type = match &self._type.to_lowercase().as_str() {
            &"number" => ColumnType::Double,
            &"boolean" => ColumnType::Int8,
            _ => ColumnType::String,
        };

        match &self.agg_type {
            Some(agg_type) => match agg_type.as_str() {
                "count" => ColumnType::Int64,
                _ => from_type,
            },
            _ => from_type,
        }
    }

    fn get_column_type(&self) -> String {
        match self._type.to_lowercase().as_str() {
            _ => "int".to_string(),
        }
    }

    fn get_data_type(&self) -> String {
        match self._type.to_lowercase().as_str() {
            _ => "int".to_string(),
        }
    }
}

pub trait V1CubeMetaSegmentExt {
    fn get_real_name(&self) -> String;
}

impl V1CubeMetaSegmentExt for V1CubeMetaSegment {
    fn get_real_name(&self) -> String {
        let (_, segment_name) = self.name.split_once('.').unwrap();

        segment_name.to_string()
    }
}

pub trait V1CubeMetaDimensionExt {
    fn get_real_name(&self) -> String;

    fn mysql_can_be_null(&self) -> bool;

    fn get_column_type(&self) -> String;

    fn get_data_type(&self) -> String;

    fn is_time(&self) -> bool;
}

impl V1CubeMetaDimensionExt for V1CubeMetaDimension {
    fn get_real_name(&self) -> String {
        let (_, dimension_name) = self.name.split_once('.').unwrap();

        dimension_name.to_string()
    }

    fn is_time(&self) -> bool {
        self._type.to_lowercase().eq("time")
    }

    fn mysql_can_be_null(&self) -> bool {
        // @todo Possible not null?
        true
    }

    fn get_column_type(&self) -> String {
        match self._type.to_lowercase().as_str() {
            "time" => "datetime".to_string(),
            _ => "varchar(255)".to_string(),
        }
    }

    fn get_data_type(&self) -> String {
        match self._type.to_lowercase().as_str() {
            "time" => "datetime".to_string(),
            _ => "varchar".to_string(),
        }
    }
}

#[derive(Debug)]
pub struct CubeColumn {
    name: String,
    data_type: String,
    column_type: String,
    can_be_null: bool,
}

impl CubeColumn {
    pub fn get_name(&self) -> &String {
        &self.name
    }

    /// varchar
    pub fn get_data_type(&self) -> &String {
        &self.data_type
    }

    /// varchar(97)
    pub fn get_column_type(&self) -> &String {
        &self.column_type
    }

    pub fn sql_can_be_null(&self) -> bool {
        self.can_be_null
    }
}

pub trait V1CubeMetaExt {
    fn get_columns(&self) -> Vec<CubeColumn>;
}

impl V1CubeMetaExt for V1CubeMeta {
    fn get_columns(&self) -> Vec<CubeColumn> {
        let mut columns = Vec::new();

        for measure in &self.measures {
            columns.push(CubeColumn {
                name: measure.get_real_name(),
                data_type: measure.get_data_type(),
                column_type: measure.get_column_type(),
                can_be_null: false,
            });
        }

        for dimension in &self.dimensions {
            columns.push(CubeColumn {
                name: dimension.get_real_name(),
                data_type: dimension.get_data_type(),
                column_type: dimension.get_column_type(),
                can_be_null: dimension.mysql_can_be_null(),
            });
        }

        for segment in &self.segments {
            columns.push(CubeColumn {
                name: segment.get_real_name(),
                column_type: "boolean".to_string(),
                data_type: "boolean".to_string(),
                can_be_null: false,
            });
        }

        columns
    }
}
