use cubeclient::models::{V1CubeMeta, V1CubeMetaDimension, V1CubeMetaMeasure, V1CubeMetaSegment};
use datafusion::arrow::datatypes::{DataType, TimeUnit};

use crate::sql::ColumnType;

pub trait V1CubeMetaMeasureExt {
    fn get_real_name(&self) -> String;

    fn is_same_agg_type(&self, expect_agg_type: &str) -> bool;

    fn get_sql_type(&self) -> ColumnType;
}

impl V1CubeMetaMeasureExt for V1CubeMetaMeasure {
    fn get_real_name(&self) -> String {
        let (_, dimension_name) = self.name.split_once('.').unwrap();

        dimension_name.to_string()
    }

    fn is_same_agg_type(&self, expect_agg_type: &str) -> bool {
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

    fn get_sql_type(&self) -> ColumnType {
        let from_type = match &self._type.to_lowercase().as_str() {
            &"number" => ColumnType::Double,
            &"boolean" => ColumnType::Boolean,
            _ => ColumnType::String,
        };

        match &self.agg_type {
            Some(agg_type) => match agg_type.as_str() {
                "count" => ColumnType::Int64,
                "countDistinct" => ColumnType::Int64,
                "countDistinctApprox" => ColumnType::Int64,
                "sum" => ColumnType::Double,
                "avg" => ColumnType::Double,
                "min" => ColumnType::Double,
                "max" => ColumnType::Double,
                "runningTotal" => ColumnType::Double,
                _ => from_type,
            },
            _ => from_type,
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

    fn sql_can_be_null(&self) -> bool;

    fn get_sql_type(&self) -> ColumnType;

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

    fn sql_can_be_null(&self) -> bool {
        // @todo Possible not null?
        true
    }

    fn get_sql_type(&self) -> ColumnType {
        match self._type.to_lowercase().as_str() {
            "time" => ColumnType::Timestamp,
            "number" => ColumnType::Double,
            "boolean" => ColumnType::Boolean,
            _ => ColumnType::String,
        }
    }
}

#[derive(Debug)]
pub struct CubeColumn {
    name: String,
    column_type: ColumnType,
    can_be_null: bool,
}

impl CubeColumn {
    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn sql_can_be_null(&self) -> bool {
        self.can_be_null
    }

    pub fn get_column_type(&self) -> ColumnType {
        self.column_type
    }
}

pub trait V1CubeMetaExt {
    fn get_columns(&self) -> Vec<CubeColumn>;

    fn get_scan_columns(&self) -> Vec<CubeColumn>;

    fn contains_member(&self, member_name: &str) -> bool;

    fn lookup_dimension(&self, member_name: &str) -> Option<&V1CubeMetaDimension>;

    fn df_data_type(&self, member_name: &str) -> Option<DataType>;

    fn member_type(&self, member_name: &str) -> Option<MemberType>;
}

pub enum MemberType {
    String,
    Number,
    Time,
    Boolean,
}

impl V1CubeMetaExt for V1CubeMeta {
    fn get_columns(&self) -> Vec<CubeColumn> {
        let mut columns = Vec::new();

        for measure in &self.measures {
            columns.push(CubeColumn {
                name: measure.get_real_name(),
                column_type: measure.get_sql_type(),
                can_be_null: false,
            });
        }

        for dimension in &self.dimensions {
            columns.push(CubeColumn {
                name: dimension.get_real_name(),
                column_type: dimension.get_sql_type(),
                can_be_null: dimension.sql_can_be_null(),
            });
        }

        for segment in &self.segments {
            columns.push(CubeColumn {
                name: segment.get_real_name(),
                column_type: ColumnType::Blob,
                can_be_null: false,
            });
        }

        columns
    }

    fn get_scan_columns(&self) -> Vec<CubeColumn> {
        let mut columns = Vec::new();

        for measure in &self.measures {
            columns.push(CubeColumn {
                name: measure.get_real_name(),
                column_type: measure.get_sql_type(),
                can_be_null: false,
            });
        }

        for dimension in &self.dimensions {
            columns.push(CubeColumn {
                name: dimension.get_real_name(),
                column_type: dimension.get_sql_type(),
                can_be_null: dimension.sql_can_be_null(),
            });
        }

        columns
    }

    fn contains_member(&self, member_name: &str) -> bool {
        self.measures
            .iter()
            .any(|m| m.name.eq_ignore_ascii_case(member_name))
            || self
                .dimensions
                .iter()
                .any(|m| m.name.eq_ignore_ascii_case(member_name))
    }

    fn lookup_dimension(&self, member_name: &str) -> Option<&V1CubeMetaDimension> {
        self.dimensions
            .iter()
            .find(|m| m.name.eq_ignore_ascii_case(member_name))
    }

    fn df_data_type(&self, member_name: &str) -> Option<DataType> {
        if let Some(m) = self
            .measures
            .iter()
            .find(|m| m.name.eq_ignore_ascii_case(member_name))
        {
            return Some(df_data_type_by_column_type(m.get_sql_type()));
        }

        if let Some(m) = self
            .dimensions
            .iter()
            .find(|m| m.name.eq_ignore_ascii_case(member_name))
        {
            return Some(df_data_type_by_column_type(m.get_sql_type()));
        }

        if let Some(_) = self
            .segments
            .iter()
            .find(|m| m.name.eq_ignore_ascii_case(member_name))
        {
            return Some(df_data_type_by_column_type(ColumnType::Int8));
        }
        None
    }

    fn member_type(&self, member_name: &str) -> Option<MemberType> {
        if let Some(_) = self
            .measures
            .iter()
            .find(|m| m.name.eq_ignore_ascii_case(member_name))
        {
            return Some(MemberType::Number);
        }

        if let Some(dimension) = self
            .dimensions
            .iter()
            .find(|m| m.name.eq_ignore_ascii_case(member_name))
        {
            return Some(match dimension._type.as_str() {
                "number" => MemberType::Number,
                "boolean" => MemberType::Boolean,
                "string" => MemberType::String,
                "time" => MemberType::Time,
                x => panic!("Unexpected dimension type: {}", x),
            });
        }

        if let Some(_) = self
            .segments
            .iter()
            .find(|m| m.name.eq_ignore_ascii_case(member_name))
        {
            return Some(MemberType::Boolean);
        }
        None
    }
}

pub fn df_data_type_by_column_type(column_type: ColumnType) -> DataType {
    match column_type {
        ColumnType::Int32 | ColumnType::Int64 | ColumnType::Int8 => DataType::Int64,
        ColumnType::String => DataType::Utf8,
        ColumnType::Double => DataType::Float64,
        ColumnType::Boolean => DataType::Boolean,
        ColumnType::Timestamp => DataType::Timestamp(TimeUnit::Nanosecond, None),
        _ => panic!("Unimplemented support for {:?}", column_type),
    }
}
