use datafusion::arrow::datatypes::{DataType, TimeUnit};

use crate::{
    sql::ColumnType,
    transport::{CubeMeta, CubeMetaDimension, CubeMetaMeasure, CubeMetaSegment},
};

pub trait V1CubeMetaMeasureExt {
    fn get_real_name(&self) -> String;

    fn is_same_agg_type(&self, expect_agg_type: &str, disable_strict_match: bool) -> bool;

    fn allow_replace_agg_type(&self, query_agg_type: &str, disable_strict_match: bool) -> bool;

    fn allow_add_filter(&self, query_agg_type: Option<&str>) -> bool;

    fn get_sql_type(&self) -> ColumnType;
}

impl V1CubeMetaMeasureExt for CubeMetaMeasure {
    fn get_real_name(&self) -> String {
        let (_, dimension_name) = self.name.split_once('.').unwrap();

        dimension_name.to_string()
    }

    fn is_same_agg_type(&self, expect_agg_type: &str, disable_strict_match: bool) -> bool {
        if disable_strict_match {
            return true;
        }
        let Some(agg_type) = &self.agg_type else {
            return false;
        };
        match expect_agg_type {
            "countDistinct" => {
                agg_type == "countDistinct"
                    || agg_type == "countDistinctApprox"
                    || agg_type == "number"
            }
            "sum" => agg_type == "sum" || agg_type == "count" || agg_type == "number",
            "min" | "max" => {
                agg_type == "number"
                    || agg_type == "string"
                    || agg_type == "time"
                    || agg_type == "boolean"
                    || agg_type == expect_agg_type
            }
            _ => agg_type == "number" || agg_type == expect_agg_type,
        }
    }

    // This should be aligned with BaseMeasure.preparePatchedMeasure
    // See packages/cubejs-schema-compiler/src/adapter/BaseMeasure.ts:16
    fn allow_replace_agg_type(&self, query_agg_type: &str, disable_strict_match: bool) -> bool {
        if disable_strict_match {
            return true;
        }
        let Some(agg_type) = &self.agg_type else {
            return false;
        };

        match (agg_type.as_str(), query_agg_type) {
            (
                "sum" | "avg" | "min" | "max",
                "sum" | "avg" | "min" | "max" | "count_distinct" | "count_distinct_approx",
            ) => true,

            (
                "count_distinct" | "count_distinct_approx",
                "count_distinct" | "count_distinct_approx",
            ) => true,

            _ => false,
        }
    }

    // This should be aligned with BaseMeasure.preparePatchedMeasure
    // See packages/cubejs-schema-compiler/src/adapter/BaseMeasure.ts:16
    fn allow_add_filter(&self, query_agg_type: Option<&str>) -> bool {
        let Some(agg_type) = &self.agg_type else {
            return false;
        };

        let agg_type = match query_agg_type {
            Some(query_agg_type) => query_agg_type,
            None => agg_type,
        };

        match agg_type {
            "sum"
            | "avg"
            | "min"
            | "max"
            | "count"
            | "count_distinct"
            | "count_distinct_approx" => true,
            _ => false,
        }
    }

    fn get_sql_type(&self) -> ColumnType {
        let from_type = match &self.r#type.to_lowercase().as_str() {
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

impl V1CubeMetaSegmentExt for CubeMetaSegment {
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

impl V1CubeMetaDimensionExt for CubeMetaDimension {
    fn get_real_name(&self) -> String {
        let (_, dimension_name) = self.name.split_once('.').unwrap();

        dimension_name.to_string()
    }

    fn is_time(&self) -> bool {
        self.r#type.to_lowercase().eq("time")
    }

    fn sql_can_be_null(&self) -> bool {
        // @todo Possible not null?
        true
    }

    fn get_sql_type(&self) -> ColumnType {
        match self.r#type.to_lowercase().as_str() {
            "time" => ColumnType::Timestamp,
            "number" => ColumnType::Double,
            "boolean" => ColumnType::Boolean,
            _ => ColumnType::String,
        }
    }
}

#[derive(Debug)]
pub struct CubeColumn {
    member_name: String,
    name: String,
    description: Option<String>,
    column_type: ColumnType,
    can_be_null: bool,
}

impl CubeColumn {
    pub fn member_name(&self) -> &String {
        &self.member_name
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_description(&self) -> &Option<String> {
        &self.description
    }

    pub fn sql_can_be_null(&self) -> bool {
        self.can_be_null
    }

    pub fn get_column_type(&self) -> ColumnType {
        self.column_type.clone()
    }
}

pub trait V1CubeMetaExt {
    fn get_columns(&self) -> Vec<CubeColumn>;

    fn get_scan_columns(&self) -> Vec<CubeColumn>;

    fn contains_member(&self, member_name: &str) -> bool;

    fn member_name(&self, column_name: &str) -> String;

    fn lookup_dimension(&self, column_name: &str) -> Option<&CubeMetaDimension>;

    fn lookup_dimension_by_member_name(&self, member_name: &str) -> Option<&CubeMetaDimension>;

    fn lookup_measure(&self, column_name: &str) -> Option<&CubeMetaMeasure>;

    fn lookup_measure_by_member_name(&self, member_name: &str) -> Option<&CubeMetaMeasure>;

    fn lookup_segment(&self, column_name: &str) -> Option<&CubeMetaSegment>;

    fn df_data_type(&self, member_name: &str) -> Option<DataType>;

    fn member_type(&self, member_name: &str) -> Option<MemberType>;
}

pub enum MemberType {
    String,
    Number,
    Time,
    Boolean,
}

impl V1CubeMetaExt for CubeMeta {
    fn get_columns(&self) -> Vec<CubeColumn> {
        let mut columns = Vec::new();

        for measure in &self.measures {
            columns.push(CubeColumn {
                member_name: measure.name.clone(),
                name: measure.get_real_name(),
                description: measure.description.clone(),
                column_type: measure.get_sql_type(),
                can_be_null: false,
            });
        }

        for dimension in &self.dimensions {
            columns.push(CubeColumn {
                member_name: dimension.name.clone(),
                name: dimension.get_real_name(),
                description: dimension.description.clone(),
                column_type: dimension.get_sql_type(),
                can_be_null: dimension.sql_can_be_null(),
            });
        }

        for segment in &self.segments {
            columns.push(CubeColumn {
                member_name: segment.name.clone(),
                name: segment.get_real_name(),
                description: segment.description.clone(),
                column_type: ColumnType::Boolean,
                can_be_null: false,
            });
        }

        columns.push(CubeColumn {
            member_name: "__user".to_string(),
            name: "__user".to_string(),
            description: Some("Virtual column for security context switching".to_string()),
            column_type: ColumnType::String,
            can_be_null: true,
        });

        columns.push(CubeColumn {
            member_name: "__cubeJoinField".to_string(),
            name: "__cubeJoinField".to_string(),
            description: Some("Virtual column for joining cubes".to_string()),
            column_type: ColumnType::String,
            can_be_null: true,
        });

        columns
    }

    fn get_scan_columns(&self) -> Vec<CubeColumn> {
        let mut columns = Vec::new();

        for measure in &self.measures {
            columns.push(CubeColumn {
                member_name: measure.name.clone(),
                name: measure.get_real_name(),
                description: None,
                column_type: measure.get_sql_type(),
                can_be_null: false,
            });
        }

        for dimension in &self.dimensions {
            columns.push(CubeColumn {
                member_name: dimension.name.clone(),
                name: dimension.get_real_name(),
                description: None,
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

    fn member_name(&self, column_name: &str) -> String {
        format!("{}.{}", self.name, column_name)
    }

    fn lookup_measure(&self, column_name: &str) -> Option<&CubeMetaMeasure> {
        let member_name = self.member_name(column_name);
        self.lookup_measure_by_member_name(&member_name)
    }

    fn lookup_measure_by_member_name(&self, member_name: &str) -> Option<&CubeMetaMeasure> {
        self.measures
            .iter()
            .find(|m| m.name.eq_ignore_ascii_case(&member_name))
    }

    fn lookup_dimension(&self, column_name: &str) -> Option<&CubeMetaDimension> {
        let member_name = self.member_name(column_name);
        self.lookup_dimension_by_member_name(&member_name)
    }

    fn lookup_dimension_by_member_name(&self, member_name: &str) -> Option<&CubeMetaDimension> {
        self.dimensions
            .iter()
            .find(|m| m.name.eq_ignore_ascii_case(&member_name))
    }

    fn lookup_segment(&self, column_name: &str) -> Option<&CubeMetaSegment> {
        let member_name = self.member_name(column_name);
        self.segments
            .iter()
            .find(|m| m.name.eq_ignore_ascii_case(&member_name))
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
            return Some(match dimension.r#type.as_str() {
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
