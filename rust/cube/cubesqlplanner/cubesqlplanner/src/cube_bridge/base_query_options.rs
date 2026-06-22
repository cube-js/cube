use super::join_graph::{JoinGraph, NativeJoinGraph};
use super::join_hints::JoinHintItem;
use super::options_member::OptionsMember;
use super::security_context::{NativeSecurityContext, SecurityContext};
use crate::cube_bridge::base_tools::{BaseTools, NativeBaseTools};
use crate::cube_bridge::evaluator::{CubeEvaluator, NativeCubeEvaluator};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::{NativeArray, NativeContextHolder, NativeObjectHandle};
use cubenativeutils::CubeError;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;

/// A single value of a filter (`equals`, `in`, `gt`, …).
#[derive(Debug, Clone, PartialEq)]
pub enum FilterValue {
    Str(String),
    Bool(bool),
    Num(f64),
    Null,
}

impl FilterValue {
    pub fn is_null(&self) -> bool {
        matches!(self, FilterValue::Null)
    }

    /// Canonical string representation bound as a SQL parameter. `Null` yields
    /// `None` (the value is dropped / handled as `IS NULL`). Whole numbers are
    /// rendered without a trailing `.0` (`42.0` → `"42"`).
    pub fn to_param_string(&self) -> Option<String> {
        match self {
            FilterValue::Str(s) => Some(s.clone()),
            FilterValue::Bool(b) => Some(b.to_string()),
            FilterValue::Num(n) => Some(Self::format_number(*n)),
            FilterValue::Null => None,
        }
    }

    fn format_number(n: f64) -> String {
        if n.is_finite() && n.fract() == 0.0 && n.abs() < 1e15 {
            format!("{}", n as i64)
        } else {
            format!("{}", n)
        }
    }
}

impl From<Option<String>> for FilterValue {
    fn from(value: Option<String>) -> Self {
        match value {
            Some(s) => FilterValue::Str(s),
            None => FilterValue::Null,
        }
    }
}

impl From<String> for FilterValue {
    fn from(value: String) -> Self {
        FilterValue::Str(value)
    }
}

impl Serialize for FilterValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            FilterValue::Str(s) => serializer.serialize_str(s),
            FilterValue::Bool(b) => serializer.serialize_bool(*b),
            FilterValue::Num(n) => serializer.serialize_f64(*n),
            FilterValue::Null => serializer.serialize_none(),
        }
    }
}

impl<'de> Deserialize<'de> for FilterValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FilterValueVisitor;

        impl<'de> Visitor<'de> for FilterValueVisitor {
            type Value = FilterValue;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string, boolean, number, or null")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E> {
                Ok(FilterValue::Bool(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(FilterValue::Num(v as f64))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(FilterValue::Num(v as f64))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
                Ok(FilterValue::Num(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                Ok(FilterValue::Str(v.to_string()))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                Ok(FilterValue::Str(v))
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(FilterValue::Null)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(FilterValue::Null)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_any(self)
            }
        }

        deserializer.deserialize_any(FilterValueVisitor)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MaskedMemberItem {
    pub member: String,
    pub filter: Option<FilterItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TimeDimension {
    pub dimension: String,
    pub granularity: Option<String>,
    #[serde(rename = "dateRange")]
    pub date_range: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FilterItem {
    pub or: Option<Vec<FilterItem>>,
    pub and: Option<Vec<FilterItem>>,
    pub member: Option<String>,
    pub dimension: Option<String>,
    pub operator: Option<String>,
    pub values: Option<Vec<FilterValue>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrderByItem {
    pub id: String,
    pub desc: Option<bool>,
}

impl OrderByItem {
    pub fn is_desc(&self) -> bool {
        self.desc.unwrap_or(false)
    }
}

impl FilterItem {
    pub fn member(&self) -> Option<&String> {
        self.member.as_ref().or(self.dimension.as_ref())
    }
}

#[derive(Serialize, Deserialize, Debug, nativebridge::NativeBridgeStatic)]
pub struct BaseQueryOptionsStatic {
    #[serde(rename = "timeDimensions")]
    pub time_dimensions: Option<Vec<TimeDimension>>,
    pub timezone: Option<String>,
    pub filters: Option<Vec<FilterItem>>,
    pub order: Option<Vec<OrderByItem>>,
    pub limit: Option<String>,
    #[serde(rename = "rowLimit")]
    pub row_limit: Option<String>,
    pub offset: Option<String>,
    pub ungrouped: Option<bool>,
    #[serde(rename = "exportAnnotatedSql")]
    pub export_annotated_sql: bool,
    #[serde(rename = "preAggregationQuery")]
    pub pre_aggregation_query: Option<bool>,
    #[serde(rename = "totalQuery")]
    pub total_query: Option<bool>,
    #[serde(rename = "cubestoreSupportMultistage")]
    pub cubestore_support_multistage: Option<bool>,
    #[serde(rename = "disableExternalPreAggregations")]
    pub disable_external_pre_aggregations: bool,
    #[serde(rename = "preAggregationId")]
    pub pre_aggregation_id: Option<String>,
    #[serde(rename = "convertTzForRawTimeDimension")]
    pub convert_tz_for_raw_time_dimension: Option<bool>,
    #[serde(rename = "maskedMembers")]
    pub masked_members: Option<Vec<MaskedMemberItem>>,
    #[serde(rename = "memberToAlias", default)]
    pub member_to_alias: Option<HashMap<String, String>>,
}

#[nativebridge::native_bridge(BaseQueryOptionsStatic, with_static_meta)]
pub trait BaseQueryOptions {
    #[nbridge(field, optional, vec)]
    fn measures(&self) -> Result<Option<Vec<OptionsMember>>, CubeError>;
    #[nbridge(field, optional, vec)]
    fn dimensions(&self) -> Result<Option<Vec<OptionsMember>>, CubeError>;
    #[nbridge(field, optional, vec)]
    fn segments(&self) -> Result<Option<Vec<OptionsMember>>, CubeError>;
    #[nbridge(field)]
    fn cube_evaluator(&self) -> Result<Rc<dyn CubeEvaluator>, CubeError>;
    #[nbridge(field)]
    fn base_tools(&self) -> Result<Rc<dyn BaseTools>, CubeError>;
    #[nbridge(field)]
    fn join_graph(&self) -> Result<Rc<dyn JoinGraph>, CubeError>;
    #[nbridge(field)]
    fn security_context(&self) -> Result<Rc<dyn SecurityContext>, CubeError>;
    #[nbridge(field, optional, vec)]
    fn join_hints(&self) -> Result<Option<Vec<JoinHintItem>>, CubeError>;
}
