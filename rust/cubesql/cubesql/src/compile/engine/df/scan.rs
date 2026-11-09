use crate::compile::date_parser::parse_date_str;
use crate::{
    compile::{
        engine::df::wrapper::{CubeScanWrappedSqlNode, CubeScanWrapperNode, SqlQuery},
        test::find_cube_scans_deep_search,
    },
    config::ConfigObj,
    sql::AuthContextRef,
    transport::{CubeStreamReceiver, LoadRequestMeta, SpanId, TransportService},
    CubeError, CubeErrorCauseType,
};
use async_trait::async_trait;
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use cubeclient::models::{
    V1LoadRequestQuery, V1LoadResponse, V1LoadResult, V1LoadResultDataColumnar,
};
pub use datafusion::{
    arrow::{
        array::{
            ArrayRef, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
            Float64Builder, Int16Builder, Int32Builder, Int64Builder, NullArray, PrimitiveBuilder,
            StringBuilder, UInt64Builder,
        },
        datatypes::{ArrowPrimitiveType, DataType, Schema, SchemaRef},
        error::{ArrowError, Result as ArrowResult},
        record_batch::RecordBatch,
    },
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_plan::{DFSchemaRef, Expr, LogicalPlan, UserDefinedLogicalNode},
    physical_plan::{
        expressions::PhysicalSortExpr, planner::ExtensionPlanner, DisplayFormatType, ExecutionPlan,
        Partitioning, PhysicalPlanner, RecordBatchStream, SendableRecordBatchStream, Statistics,
    },
};
use datafusion::{
    arrow::{
        array::{
            IntervalDayTimeBuilder, IntervalMonthDayNanoBuilder, IntervalYearMonthBuilder,
            TimestampMillisecondBuilder, TimestampNanosecondBuilder,
        },
        datatypes::{IntervalUnit, TimeUnit},
    },
    execution::context::TaskContext,
    scalar::ScalarValue,
};
use futures::Stream;
use log::warn;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::str::FromStr;
use std::{
    any::Any,
    borrow::Cow,
    fmt::{self, Display},
    sync::Arc,
    task::{Context, Poll},
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RegularMember {
    pub member: String,
    /// Field name in Cube response for this member. Can be different from member, i.e. for
    /// time dimension with granularity: member is `cube.dimension`, field is `cube.dim.granularity`
    pub field_name: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MemberField {
    Member(RegularMember),
    Literal(ScalarValue),
}

impl MemberField {
    pub fn regular(member: String) -> Self {
        let field_name = member.clone();
        MemberField::Member(RegularMember { member, field_name })
    }

    pub fn time_dimension(member: String, granularity: String) -> Self {
        let field_name = format!("{}.{}", member, granularity);
        MemberField::Member(RegularMember { member, field_name })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum CacheMode {
    #[serde(rename = "stale-if-slow")]
    StaleIfSlow,
    #[serde(rename = "stale-while-revalidate")]
    StaleWhileRevalidate,
    #[serde(rename = "must-revalidate")]
    MustRevalidate,
    #[serde(rename = "no-cache")]
    NoCache,
}

impl FromStr for CacheMode {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "stale-if-slow" => Ok(Self::StaleIfSlow),
            "stale-while-revalidate" => Ok(Self::StaleWhileRevalidate),
            "must-revalidate" => Ok(Self::MustRevalidate),
            "no-cache" => Ok(Self::NoCache),
            other => Err(format!("Unknown cache mode: {}", other)),
        }
    }
}

impl Display for CacheMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CacheMode::StaleIfSlow => write!(f, "stale-if-slow"),
            CacheMode::StaleWhileRevalidate => write!(f, "stale-while-revalidate"),
            CacheMode::MustRevalidate => write!(f, "must-revalidate"),
            CacheMode::NoCache => write!(f, "no-cache"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CubeScanOptions {
    pub change_user: Option<String>,
    pub max_records: Option<usize>,
    pub cache_mode: Option<CacheMode>,
    pub throw_continue_wait: bool,
    /// The scan is wrapped, and its SQL is to be generated over a Cube Store
    /// pre-aggregation. Cube was asked at planning whether it matches one, so SQL
    /// generation fails when it no longer does instead of falling back
    pub pre_aggregation: bool,
}

#[derive(Debug, Clone)]
pub struct CubeScanNode {
    pub schema: DFSchemaRef,
    pub member_fields: Vec<MemberField>,
    pub request: V1LoadRequestQuery,
    pub auth_context: AuthContextRef,
    pub options: CubeScanOptions,
    pub used_cubes: Vec<String>,
    pub span_id: Option<Arc<SpanId>>,
}

impl CubeScanNode {
    pub fn new(
        schema: DFSchemaRef,
        member_fields: Vec<MemberField>,
        request: V1LoadRequestQuery,
        auth_context: AuthContextRef,
        options: CubeScanOptions,
        used_cubes: Vec<String>,
        span_id: Option<Arc<SpanId>>,
    ) -> Self {
        Self {
            schema,
            member_fields,
            request,
            auth_context,
            options,
            used_cubes,
            span_id,
        }
    }
}

impl UserDefinedLogicalNode for CubeScanNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CubeScan: request={}",
            serde_json::to_string_pretty(&self.request).unwrap()
        )
    }

    fn from_template(
        &self,
        exprs: &[datafusion::logical_plan::Expr],
        inputs: &[datafusion::logical_plan::LogicalPlan],
    ) -> std::sync::Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        assert_eq!(inputs.len(), 0, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");

        Arc::new(CubeScanNode {
            schema: self.schema.clone(),
            member_fields: self.member_fields.clone(),
            request: self.request.clone(),
            auth_context: self.auth_context.clone(),
            options: self.options.clone(),
            used_cubes: self.used_cubes.clone(),
            span_id: self.span_id.clone(),
        })
    }
}

//  Produces an execution plan where the schema is mismatched from
//  the logical plan node.
pub struct CubeScanExtensionPlanner {
    pub transport: Arc<dyn TransportService>,
    pub meta: LoadRequestMeta,
    pub config_obj: Arc<dyn ConfigObj>,
}

impl ExtensionPlanner for CubeScanExtensionPlanner {
    /// Create a physical plan for an extension node
    fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(
            if let Some(scan_node) = node.as_any().downcast_ref::<CubeScanNode>() {
                assert_eq!(logical_inputs.len(), 0, "Inconsistent number of inputs");
                assert_eq!(physical_inputs.len(), 0, "Inconsistent number of inputs");

                // figure out input name
                Some(Arc::new(CubeScanExecutionPlan {
                    schema: SchemaRef::new(scan_node.schema().as_ref().into()),
                    member_fields: scan_node.member_fields.clone(),
                    transport: self.transport.clone(),
                    request: scan_node.request.clone(),
                    wrapped_sql: None,
                    auth_context: scan_node.auth_context.clone(),
                    options: scan_node.options.clone(),
                    meta: self.meta.clone(),
                    span_id: scan_node.span_id.clone(),
                    config_obj: self.config_obj.clone(),
                }))
            } else if let Some(wrapper_node) = node.as_any().downcast_ref::<CubeScanWrapperNode>() {
                return Err(DataFusionError::Internal(format!(
                    "CubeScanWrapperNode is not executable, SQL should be generated first with QueryEngine::evaluate_wrapped_sql: {:?}",
                    wrapper_node
                )));
            } else if let Some(wrapped_sql_node) =
                node.as_any().downcast_ref::<CubeScanWrappedSqlNode>()
            {
                // TODO
                // assert_eq!(logical_inputs.len(), 0, "Inconsistent number of inputs");
                // assert_eq!(physical_inputs.len(), 0, "Inconsistent number of inputs");
                let scan_node =
                    find_cube_scans_deep_search(wrapped_sql_node.wrapped_plan.clone(), false)
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "No cube scans found in wrapper node: {:?}",
                                wrapped_sql_node
                            ))
                        })?;

                let schema = SchemaRef::new(wrapped_sql_node.schema().as_ref().into());
                Some(Arc::new(CubeScanExecutionPlan {
                    schema,
                    member_fields: wrapped_sql_node.member_fields.clone(),
                    transport: self.transport.clone(),
                    request: wrapped_sql_node.request.clone(),
                    wrapped_sql: Some(wrapped_sql_node.wrapped_sql.clone()),
                    auth_context: scan_node.auth_context.clone(),
                    options: scan_node.options.clone(),
                    meta: self.meta.clone(),
                    span_id: scan_node.span_id.clone(),
                    config_obj: self.config_obj.clone(),
                }))
            } else {
                None
            },
        )
    }
}

#[derive(Debug)]
struct CubeScanExecutionPlan {
    // Options from logical node
    schema: SchemaRef,
    member_fields: Vec<MemberField>,
    request: V1LoadRequestQuery,
    wrapped_sql: Option<SqlQuery>,
    auth_context: AuthContextRef,
    options: CubeScanOptions,
    // Shared references which will be injected by extension planner
    transport: Arc<dyn TransportService>,
    // injected by extension planner
    meta: LoadRequestMeta,
    span_id: Option<Arc<SpanId>>,
    config_obj: Arc<dyn ConfigObj>,
}

#[derive(Debug, Clone)]
pub enum FieldValue<'a> {
    // Why Cow?
    // We use N-API via Neon (only for streaming), which doesn't allow us to build string reference,
    // because V8 uses UTF-16 It allocates/converts a new strings while doing JsString.value()
    // @see v8 WriteUtf8 for more details. Cow::Owned is used for this variant
    String(Cow<'a, str>),
    Number(f64),
    Bool(bool),
    Null,
}

pub trait ColumnarValueObject {
    fn len(&mut self) -> std::result::Result<usize, CubeError>;

    fn column<'a>(
        &'a mut self,
        field_name: &str,
    ) -> std::result::Result<
        Box<dyn Iterator<Item = std::result::Result<FieldValue<'a>, CubeError>> + 'a>,
        CubeError,
    >;
}

fn json_value_to_field_value(value: &Value) -> std::result::Result<FieldValue<'_>, CubeError> {
    Ok(match value {
        Value::String(s) => FieldValue::String(Cow::Borrowed(s)),
        Value::Number(n) => FieldValue::Number(n.as_f64().ok_or_else(|| {
            DataFusionError::Execution(format!("Can't convert {:?} to float", n))
        })?),
        Value::Bool(b) => FieldValue::Bool(*b),
        Value::Null => FieldValue::Null,
        x @ (Value::Array(_) | Value::Object(_)) => {
            FieldValue::String(Cow::Owned(serde_json::to_string(x).map_err(|e| {
                CubeError::internal(format!("Can't serialize non-scalar value to JSON: {}", e))
            })?))
        }
    })
}

#[derive(Deserialize)]
pub struct JsonColumnarValueObjectRaw {
    members: Vec<String>,
    columns: Vec<Vec<Value>>,
}

impl std::convert::TryFrom<JsonColumnarValueObjectRaw> for JsonColumnarValueObject {
    type Error = CubeError;

    fn try_from(raw: JsonColumnarValueObjectRaw) -> std::result::Result<Self, Self::Error> {
        Self::try_new(raw.members, raw.columns)
    }
}

#[derive(Deserialize)]
#[serde(try_from = "JsonColumnarValueObjectRaw")]
pub struct JsonColumnarValueObject {
    members: Vec<String>,
    columns: Vec<Vec<Value>>,
}

impl JsonColumnarValueObject {
    pub fn try_new(
        members: Vec<String>,
        columns: Vec<Vec<Value>>,
    ) -> std::result::Result<Self, CubeError> {
        if let Some(expected) = columns.first().map(|c| c.len()) {
            if let Some(idx) = columns.iter().position(|c| c.len() != expected) {
                return Err(CubeError::internal(format!(
                    "columnar response has ragged columns: column {} has {} rows, expected {}",
                    idx,
                    columns[idx].len(),
                    expected
                )));
            }
        }

        Ok(Self { members, columns })
    }
}

impl ColumnarValueObject for JsonColumnarValueObject {
    fn len(&mut self) -> std::result::Result<usize, CubeError> {
        Ok(self.columns.first().map(|c| c.len()).unwrap_or(0))
    }

    fn column<'a>(
        &'a mut self,
        field_name: &str,
    ) -> std::result::Result<
        Box<dyn Iterator<Item = std::result::Result<FieldValue<'a>, CubeError>> + 'a>,
        CubeError,
    > {
        let Some(idx) = self.members.iter().position(|m| m == field_name) else {
            // A missing field is treated as a column of NULLs.
            let len = self.columns.first().map(|c| c.len()).unwrap_or(0);
            return Ok(Box::new((0..len).map(|_| Ok(FieldValue::Null))));
        };

        let Some(column) = self.columns.get(idx) else {
            return Err(CubeError::internal(format!(
                "Unexpected response from Cube, missing column for '{}'",
                field_name
            )));
        };

        Ok(Box::new(column.iter().map(json_value_to_field_value)))
    }
}

/// A columnar value object with no data columns, representing `row_count` rows of a
/// literal-only projection (the `no_members_query` shortcut). Every schema field is a
/// `MemberField::Literal`, so `column()` is never actually invoked — only `len()` matters.
struct LiteralRowsValueObject {
    row_count: usize,
}

impl ColumnarValueObject for LiteralRowsValueObject {
    fn len(&mut self) -> std::result::Result<usize, CubeError> {
        Ok(self.row_count)
    }

    fn column<'a>(
        &'a mut self,
        _field_name: &str,
    ) -> std::result::Result<
        Box<dyn Iterator<Item = std::result::Result<FieldValue<'a>, CubeError>> + 'a>,
        CubeError,
    > {
        let row_count = self.row_count;
        Ok(Box::new((0..row_count).map(|_| Ok(FieldValue::Null))))
    }
}

/// Matches a `ColumnBuilder` once and runs `$body` with its typed builder and the function that
/// converts a `FieldValue` into it, so every caller converts each type the same way.
macro_rules! with_converter {
    ($column_builder:expr, |$builder:ident, $convert:ident| $body:expr) => {
        match $column_builder {
            ColumnBuilder::Utf8($builder) => {
                let $convert = append_utf8;
                $body
            }
            ColumnBuilder::Int16($builder) => {
                let $convert =
                    |b: &mut Int16Builder, v| append_number(b, v, |n| n.round() as i16, "i16");
                $body
            }
            ColumnBuilder::Int32($builder) => {
                let $convert =
                    |b: &mut Int32Builder, v| append_number(b, v, |n| n.round() as i32, "i32");
                $body
            }
            ColumnBuilder::Int64($builder) => {
                let $convert =
                    |b: &mut Int64Builder, v| append_number(b, v, |n| n.round() as i64, "i64");
                $body
            }
            ColumnBuilder::UInt64($builder) => {
                let $convert =
                    |b: &mut UInt64Builder, v| append_number(b, v, |n| n.round() as u64, "u64");
                $body
            }
            ColumnBuilder::Float32($builder) => {
                let $convert = |b: &mut Float32Builder, v| append_number(b, v, |n| n as f32, "f32");
                $body
            }
            ColumnBuilder::Float64($builder) => {
                let $convert = |b: &mut Float64Builder, v| append_number(b, v, |n| n, "f64");
                $body
            }
            ColumnBuilder::Boolean($builder) => {
                let $convert = append_boolean;
                $body
            }
            ColumnBuilder::TimestampNanosecond($builder) => {
                let $convert = append_timestamp_nanosecond;
                $body
            }
            ColumnBuilder::TimestampMillisecond($builder) => {
                let $convert = append_timestamp_millisecond;
                $body
            }
            ColumnBuilder::Date32($builder) => {
                let $convert = append_date32;
                $body
            }
            ColumnBuilder::Decimal {
                builder: $builder,
                precision,
                scale,
            } => {
                let (precision, scale) = (*precision, *scale);
                let $convert =
                    |b: &mut Decimal128Builder, v| append_decimal(b, precision, scale, v);
                $body
            }
            ColumnBuilder::IntervalYearMonth($builder) => {
                let $convert = append_null_only;
                $body
            }
            ColumnBuilder::IntervalDayTime($builder) => {
                let $convert = append_null_only;
                $body
            }
            ColumnBuilder::IntervalMonthDayNano($builder) => {
                let $convert = append_null_only;
                $body
            }
        }
    };
}

/// Arrow builder for one schema field of a Cube response, converting each `FieldValue` the
/// way the field's type expects. Shared by `transform_response` and `deserialize_columnar_chunk`
/// so both read a response identically.
enum ColumnBuilder {
    Utf8(StringBuilder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    TimestampNanosecond(TimestampNanosecondBuilder),
    TimestampMillisecond(TimestampMillisecondBuilder),
    Date32(Date32Builder),
    Decimal {
        builder: Decimal128Builder,
        precision: usize,
        scale: usize,
    },
    IntervalYearMonth(IntervalYearMonthBuilder),
    IntervalDayTime(IntervalDayTimeBuilder),
    IntervalMonthDayNano(IntervalMonthDayNanoBuilder),
}

impl ColumnBuilder {
    fn try_new(data_type: &DataType, capacity: usize) -> std::result::Result<Self, CubeError> {
        Ok(match data_type {
            DataType::Utf8 => Self::Utf8(StringBuilder::with_capacity(capacity, capacity)),
            DataType::Int16 => Self::Int16(Int16Builder::with_capacity(capacity)),
            DataType::Int32 => Self::Int32(Int32Builder::with_capacity(capacity)),
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            // Cube Store answers a pushed down window function, like ROW_NUMBER, as
            // an unsigned count, which the plan on top reads as such
            DataType::UInt64 => Self::UInt64(UInt64Builder::with_capacity(capacity)),
            DataType::Float32 => Self::Float32(Float32Builder::with_capacity(capacity)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                Self::TimestampNanosecond(TimestampNanosecondBuilder::with_capacity(capacity))
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                Self::TimestampMillisecond(TimestampMillisecondBuilder::with_capacity(capacity))
            }
            DataType::Date32 => Self::Date32(Date32Builder::with_capacity(capacity)),
            DataType::Decimal128(precision, scale) => Self::Decimal {
                builder: Decimal128Builder::with_capacity(capacity)
                    .with_data_type(data_type.clone()),
                precision: *precision as usize,
                scale: <usize as std::convert::TryFrom<i8>>::try_from(*scale).map_err(|_| {
                    CubeError::internal(format!(
                        "Negative decimal scale is not supported: {}",
                        scale
                    ))
                })?,
            },
            DataType::Interval(IntervalUnit::YearMonth) => {
                Self::IntervalYearMonth(IntervalYearMonthBuilder::with_capacity(capacity))
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                Self::IntervalDayTime(IntervalDayTimeBuilder::with_capacity(capacity))
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                Self::IntervalMonthDayNano(IntervalMonthDayNanoBuilder::with_capacity(capacity))
            }
            t => {
                return Err(CubeError::internal(format!(
                    "Type {} is not supported in response transformation from Cube",
                    t,
                )))
            }
        })
    }

    fn data_type(&self) -> DataType {
        match self {
            Self::Utf8(_) => DataType::Utf8,
            Self::Int16(_) => DataType::Int16,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::UInt64(_) => DataType::UInt64,
            Self::Float32(_) => DataType::Float32,
            Self::Float64(_) => DataType::Float64,
            Self::Boolean(_) => DataType::Boolean,
            Self::TimestampNanosecond(_) => DataType::Timestamp(TimeUnit::Nanosecond, None),
            Self::TimestampMillisecond(_) => DataType::Timestamp(TimeUnit::Millisecond, None),
            Self::Date32(_) => DataType::Date32,
            Self::Decimal {
                precision, scale, ..
            } => DataType::Decimal128(*precision as u8, *scale as i8),
            Self::IntervalYearMonth(_) => DataType::Interval(IntervalUnit::YearMonth),
            Self::IntervalDayTime(_) => DataType::Interval(IntervalUnit::DayTime),
            Self::IntervalMonthDayNano(_) => DataType::Interval(IntervalUnit::MonthDayNano),
        }
    }

    fn append(&mut self, value: FieldValue<'_>) -> std::result::Result<(), CubeError> {
        with_converter!(self, |builder, convert| convert(builder, value))
    }

    /// Appends a whole column, picking the builder once instead of once per value.
    fn append_column<'a>(
        &mut self,
        values: impl Iterator<Item = std::result::Result<FieldValue<'a>, CubeError>>,
    ) -> std::result::Result<(), CubeError> {
        with_converter!(self, |builder, convert| {
            for value in values {
                convert(builder, value?)?;
            }

            Ok(())
        })
    }

    fn append_literal(
        &mut self,
        value: &ScalarValue,
        count: usize,
    ) -> std::result::Result<(), CubeError> {
        for _ in 0..count {
            match (value, &mut *self) {
                (ScalarValue::Utf8(v), Self::Utf8(builder)) => builder.append_option(v.as_ref()),
                (ScalarValue::Int16(v), Self::Int16(builder)) => builder.append_option(*v),
                (ScalarValue::Int32(v), Self::Int32(builder)) => builder.append_option(*v),
                (ScalarValue::Int64(v), Self::Int64(builder)) => builder.append_option(*v),
                (ScalarValue::UInt64(v), Self::UInt64(builder)) => builder.append_option(*v),
                (ScalarValue::Float32(v), Self::Float32(builder)) => builder.append_option(*v),
                (ScalarValue::Float64(v), Self::Float64(builder)) => builder.append_option(*v),
                (ScalarValue::Boolean(v), Self::Boolean(builder)) => builder.append_option(*v),
                (ScalarValue::TimestampNanosecond(v, None), Self::TimestampNanosecond(builder)) => {
                    builder.append_option(*v)
                }
                (
                    ScalarValue::TimestampMillisecond(v, None),
                    Self::TimestampMillisecond(builder),
                ) => builder.append_option(*v),
                (ScalarValue::Date32(v), Self::Date32(builder)) => builder.append_option(*v),
                // TODO: check precision and scale, adjust accordingly
                (ScalarValue::Decimal128(v, _, _), Self::Decimal { builder, .. }) => match v {
                    Some(v) => builder.append_value(*v),
                    None => builder.append_null(),
                },
                (ScalarValue::IntervalYearMonth(v), Self::IntervalYearMonth(builder)) => {
                    builder.append_option(*v)
                }
                (ScalarValue::IntervalDayTime(v), Self::IntervalDayTime(builder)) => {
                    builder.append_option(*v)
                }
                (ScalarValue::IntervalMonthDayNano(v), Self::IntervalMonthDayNano(builder)) => {
                    builder.append_option(*v)
                }
                (v, builder) => {
                    return Err(CubeError::internal(format!(
                        "Unable to map value {:?} to {:?}",
                        v,
                        builder.data_type()
                    )));
                }
            }
        }

        Ok(())
    }

    fn append_null(&mut self) -> std::result::Result<(), CubeError> {
        match self {
            Self::Utf8(builder) => builder.append_null(),
            Self::Int16(builder) => builder.append_null(),
            Self::Int32(builder) => builder.append_null(),
            Self::Int64(builder) => builder.append_null(),
            Self::UInt64(builder) => builder.append_null(),
            Self::Float32(builder) => builder.append_null(),
            Self::Float64(builder) => builder.append_null(),
            Self::Boolean(builder) => builder.append_null(),
            Self::TimestampNanosecond(builder) => builder.append_null(),
            Self::TimestampMillisecond(builder) => builder.append_null(),
            Self::Date32(builder) => builder.append_null(),
            Self::Decimal { builder, .. } => builder.append_null(),
            Self::IntervalYearMonth(builder) => builder.append_null(),
            Self::IntervalDayTime(builder) => builder.append_null(),
            Self::IntervalMonthDayNano(builder) => builder.append_null(),
        };

        Ok(())
    }

    fn append_nulls(&mut self, count: usize) -> std::result::Result<(), CubeError> {
        for _ in 0..count {
            self.append_null()?;
        }

        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Utf8(mut builder) => Arc::new(builder.finish()),
            Self::Int16(mut builder) => Arc::new(builder.finish()),
            Self::Int32(mut builder) => Arc::new(builder.finish()),
            Self::Int64(mut builder) => Arc::new(builder.finish()),
            Self::UInt64(mut builder) => Arc::new(builder.finish()),
            Self::Float32(mut builder) => Arc::new(builder.finish()),
            Self::Float64(mut builder) => Arc::new(builder.finish()),
            Self::Boolean(mut builder) => Arc::new(builder.finish()),
            Self::TimestampNanosecond(mut builder) => Arc::new(builder.finish()),
            Self::TimestampMillisecond(mut builder) => Arc::new(builder.finish()),
            Self::Date32(mut builder) => Arc::new(builder.finish()),
            Self::Decimal { mut builder, .. } => Arc::new(builder.finish()),
            Self::IntervalYearMonth(mut builder) => Arc::new(builder.finish()),
            Self::IntervalDayTime(mut builder) => Arc::new(builder.finish()),
            Self::IntervalMonthDayNano(mut builder) => Arc::new(builder.finish()),
        }
    }
}

fn unmapped_value(value: FieldValue<'_>, data_type: &DataType) -> CubeError {
    CubeError::internal(format!(
        "Unable to map value {:?} to {:?}",
        value, data_type
    ))
}

#[inline]
fn append_utf8(
    builder: &mut StringBuilder,
    value: FieldValue<'_>,
) -> std::result::Result<(), CubeError> {
    match value {
        FieldValue::Null => builder.append_null(),
        FieldValue::String(v) => builder.append_value(v),
        FieldValue::Bool(v) => builder.append_value(if v { "true" } else { "false" }),
        FieldValue::Number(v) => builder.append_value(v.to_string()),
    };

    Ok(())
}

#[inline]
fn append_number<T>(
    builder: &mut PrimitiveBuilder<T>,
    value: FieldValue<'_>,
    from_f64: impl Fn(f64) -> T::Native,
    type_name: &str,
) -> std::result::Result<(), CubeError>
where
    T: ArrowPrimitiveType,
    T::Native: FromStr,
    <T::Native as FromStr>::Err: Display,
{
    match value {
        FieldValue::Null => builder.append_null(),
        FieldValue::Number(number) => builder.append_value(from_f64(number)),
        FieldValue::String(s) => match s.parse::<T::Native>() {
            Ok(v) => builder.append_value(v),
            Err(error) => {
                warn!("Unable to parse value as {}: {}", type_name, error);

                builder.append_null()
            }
        },
        v => return Err(unmapped_value(v, &T::DATA_TYPE)),
    };

    Ok(())
}

#[inline]
fn append_boolean(
    builder: &mut BooleanBuilder,
    value: FieldValue<'_>,
) -> std::result::Result<(), CubeError> {
    match value {
        FieldValue::Null => builder.append_null(),
        FieldValue::Bool(v) => builder.append_value(v),
        FieldValue::String(v) => match v.as_ref() {
            "true" | "1" => builder.append_value(true),
            "false" | "0" => builder.append_value(false),
            _ => {
                log::error!(
                    "Unable to map value {:?} to DataType::Boolean (returning null)",
                    v
                );

                builder.append_null()
            }
        },
        v => return Err(unmapped_value(v, &DataType::Boolean)),
    };

    Ok(())
}

#[inline]
fn append_timestamp_nanosecond(
    builder: &mut TimestampNanosecondBuilder,
    value: FieldValue<'_>,
) -> std::result::Result<(), CubeError> {
    match value {
        FieldValue::Null => builder.append_null(),
        FieldValue::String(s) => {
            let timestamp = parse_date_str(s.as_ref())?;
            // TODO switch parsing to microseconds
            if let Some(nanos) = timestamp.and_utc().timestamp_nanos_opt() {
                builder.append_value(nanos);
            } else {
                log::error!(
                    "Unable to cast timestamp value to nanoseconds: {}",
                    timestamp
                );
                builder.append_null();
            }
        }
        v => {
            return Err(unmapped_value(
                v,
                &DataType::Timestamp(TimeUnit::Nanosecond, None),
            ))
        }
    };

    Ok(())
}

#[inline]
fn append_timestamp_millisecond(
    builder: &mut TimestampMillisecondBuilder,
    value: FieldValue<'_>,
) -> std::result::Result<(), CubeError> {
    match value {
        FieldValue::Null => builder.append_null(),
        FieldValue::String(s) => {
            let timestamp = parse_date_str(s.as_ref())?;
            builder.append_value(timestamp.and_utc().timestamp_millis());
        }
        v => {
            return Err(unmapped_value(
                v,
                &DataType::Timestamp(TimeUnit::Millisecond, None),
            ))
        }
    };

    Ok(())
}

#[inline]
fn append_date32(
    builder: &mut Date32Builder,
    value: FieldValue<'_>,
) -> std::result::Result<(), CubeError> {
    match value {
        FieldValue::Null => builder.append_null(),
        FieldValue::String(s) => match parse_date_str(s.as_ref()) {
            Ok(timestamp) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let days_since_epoch =
                    timestamp.date().num_days_from_ce() - epoch.num_days_from_ce();
                builder.append_value(days_since_epoch);
            }
            Err(error) => {
                log::error!("Unable to parse value as Date32: {}", error);
                builder.append_null()
            }
        },
        v => return Err(unmapped_value(v, &DataType::Date32)),
    };

    Ok(())
}

fn append_decimal(
    builder: &mut Decimal128Builder,
    precision: usize,
    scale: usize,
    value: FieldValue<'_>,
) -> std::result::Result<(), CubeError> {
    let s = match value {
        FieldValue::Null => {
            return {
                builder.append_null();
                Ok(())
            }
        }
        FieldValue::String(s) => s,
        v => {
            return Err(unmapped_value(
                v,
                &DataType::Decimal128(precision as u8, scale as i8),
            ))
        }
    };

    let mut parts = s.split(".");
    match parts.next() {
        None => builder.append_null(),
        Some(int_part) => {
            let frac_part = format!("{:0<width$}", parts.next().unwrap_or(""), width = scale);
            if frac_part.len() > scale {
                Err(DataFusionError::Execution(format!(
                    "Decimal scale is higher than requested: expected {}, got {}",
                    scale,
                    frac_part.len()
                )))?;
            }
            if let Some(_) = parts.next() {
                Err(DataFusionError::Execution(format!(
                    "Unable to parse decimal, value contains two dots: {}",
                    s
                )))?;
            }
            let decimal_str = format!("{}{}", int_part, frac_part);
            if decimal_str.len() > precision {
                Err(DataFusionError::Execution(format!(
                    "Decimal precision is higher than requested: expected {}, got {}",
                    precision,
                    decimal_str.len()
                )))?;
            }
            if let Ok(value) = decimal_str.parse::<i128>() {
                builder.append_value(value);
            } else {
                Err(DataFusionError::Execution(format!(
                    "Unable to parse decimal as an i128: {}",
                    decimal_str
                )))?;
            }
        }
    };

    Ok(())
}

/// Types read from Cube only as NULLs, like intervals.
fn append_null_only<T: ArrowPrimitiveType>(
    builder: &mut PrimitiveBuilder<T>,
    value: FieldValue<'_>,
) -> std::result::Result<(), CubeError> {
    match value {
        FieldValue::Null => {
            builder.append_null();
            Ok(())
        }
        v => Err(unmapped_value(v, &T::DATA_TYPE)),
    }
}

#[async_trait]
impl ExecutionPlan for CubeScanExecutionPlan {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {:?}",
            self
        )))
    }

    async fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // TODO: move envs to config
        let stream_mode = self.config_obj.stream_mode();
        let query_limit = self.config_obj.non_streaming_query_max_row_limit();
        let max_batch_rows = self.config_obj.cube_scan_max_batch_rows();

        // Cube Store serves a query over a pre-aggregation, and its driver does not stream
        let streamable = !self.wrapped_sql.as_ref().is_some_and(|sql| sql.external);
        let stream_mode = match (stream_mode && streamable, self.request.limit) {
            (true, None) => true,
            (true, Some(limit)) if limit > query_limit => true,
            (_, _) => false,
        };

        let mut request = self.request.clone();
        if request.limit.unwrap_or_default() > query_limit || request.limit.is_none() {
            request.limit = Some(query_limit);
        }

        let mut meta = self.meta.clone();
        meta.set_change_user(self.options.change_user.clone());

        let mut one_shot_stream = CubeScanOneShotStream::new(
            self.schema.clone(),
            self.member_fields.clone(),
            request.clone(),
            self.auth_context.clone(),
            self.transport.clone(),
            meta.clone(),
            self.options.clone(),
            self.wrapped_sql.clone(),
            self.span_id.clone(),
        );

        if stream_mode {
            let result = self
                .transport
                .load_stream(
                    self.span_id.clone(),
                    self.request.clone(),
                    self.wrapped_sql.clone(),
                    self.auth_context.clone(),
                    meta,
                    self.schema.clone(),
                    self.member_fields.clone(),
                    self.options.throw_continue_wait,
                )
                .await;
            let stream = result.map_err(|err| DataFusionError::External(Box::new(err)))?;
            let main_stream = CubeScanMemoryStream::new(stream);

            return Ok(Box::pin(CubeScanStreamRouter::new(
                Some(main_stream),
                one_shot_stream,
                self.schema.clone(),
                max_batch_rows,
            )));
        }

        let response = load_data(
            self.span_id.clone(),
            request,
            self.auth_context.clone(),
            self.transport.clone(),
            meta.clone(),
            self.schema.clone(),
            self.member_fields.clone(),
            self.options.clone(),
            self.wrapped_sql.clone(),
        )
        .await?;

        // For now execute method executes only one query at a time, so we
        // take the first result
        let rb_schema = response.first().unwrap().schema().clone();
        one_shot_stream.data = Some(response.first().unwrap().clone());

        Ok(Box::pin(CubeScanStreamRouter::new(
            None,
            one_shot_stream,
            rb_schema,
            max_batch_rows,
        )))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                // TODO padding
                if let Some(sql) = self.wrapped_sql.as_ref() {
                    write!(f, "CubeScanExecutionPlan, SQL:\n{}", sql.sql)
                } else {
                    write!(
                        f,
                        "CubeScanExecutionPlan, Request:\n{}",
                        serde_json::to_string(&self.request).map_err(|_| fmt::Error)?
                    )
                }
            }
        }
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
            is_exact: false,
        }
    }
}

struct CubeScanOneShotStream {
    data: Option<RecordBatch>,
    schema: SchemaRef,
    member_fields: Vec<MemberField>,
    request: V1LoadRequestQuery,
    auth_context: AuthContextRef,
    transport: Arc<dyn TransportService>,
    meta: LoadRequestMeta,
    options: CubeScanOptions,
    wrapped_sql: Option<SqlQuery>,
    span_id: Option<Arc<SpanId>>,
}

impl CubeScanOneShotStream {
    pub fn new(
        schema: SchemaRef,
        member_fields: Vec<MemberField>,
        request: V1LoadRequestQuery,
        auth_context: AuthContextRef,
        transport: Arc<dyn TransportService>,
        meta: LoadRequestMeta,
        options: CubeScanOptions,
        wrapped_sql: Option<SqlQuery>,
        span_id: Option<Arc<SpanId>>,
    ) -> Self {
        Self {
            data: None,
            schema,
            member_fields,
            request,
            auth_context,
            transport,
            meta,
            options,
            wrapped_sql,
            span_id,
        }
    }

    fn poll_next(&mut self) -> Option<ArrowResult<RecordBatch>> {
        self.data.take().map(Ok)
    }
}

struct CubeScanMemoryStream {
    receiver: CubeStreamReceiver,
}

impl CubeScanMemoryStream {
    pub fn new(receiver: CubeStreamReceiver) -> Self {
        Self { receiver }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<ArrowResult<RecordBatch>>> {
        self.receiver.poll_recv(cx).map(|res| match res {
            Some(Some(Ok(chunk))) => Some(Ok(chunk)),
            Some(Some(Err(mut err))) => {
                // Remove `Error: ` prefix that can come from database
                err.message = if let Some(message) = err.message.strip_prefix("Error: ") {
                    message.to_string()
                } else {
                    err.message
                };
                // A continue wait also gets the `ContinueWait` cause here, the way
                // `load_data` sets it below, so consumers can match on the cause
                // rather than on the message. The other branch still only
                // prefixes the message: unlike `load_data` this path leaves the
                // incoming cause alone, and re-classifying it would change which
                // Postgres error code a streaming failure reports.
                if err.is_continue_wait() {
                    err.cause = CubeErrorCauseType::ContinueWait;
                } else {
                    err.message = format!("Database Execution Error: {}", err.message);
                }
                Some(Err(ArrowError::ExternalError(Box::new(err))))
            }
            Some(None) => None,
            None => None,
        })
    }
}

/// Splits oversized `RecordBatch`es coming out of a `CubeScan` into chunks of at most
/// `max_rows` rows. Cube can hand back a whole result set as a single batch (see
/// `CubeScanOneShotStream`), which forces every downstream consumer to materialize it
/// in one piece — most visibly the `/v1/cubesql` JSONL writer, which emits one line
/// per batch. Slicing is zero-copy, so only the batch wrappers are allocated.
struct RecordBatchSplitter {
    /// Maximum rows per emitted batch. `None` disables splitting.
    max_rows: Option<usize>,
    /// Chunks left to emit, in reverse row order so `pop()` yields them front-to-back.
    pending: Vec<RecordBatch>,
}

impl RecordBatchSplitter {
    pub fn new(max_rows: usize) -> Self {
        Self {
            // 0 turns splitting off
            max_rows: (max_rows > 0).then_some(max_rows),
            pending: Vec::new(),
        }
    }

    /// Takes the next buffered chunk, if any. Must be drained before feeding another
    /// batch into `split`.
    fn next_pending(&mut self) -> Option<RecordBatch> {
        self.pending.pop()
    }

    /// Returns the first chunk of `batch`, buffering the rest for `next_pending`.
    fn split(&mut self, batch: RecordBatch) -> ArrowResult<RecordBatch> {
        debug_assert!(
            self.pending.is_empty(),
            "pending chunks must be drained before splitting the next batch"
        );

        let num_rows = batch.num_rows();
        let Some(max_rows) = self.max_rows.filter(|max_rows| num_rows > *max_rows) else {
            return Ok(batch);
        };

        let schema = batch.schema();
        let mut chunks = Vec::with_capacity(num_rows.div_ceil(max_rows));
        let mut offset = 0;
        while offset < num_rows {
            let len = max_rows.min(num_rows - offset);
            let columns = batch
                .columns()
                .iter()
                .map(|column| column.slice(offset, len))
                .collect();
            chunks.push(RecordBatch::try_new(schema.clone(), columns)?);
            offset += len;
        }

        // `pending` is popped from the back, so reverse to keep rows in order
        chunks.reverse();
        let first = chunks
            .pop()
            .expect("num_rows > max_rows >= 1 always yields at least one chunk");
        self.pending = chunks;

        Ok(first)
    }
}

struct CubeScanStreamRouter {
    main_stream: Option<CubeScanMemoryStream>,
    one_shot_stream: CubeScanOneShotStream,
    schema: SchemaRef,
    splitter: RecordBatchSplitter,
}

impl CubeScanStreamRouter {
    pub fn new(
        main_stream: Option<CubeScanMemoryStream>,
        one_shot_stream: CubeScanOneShotStream,
        schema: SchemaRef,
        max_batch_rows: usize,
    ) -> Self {
        Self {
            main_stream,
            one_shot_stream,
            schema,
            splitter: RecordBatchSplitter::new(max_batch_rows),
        }
    }

    /// Pulls the next batch from the streaming transport, falling back to a one-shot
    /// load when it turns out to not implement streaming.
    fn poll_next_batch(&mut self, cx: &mut Context<'_>) -> Poll<Option<ArrowResult<RecordBatch>>> {
        let Some(main_stream) = &mut self.main_stream else {
            return Poll::Ready(self.one_shot_stream.poll_next());
        };

        let next = main_stream.poll_next(cx);
        if let Poll::Ready(Some(Err(ArrowError::ExternalError(err)))) = &next {
            if err
                .to_string()
                .contains("streamQuery() method is not implemented yet")
            {
                warn!("{}", err);

                self.main_stream = None;

                return Poll::Ready(match load_to_stream_sync(&mut self.one_shot_stream) {
                    Ok(_) => self.one_shot_stream.poll_next(),
                    Err(e) => Some(Err(e.into())),
                });
            }
        }

        next
    }
}

impl Stream for CubeScanStreamRouter {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // Emit the leftovers of an already split batch before pulling more data
        if let Some(batch) = self.splitter.next_pending() {
            return Poll::Ready(Some(Ok(batch)));
        }

        match self.poll_next_batch(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(self.splitter.split(batch))),
            next => next,
        }
    }
}

impl RecordBatchStream for CubeScanStreamRouter {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

async fn load_data(
    span_id: Option<Arc<SpanId>>,
    request: V1LoadRequestQuery,
    auth_context: AuthContextRef,
    transport: Arc<dyn TransportService>,
    meta: LoadRequestMeta,
    schema: SchemaRef,
    member_fields: Vec<MemberField>,
    options: CubeScanOptions,
    sql_query: Option<SqlQuery>,
) -> ArrowResult<Vec<RecordBatch>> {
    let no_members_query = request.measures.as_ref().map(|v| v.len()).unwrap_or(0) == 0
        && request.dimensions.as_ref().map(|v| v.len()).unwrap_or(0) == 0
        && request
            .time_dimensions
            .as_ref()
            .map(|v| v.iter().filter(|d| d.granularity.is_some()).count())
            .unwrap_or(0)
            == 0;

    let result = if no_members_query {
        let limit = request.limit.unwrap_or(1);

        let mut response = LiteralRowsValueObject {
            row_count: limit as usize,
        };
        let rec = transform_response(&mut response, schema.clone(), &member_fields)
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

        rec
    } else {
        let result = transport
            .load(
                span_id.clone(),
                request,
                sql_query,
                auth_context,
                meta,
                schema,
                member_fields,
                options.cache_mode,
                options.throw_continue_wait,
            )
            .await
            .map_err(|mut err| {
                // Remove `Error: ` prefix that can come from database
                err.message = if let Some(message) = err.message.strip_prefix("Error: ") {
                    message.to_string()
                } else {
                    err.message
                };

                if err.is_continue_wait() {
                    err.cause = CubeErrorCauseType::ContinueWait;
                } else {
                    err.cause = CubeErrorCauseType::DatabaseExecution(err.cause.meta().cloned());
                }

                ArrowError::ExternalError(Box::new(err))
            })?;

        if let Some(data) = result.into_iter().next() {
            // Mirror the result freshness metadata onto the span. The schema
            // metadata only survives while this scan's batch is the root of the
            // plan — any post-processing DataFusion node on top (a calculated
            // projection over MEASURE(), a sort, a filter) builds its own schema
            // and drops it. The span outlives the whole plan, so consumers that
            // report freshness read it from there instead.
            if let Some(span_id) = &span_id {
                let schema = data.schema();
                let metadata = schema.metadata();

                if let Some(last_refresh_time) = metadata.get("lastRefreshTime") {
                    if let Ok(last_refresh_time) = DateTime::parse_from_rfc3339(last_refresh_time) {
                        span_id
                            .set_last_refresh_time(last_refresh_time.with_timezone(&Utc))
                            .await;
                    }
                }

                span_id
                    .set_external(
                        metadata
                            .get("external")
                            .map(|v| v == "true")
                            .unwrap_or(false),
                    )
                    .await;

                if let Some(used_pre_aggregations) = metadata
                    .get("usedPreAggregations")
                    .map(String::as_str)
                    .and_then(parse_used_pre_aggregations)
                {
                    span_id
                        .merge_used_pre_aggregations(used_pre_aggregations)
                        .await;
                }
            }

            match (options.max_records, data.num_rows()) {
                (Some(max_records), len) if len >= max_records => {
                    return Err(ArrowError::ExternalError(Box::new(CubeError::user(
                        format!(
                            "One of the Cube queries exceeded the maximum row limit ({}). \
                            JOIN/UNION is not possible as it will produce incorrect results. \
                            Try filtering the results more precisely \
                            or moving post-processing functions to an outer query.",
                            max_records
                        ),
                    ))));
                }
                (_, _) => (),
            }

            data
        } else {
            return Err(ArrowError::ExternalError(Box::new(CubeError::internal(
                "Unable to extract results from response: results is empty".to_string(),
            ))));
        }
    };

    Ok(vec![result])
}

fn load_to_stream_sync(one_shot_stream: &mut CubeScanOneShotStream) -> Result<()> {
    let span_id = one_shot_stream.span_id.clone();
    let req = one_shot_stream.request.clone();
    let auth = one_shot_stream.auth_context.clone();
    let transport = one_shot_stream.transport.clone();
    let meta = one_shot_stream.meta.clone();
    let schema = one_shot_stream.schema.clone();
    let member_fields = one_shot_stream.member_fields.clone();
    let options = one_shot_stream.options.clone();
    let wrapped_sql = one_shot_stream.wrapped_sql.clone();

    let handle = tokio::runtime::Handle::current();
    let res = std::thread::spawn(move || {
        handle.block_on(load_data(
            span_id,
            req,
            auth,
            transport,
            meta,
            schema,
            member_fields,
            options,
            wrapped_sql,
        ))
    })
    .join()
    .map_err(|_| DataFusionError::Execution(format!("Can't load to stream")))??;

    let response = res.first();
    one_shot_stream.data = Some(response.cloned().unwrap());

    Ok(())
}

pub fn transform_response<C: ColumnarValueObject>(
    response: &mut C,
    schema: SchemaRef,
    member_fields: &[MemberField],
) -> std::result::Result<RecordBatch, CubeError> {
    check_member_fields(&schema, member_fields)?;
    let len = response.len()?;
    let mut columns = Vec::with_capacity(schema.fields().len());

    for (i, schema_field) in schema.fields().iter().enumerate() {
        // A NULL column is never read, its values can't change it
        if schema_field.data_type() == &DataType::Null {
            columns.push(Arc::new(NullArray::new(len)) as ArrayRef);
            continue;
        }

        let mut builder = ColumnBuilder::try_new(schema_field.data_type(), len)?;
        match &member_fields[i] {
            MemberField::Member(member) => {
                builder.append_column(response.column(&member.field_name)?)?;
            }
            MemberField::Literal(value) => builder.append_literal(value, len)?,
        }

        columns.push(builder.finish());
    }

    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Reads a `{"members", "columns"}` chunk straight into Arrow builders, matching
/// `JsonColumnarValueObject` + `transform_response`. `rows_hint` sizes the first column's
/// builders, read before the row count is known (the previous chunk's count in a stream).
pub fn deserialize_columnar_chunk(
    chunk: &[u8],
    schema: SchemaRef,
    member_fields: &[MemberField],
    rows_hint: usize,
) -> std::result::Result<RecordBatch, CubeError> {
    check_member_fields(&schema, member_fields)?;
    let mut state = ChunkState {
        schema: &schema,
        member_fields,
        members: None,
        columns: None,
        targets: vec![],
        builders: (0..member_fields.len()).map(|_| None).collect(),
        rows: None,
        rows_hint,
        error: None,
    };

    let mut deserializer = serde_json::Deserializer::from_slice(chunk);
    serde::de::DeserializeSeed::deserialize(&mut state, &mut deserializer)
        .and_then(|_| deserializer.end())
        .map_err(|e| {
            state.error.take().unwrap_or_else(|| {
                CubeError::internal(format!("Can't parse columnar chunk JSON: {}", e))
            })
        })?;

    state.finish()
}

/// Every schema field is read through the member field at the same index.
fn check_member_fields(
    schema: &SchemaRef,
    member_fields: &[MemberField],
) -> std::result::Result<(), CubeError> {
    if schema.fields().len() != member_fields.len() {
        return Err(CubeError::internal(format!(
            "Response schema has {} fields, but {} member fields map them",
            schema.fields().len(),
            member_fields.len()
        )));
    }

    Ok(())
}

struct ChunkState<'a> {
    schema: &'a SchemaRef,
    member_fields: &'a [MemberField],
    members: Option<Vec<String>>,
    /// `columns` that came before `members`, read as values since nothing maps them yet
    columns: Option<Vec<Vec<Value>>>,
    /// Schema fields each response column is read into, by column index
    targets: Vec<Vec<usize>>,
    /// Builder per schema field, created when its column is read
    builders: Vec<Option<ColumnBuilder>>,
    rows: Option<usize>,
    rows_hint: usize,
    /// The error behind a failed deserialization, kept as is instead of a serde message
    error: Option<CubeError>,
}

impl<'a> ChunkState<'a> {
    fn set_members(&mut self, members: Vec<String>) {
        self.targets = vec![vec![]; members.len()];
        for (field_idx, member_field) in self.member_fields.iter().enumerate() {
            let MemberField::Member(member) = member_field else {
                continue;
            };
            // NULL columns are never read, `finish` fills them
            if self.schema.field(field_idx).data_type() == &DataType::Null {
                continue;
            }
            // The first column of a repeated member wins, as in `JsonColumnarValueObject`
            if let Some(idx) = members.iter().position(|m| m == &member.field_name) {
                self.targets[idx].push(field_idx);
            }
        }

        self.members = Some(members);
    }

    fn fail<E: serde::de::Error>(&mut self, error: CubeError) -> E {
        let e = E::custom(&error.message);
        self.error = Some(error);
        e
    }

    fn finish(self) -> std::result::Result<RecordBatch, CubeError> {
        let Some(members) = self.members else {
            return Err(CubeError::internal(
                "Can't parse columnar chunk JSON: missing field `members`".to_string(),
            ));
        };
        if let Some(columns) = self.columns {
            let mut response = JsonColumnarValueObject::try_new(members, columns)?;
            return transform_response(&mut response, self.schema.clone(), self.member_fields);
        }
        let Some(rows) = self.rows else {
            return Err(CubeError::internal(
                "Can't parse columnar chunk JSON: missing field `columns`".to_string(),
            ));
        };

        let mut columns = Vec::with_capacity(self.schema.fields().len());
        for (i, (schema_field, builder)) in
            self.schema.fields().iter().zip(self.builders).enumerate()
        {
            if schema_field.data_type() == &DataType::Null {
                columns.push(Arc::new(NullArray::new(rows)) as ArrayRef);
                continue;
            }

            let builder = match builder {
                Some(builder) => builder,
                None => {
                    let mut builder = ColumnBuilder::try_new(schema_field.data_type(), rows)?;
                    match &self.member_fields[i] {
                        MemberField::Member(member) => {
                            if members.iter().any(|m| m == &member.field_name) {
                                return Err(CubeError::internal(format!(
                                    "Unexpected response from Cube, missing column for '{}'",
                                    member.field_name
                                )));
                            }
                            // A missing field is treated as a column of NULLs
                            builder.append_nulls(rows)?
                        }
                        MemberField::Literal(value) => builder.append_literal(value, rows)?,
                    }
                    builder
                }
            };

            columns.push(builder.finish());
        }

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "lowercase")]
enum ChunkKey {
    Members,
    Columns,
    #[serde(other)]
    Other,
}

impl<'de, 'a, 'b> serde::de::DeserializeSeed<'de> for &'b mut ChunkState<'a> {
    type Value = ();

    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<(), D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de, 'a, 'b> serde::de::Visitor<'de> for &'b mut ChunkState<'a> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("columnar chunk object")
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(
        self,
        mut map: A,
    ) -> std::result::Result<(), A::Error> {
        while let Some(key) = map.next_key::<ChunkKey>()? {
            match key {
                ChunkKey::Members => {
                    if self.members.is_some() {
                        return Err(serde::de::Error::duplicate_field("members"));
                    }
                    let members = map.next_value::<Vec<String>>()?;
                    self.set_members(members);
                }
                ChunkKey::Columns => {
                    if self.rows.is_some() || self.columns.is_some() {
                        return Err(serde::de::Error::duplicate_field("columns"));
                    }
                    if self.members.is_some() {
                        map.next_value_seed(ColumnsSeed(&mut *self))?;
                        self.rows.get_or_insert(0);
                    } else {
                        self.columns = Some(map.next_value::<Vec<Vec<Value>>>()?);
                    }
                }
                ChunkKey::Other => {
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }

        Ok(())
    }
}

struct ColumnsSeed<'b, 'a>(&'b mut ChunkState<'a>);

impl<'de, 'b, 'a> serde::de::DeserializeSeed<'de> for ColumnsSeed<'b, 'a> {
    type Value = ();

    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<(), D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de, 'b, 'a> serde::de::Visitor<'de> for ColumnsSeed<'b, 'a> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("array of columns")
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(
        self,
        mut seq: A,
    ) -> std::result::Result<(), A::Error> {
        let mut idx = 0;
        while let Some(()) = seq.next_element_seed(ColumnSeed {
            state: &mut *self.0,
            idx,
        })? {
            idx += 1;
        }

        Ok(())
    }
}

struct ColumnSeed<'b, 'a> {
    state: &'b mut ChunkState<'a>,
    idx: usize,
}

impl<'de, 'b, 'a> serde::de::DeserializeSeed<'de> for ColumnSeed<'b, 'a> {
    type Value = ();

    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<(), D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de, 'b, 'a> serde::de::Visitor<'de> for ColumnSeed<'b, 'a> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("array of column values")
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(
        self,
        mut seq: A,
    ) -> std::result::Result<(), A::Error> {
        let state = self.state;
        // Columns past the members, or members no schema field reads, are only counted
        let targets = state
            .targets
            .get_mut(self.idx)
            .map(std::mem::take)
            .unwrap_or_default();

        let mut builders = Vec::with_capacity(targets.len());
        for &field_idx in &targets {
            let data_type = state.schema.field(field_idx).data_type();
            match ColumnBuilder::try_new(data_type, state.rows.unwrap_or(state.rows_hint)) {
                Ok(builder) => builders.push(builder),
                Err(e) => return Err(state.fail(e)),
            }
        }

        let mut len = 0;
        if builders.is_empty() {
            while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                len += 1;
            }
        } else {
            while let Some(value) = seq.next_element_seed(FieldValueSeed)? {
                let (last, rest) = builders.split_last_mut().unwrap();
                for builder in rest {
                    if let Err(e) = builder.append(value.clone()) {
                        return Err(state.fail(e));
                    }
                }
                if let Err(e) = last.append(value) {
                    return Err(state.fail(e));
                }
                len += 1;
            }
        }

        match state.rows {
            None => state.rows = Some(len),
            Some(expected) if expected != len => {
                return Err(state.fail(CubeError::internal(format!(
                    "columnar response has ragged columns: column {} has {} rows, expected {}",
                    self.idx, len, expected
                ))));
            }
            Some(_) => {}
        }

        for (field_idx, builder) in targets.into_iter().zip(builders) {
            state.builders[field_idx] = Some(builder);
        }

        Ok(())
    }
}

struct FieldValueSeed;

impl<'de> serde::de::DeserializeSeed<'de> for FieldValueSeed {
    type Value = FieldValue<'de>;

    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<FieldValue<'de>, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> serde::de::Visitor<'de> for FieldValueSeed {
    type Value = FieldValue<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("JSON value")
    }

    fn visit_bool<E>(self, v: bool) -> std::result::Result<FieldValue<'de>, E> {
        Ok(FieldValue::Bool(v))
    }

    fn visit_i64<E>(self, v: i64) -> std::result::Result<FieldValue<'de>, E> {
        Ok(FieldValue::Number(v as f64))
    }

    fn visit_u64<E>(self, v: u64) -> std::result::Result<FieldValue<'de>, E> {
        Ok(FieldValue::Number(v as f64))
    }

    fn visit_f64<E>(self, v: f64) -> std::result::Result<FieldValue<'de>, E> {
        Ok(FieldValue::Number(v))
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> std::result::Result<FieldValue<'de>, E> {
        Ok(FieldValue::String(Cow::Borrowed(v)))
    }

    fn visit_str<E>(self, v: &str) -> std::result::Result<FieldValue<'de>, E> {
        Ok(FieldValue::String(Cow::Owned(v.to_string())))
    }

    fn visit_string<E>(self, v: String) -> std::result::Result<FieldValue<'de>, E> {
        Ok(FieldValue::String(Cow::Owned(v)))
    }

    fn visit_unit<E>(self) -> std::result::Result<FieldValue<'de>, E> {
        Ok(FieldValue::Null)
    }

    fn visit_none<E>(self) -> std::result::Result<FieldValue<'de>, E> {
        Ok(FieldValue::Null)
    }

    // Arrays and objects are rare and serialized back to JSON, as `json_value_to_field_value` does
    fn visit_seq<A: serde::de::SeqAccess<'de>>(
        self,
        seq: A,
    ) -> std::result::Result<FieldValue<'de>, A::Error> {
        let value = Value::deserialize(serde::de::value::SeqAccessDeserializer::new(seq))?;
        Ok(FieldValue::String(Cow::Owned(value.to_string())))
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(
        self,
        map: A,
    ) -> std::result::Result<FieldValue<'de>, A::Error> {
        let value = Value::deserialize(serde::de::value::MapAccessDeserializer::new(map))?;
        Ok(FieldValue::String(Cow::Owned(value.to_string())))
    }
}

/// Result metadata of a single load response, as reported by the API gateway.
/// Grouped in one struct so every value is named at the call site instead of
/// riding along as a positional argument.
#[derive(Debug, Clone, Default)]
pub struct ResultMetadata {
    pub last_refresh_time: Option<String>,
    /// `true` when the result was served from an external (CubeStore)
    /// pre-aggregation.
    pub external: bool,
    /// `usedPreAggregations` object of the load response, passed through
    /// verbatim.
    pub used_pre_aggregations: Option<serde_json::Value>,
}

/// Builds a schema with `lastRefreshTime` / `external` / `usedPreAggregations`
/// metadata.
///
/// `lastRefreshTime` is passed through unchanged. The `external` marker is
/// added when the flag is set so downstream code can tell that the result
/// was served from an external (CubeStore) pre-aggregation — the case
/// where cubesql's own cache-freshness decisions actually need to look at
/// the pre-agg refresh, as internal pre-aggregations hit the source DB
/// and rely on its own caching. `usedPreAggregations` rides along the same
/// way, JSON-encoded because Arrow schema metadata is a string map; it names
/// the pre-aggregations behind the result so a client can match it to a build.
pub fn build_response_schema(schema: &SchemaRef, result_metadata: &ResultMetadata) -> SchemaRef {
    let used_pre_aggregations = result_metadata
        .used_pre_aggregations
        .as_ref()
        .filter(|v| is_reportable_used_pre_aggregations(v))
        .and_then(|v| serde_json::to_string(v).ok());

    if result_metadata.last_refresh_time.is_none()
        && !result_metadata.external
        && used_pre_aggregations.is_none()
    {
        return schema.clone();
    }

    let mut metadata = schema.metadata().clone();
    if let Some(t) = &result_metadata.last_refresh_time {
        metadata.insert("lastRefreshTime".to_string(), t.clone());
    }
    if result_metadata.external {
        metadata.insert("external".to_string(), "true".to_string());
    }
    if let Some(used_pre_aggregations) = used_pre_aggregations {
        metadata.insert("usedPreAggregations".to_string(), used_pre_aggregations);
    }

    Arc::new(Schema::new_with_metadata(
        schema.fields().to_vec(),
        metadata,
    ))
}

/// Whether a `usedPreAggregations` value is worth passing on: a query that hit
/// no pre-aggregation reports an empty object, and anything that is not an
/// object at all is not something a reader can merge into a span or hand to a
/// client. The API gateway is the only writer and always sends an object, so
/// the type check is defensive.
fn is_reportable_used_pre_aggregations(value: &serde_json::Value) -> bool {
    matches!(value, serde_json::Value::Object(map) if !map.is_empty())
}

/// Reads the `usedPreAggregations` schema metadata written by
/// `build_response_schema` back into a value, applying the same
/// nothing-to-report normalization so that every reader of the metadata agrees
/// on it - the writer and the readers live in different crates. A blob that
/// does not parse is reported and dropped rather than failing the query: the
/// metadata is reporting only, and no result depends on it.
pub fn parse_used_pre_aggregations(encoded: &str) -> Option<serde_json::Value> {
    match serde_json::from_str::<serde_json::Value>(encoded) {
        Ok(value) if is_reportable_used_pre_aggregations(&value) => Some(value),
        Ok(_) => None,
        Err(e) => {
            warn!(
                "Unable to parse usedPreAggregations of a load response: {}",
                e
            );
            None
        }
    }
}

pub fn convert_transport_response(
    response: V1LoadResponse<V1LoadResultDataColumnar>,
    schema: SchemaRef,
    member_fields: Vec<MemberField>,
) -> std::result::Result<Vec<RecordBatch>, CubeError> {
    response
        .results
        .into_iter()
        .map(|result| {
            let V1LoadResult {
                data,
                last_refresh_time,
                external,
                used_pre_aggregations,
                ..
            } = result;
            let V1LoadResultDataColumnar { members, columns } = data;

            let mut response = JsonColumnarValueObject::try_new(members, columns)?;
            let updated_schema = build_response_schema(
                &schema,
                &ResultMetadata {
                    last_refresh_time,
                    external: external.unwrap_or(false),
                    used_pre_aggregations,
                },
            );

            transform_response(&mut response, updated_schema, &member_fields)
        })
        .collect::<std::result::Result<Vec<RecordBatch>, CubeError>>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        compile::{engine::df::wrapper::SqlQuery, DatabaseProtocol, DatabaseProtocolDetails},
        sql::HttpAuthContext,
        transport::{MetaContext, SqlResponse},
        CubeError,
    };
    use cubeclient::models::{V1LoadResponse, V1LoadResultDataColumnar};
    use datafusion::{
        arrow::{
            array::{
                Array, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
                TimestampNanosecondArray,
            },
            datatypes::{Field, Schema},
        },
        execution::{
            context::TaskContext,
            runtime_env::{RuntimeConfig, RuntimeEnv},
        },
        physical_plan::common,
        scalar::ScalarValue,
    };
    use std::{collections::HashMap, result::Result};

    fn build_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, true)]))
    }

    #[test]
    fn build_response_schema_no_metadata_when_nothing_to_add() {
        let schema = build_schema();
        let updated = build_response_schema(&schema, &ResultMetadata::default());
        assert!(updated.metadata().is_empty());
    }

    #[test]
    fn build_response_schema_passes_through_last_refresh_time() {
        let schema = build_schema();
        let updated = build_response_schema(
            &schema,
            &ResultMetadata {
                last_refresh_time: Some("2024-01-01T00:00:00.000Z".to_string()),
                ..Default::default()
            },
        );
        assert_eq!(
            updated.metadata().get("lastRefreshTime"),
            Some(&"2024-01-01T00:00:00.000Z".to_string())
        );
        assert!(updated.metadata().get("external").is_none());
        assert!(updated.metadata().get("usedPreAggregations").is_none());
    }

    #[test]
    fn build_response_schema_passes_through_last_refresh_time_for_external() {
        // External (CubeStore) flag must NOT alter `lastRefreshTime` — it's
        // passed through unchanged. The marker reports the external hit.
        let schema = build_schema();
        let stale = "2000-01-01T00:00:00.000Z".to_string();
        let updated = build_response_schema(
            &schema,
            &ResultMetadata {
                last_refresh_time: Some(stale.clone()),
                external: true,
                ..Default::default()
            },
        );

        assert_eq!(updated.metadata().get("lastRefreshTime"), Some(&stale));
        assert_eq!(
            updated.metadata().get("external"),
            Some(&"true".to_string())
        );
    }

    #[test]
    fn build_response_schema_sets_only_marker_when_no_last_refresh_time() {
        let schema = build_schema();
        // No incoming last_refresh_time, but external flag is set — emit
        // only the marker; do NOT synthesize a `lastRefreshTime`.
        let updated = build_response_schema(
            &schema,
            &ResultMetadata {
                external: true,
                ..Default::default()
            },
        );
        assert!(updated.metadata().get("lastRefreshTime").is_none());
        assert_eq!(
            updated.metadata().get("external"),
            Some(&"true".to_string())
        );
    }

    #[test]
    fn build_response_schema_json_encodes_used_pre_aggregations() {
        // Arrow schema metadata is a string map, so the object travels as JSON
        // and has to come back out of it unchanged.
        let schema = build_schema();
        let used_pre_aggregations = serde_json::json!({
            "schema.orders_main20240101": {
                "preAggregationId": "Orders.main",
                "targetTableName": "schema.orders_main20240101_abc_def_1712",
                "lastUpdatedAt": 1712000000000u64,
                "type": "rollup",
            }
        });
        let updated = build_response_schema(
            &schema,
            &ResultMetadata {
                used_pre_aggregations: Some(used_pre_aggregations.clone()),
                ..Default::default()
            },
        );

        let encoded = updated.metadata().get("usedPreAggregations").unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(encoded).unwrap(),
            used_pre_aggregations
        );
    }

    #[test]
    fn parse_used_pre_aggregations_normalizes_nothing_to_report() {
        // Same normalization as the writer applies, so a reader of the metadata
        // never has to special-case an empty object or a null
        assert_eq!(parse_used_pre_aggregations("null"), None);
        assert_eq!(parse_used_pre_aggregations("{}"), None);
        // Nothing a reader could merge or report, however well-formed
        assert_eq!(parse_used_pre_aggregations("\"nonsense\""), None);
        assert_eq!(parse_used_pre_aggregations("[]"), None);
        // Not JSON at all - reported and dropped, never fatal
        assert_eq!(parse_used_pre_aggregations("{oops"), None);

        let used_pre_aggregations = serde_json::json!({
            "schema.orders_main": { "preAggregationId": "Orders.main" }
        });
        assert_eq!(
            parse_used_pre_aggregations(&used_pre_aggregations.to_string()),
            Some(used_pre_aggregations)
        );
    }

    #[test]
    fn build_response_schema_skips_unreportable_used_pre_aggregations() {
        // A query that hit no pre-aggregation reports an empty object; passing
        // that on would make every plain SQL result carry a useless key. The
        // same goes for a value no reader could merge.
        let schema = build_schema();
        for used_pre_aggregations in [
            serde_json::json!({}),
            serde_json::Value::Null,
            serde_json::json!("nonsense"),
        ] {
            let updated = build_response_schema(
                &schema,
                &ResultMetadata {
                    used_pre_aggregations: Some(used_pre_aggregations),
                    ..Default::default()
                },
            );

            assert!(updated.metadata().is_empty());
        }
    }

    /// Collects everything a splitter produces for a single input batch: the chunk
    /// returned by `split` plus every buffered leftover, as row values.
    fn split_to_rows(splitter: &mut RecordBatchSplitter, batch: RecordBatch) -> Vec<Vec<i64>> {
        let to_rows = |batch: &RecordBatch| {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 column");
            (0..batch.num_rows()).map(|i| column.value(i)).collect()
        };

        let mut chunks = vec![to_rows(&splitter.split(batch).unwrap())];
        while let Some(pending) = splitter.next_pending() {
            chunks.push(to_rows(&pending));
        }

        chunks
    }

    fn build_int64_batch(rows: usize) -> RecordBatch {
        RecordBatch::try_new(
            build_schema(),
            vec![Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())) as ArrayRef],
        )
        .unwrap()
    }

    #[test]
    fn record_batch_splitter_passes_through_batches_within_limit() {
        let mut splitter = RecordBatchSplitter::new(5);

        assert_eq!(
            split_to_rows(&mut splitter, build_int64_batch(5)),
            vec![vec![0, 1, 2, 3, 4]]
        );
        assert_eq!(
            split_to_rows(&mut splitter, build_int64_batch(0)),
            vec![Vec::<i64>::new()]
        );
    }

    #[test]
    fn record_batch_splitter_splits_oversized_batches_preserving_row_order() {
        let mut splitter = RecordBatchSplitter::new(2);

        // Uneven split: the trailing chunk holds the remainder
        assert_eq!(
            split_to_rows(&mut splitter, build_int64_batch(5)),
            vec![vec![0, 1], vec![2, 3], vec![4]]
        );

        // Even split: no short trailing chunk
        assert_eq!(
            split_to_rows(&mut splitter, build_int64_batch(4)),
            vec![vec![0, 1], vec![2, 3]]
        );
    }

    #[test]
    fn record_batch_splitter_disabled_with_zero_max_rows() {
        let mut splitter = RecordBatchSplitter::new(0);

        assert_eq!(
            split_to_rows(&mut splitter, build_int64_batch(3)),
            vec![vec![0, 1, 2]]
        );
    }

    #[tokio::test]
    async fn test_df_cube_scan_execute_splits_batches() -> Result<(), CubeError> {
        // The test transport returns all 5 rows as a single batch; the splitter
        // must break that into 3 batches of at most 2 rows each.
        let scan_node = CubeScanExecutionPlan {
            config_obj: crate::config::Config::test()
                .update_config(|mut config| {
                    config.cube_scan_max_batch_rows = 2;
                    config
                })
                .config_obj(),
            ..build_test_scan_node()
        };

        let batches =
            common::collect(scan_node.execute(0, build_test_task_context()?).await?).await?;

        assert_eq!(
            batches.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
            vec![2, 2, 1]
        );

        Ok(())
    }

    #[test]
    fn convert_transport_response_threads_external_flag_into_schema_metadata() {
        // End-to-end coverage of `convert_transport_response`: a columnar
        // V1LoadResponse with `external: true` and a `lastRefreshTime` should
        // produce a RecordBatch whose schema metadata has the `external` marker
        // set and `lastRefreshTime` passed through unchanged.
        let raw = r#"
            {
                "results": [{
                    "annotation": {
                        "measures": [],
                        "dimensions": [],
                        "segments": [],
                        "timeDimensions": []
                    },
                    "data": {"members": ["c"], "columns": [[1]]},
                    "lastRefreshTime": "2000-01-01T00:00:00.000Z",
                    "external": true
                }]
            }
        "#;
        let response: V1LoadResponse<V1LoadResultDataColumnar> = serde_json::from_str(raw).unwrap();
        let schema = build_schema();
        let member_fields = vec![MemberField::regular("c".to_string())];
        let batches = convert_transport_response(response, schema, member_fields).unwrap();
        let metadata = batches[0].schema().metadata().clone();

        assert_eq!(metadata.get("external"), Some(&"true".to_string()));
        assert_eq!(
            metadata.get("lastRefreshTime"),
            Some(&"2000-01-01T00:00:00.000Z".to_string())
        );
    }

    #[test]
    fn convert_transport_response_serializes_non_scalar_values_to_json_strings() {
        let raw = r#"
            {
                "results": [{
                    "annotation": {
                        "measures": [],
                        "dimensions": [],
                        "segments": [],
                        "timeDimensions": []
                    },
                    "data": {
                        "members": ["c", "d"],
                        "columns": [
                            [["a", "b"], null],
                            [{"k": 1}, "plain"]
                        ]
                    }
                }]
            }
        "#;
        let response: V1LoadResponse<V1LoadResultDataColumnar> = serde_json::from_str(raw).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Utf8, true),
            Field::new("d", DataType::Utf8, true),
        ]));
        let member_fields = vec![
            MemberField::regular("c".to_string()),
            MemberField::regular("d".to_string()),
        ];
        let batches = convert_transport_response(response, schema, member_fields).unwrap();

        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(c.value(0), r#"["a","b"]"#);
        assert!(c.is_null(1));

        let d = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(d.value(0), r#"{"k":1}"#);
        assert_eq!(d.value(1), "plain");
    }

    fn get_test_load_meta(protocol: DatabaseProtocol) -> LoadRequestMeta {
        LoadRequestMeta::new(
            protocol.get_name().to_string(),
            "sql".to_string(),
            Some("SQL API Unit Testing".to_string()),
        )
    }

    fn get_test_transport() -> Arc<dyn TransportService> {
        #[derive(Debug)]
        struct TestConnectionTransport {}

        #[async_trait]
        impl TransportService for TestConnectionTransport {
            // Load meta information about cubes
            async fn meta(&self, _ctx: AuthContextRef) -> Result<Arc<MetaContext>, CubeError> {
                panic!("It's a fake transport");
            }

            async fn sql(
                &self,
                _span_id: Option<Arc<SpanId>>,
                _query: V1LoadRequestQuery,
                _ctx: AuthContextRef,
                _meta_fields: LoadRequestMeta,
                _member_to_alias: Option<HashMap<String, String>>,
                _expression_params: Option<Vec<Option<String>>>,
            ) -> Result<SqlResponse, CubeError> {
                todo!()
            }

            async fn pre_aggregation_matches(
                &self,
                _span_id: Option<Arc<SpanId>>,
                _queries: Vec<V1LoadRequestQuery>,
                _ctx: AuthContextRef,
                _meta_fields: LoadRequestMeta,
            ) -> Result<Vec<bool>, CubeError> {
                todo!()
            }

            // Execute load query
            async fn load(
                &self,
                _span_id: Option<Arc<SpanId>>,
                _query: V1LoadRequestQuery,
                _sql_query: Option<SqlQuery>,
                _ctx: AuthContextRef,
                _meta_fields: LoadRequestMeta,
                schema: SchemaRef,
                member_fields: Vec<MemberField>,
                _cache_mode: Option<CacheMode>,
                _throw_continue_wait: bool,
            ) -> Result<Vec<RecordBatch>, CubeError> {
                let response = r#"
                {
                    "results": [{
                        "annotation": {
                            "measures": [],
                            "dimensions": [],
                            "segments": [],
                            "timeDimensions": []
                        },
                        "data": {
                            "members": [
                                "KibanaSampleDataEcommerce.count",
                                "KibanaSampleDataEcommerce.maxPrice",
                                "KibanaSampleDataEcommerce.isBool",
                                "KibanaSampleDataEcommerce.orderTimestamp",
                                "KibanaSampleDataEcommerce.orderDate",
                                "KibanaSampleDataEcommerce.city"
                            ],
                            "columns": [
                                [null, 5, "5", null, null],
                                [null, 5.05, "5.05", null, null],
                                [null, true, false, "true", "false"],
                                [null, "2022-01-01 00:00:00.000", "2023-01-01 00:00:00.000", "9999-12-31 00:00:00.000", null],
                                [null, "2022-01-01", "2023-01-01", "9999-12-31", null],
                                ["City 1", "City 2", "City 3", "City 4", null]
                            ]
                        }
                    }]
                }
                "#;

                let result: V1LoadResponse<V1LoadResultDataColumnar> =
                    serde_json::from_str(response).unwrap();
                convert_transport_response(result, schema.clone(), member_fields)
            }

            async fn load_stream(
                &self,
                _span_id: Option<Arc<SpanId>>,
                _query: V1LoadRequestQuery,
                _sql_query: Option<SqlQuery>,
                _ctx: AuthContextRef,
                _meta_fields: LoadRequestMeta,
                _schema: SchemaRef,
                _member_fields: Vec<MemberField>,
                _throw_continue_wait: bool,
            ) -> Result<CubeStreamReceiver, CubeError> {
                panic!("It's a fake transport");
            }

            async fn can_switch_user_for_session(
                &self,
                _ctx: AuthContextRef,
                _to_user: String,
            ) -> Result<bool, CubeError> {
                panic!("It's a fake transport");
            }

            async fn log_load_state(
                &self,
                span_id: Option<Arc<SpanId>>,
                ctx: AuthContextRef,
                meta_fields: LoadRequestMeta,
                event: String,
                properties: serde_json::Value,
            ) -> Result<(), CubeError> {
                println!(
                    "Load state: {:?} {:?} {:?} {} {:?}",
                    span_id, ctx, meta_fields, event, properties
                );
                Ok(())
            }
        }

        Arc::new(TestConnectionTransport {})
    }

    fn build_test_scan_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("KibanaSampleDataEcommerce.count", DataType::Utf8, true),
            Field::new("KibanaSampleDataEcommerce.count", DataType::Utf8, true),
            Field::new(
                "KibanaSampleDataEcommerce.maxPrice",
                DataType::Float64,
                true,
            ),
            Field::new(
                "KibanaSampleDataEcommerce.orderTimestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("KibanaSampleDataEcommerce.isBool", DataType::Boolean, true),
            Field::new(
                "KibanaSampleDataEcommerce.is_female",
                DataType::Boolean,
                true,
            ),
            Field::new("KibanaSampleDataEcommerce.city", DataType::Utf8, true),
            Field::new(
                "KibanaSampleDataEcommerce.orderDate",
                DataType::Date32,
                true,
            ),
        ]))
    }

    fn build_test_task_context() -> Result<Arc<TaskContext>, CubeError> {
        let runtime = Arc::new(RuntimeEnv::new(RuntimeConfig::new())?);

        Ok(Arc::new(TaskContext::new(
            "test".to_string(),
            "session".to_string(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            runtime,
        )))
    }

    fn build_test_scan_node() -> CubeScanExecutionPlan {
        let schema = build_test_scan_schema();

        CubeScanExecutionPlan {
            schema: schema.clone(),
            member_fields: schema
                .fields()
                .iter()
                .map(|f| {
                    if f.name() == "KibanaSampleDataEcommerce.is_female" {
                        MemberField::Literal(ScalarValue::Boolean(None))
                    } else {
                        MemberField::regular(f.name().to_string())
                    }
                })
                .collect(),
            request: V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                ]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.isBool".to_string(),
                    "KibanaSampleDataEcommerce.orderTimestamp".to_string(),
                    "KibanaSampleDataEcommerce.orderDate".to_string(),
                    "KibanaSampleDataEcommerce.city".to_string(),
                ]),
                ..Default::default()
            },
            wrapped_sql: None,
            auth_context: Arc::new(HttpAuthContext {
                access_token: "access_token".to_string(),
                base_path: "base_path".to_string(),
            }),
            options: CubeScanOptions {
                change_user: None,
                max_records: None,
                cache_mode: None,
                throw_continue_wait: false,
                pre_aggregation: false,
            },
            transport: get_test_transport(),
            meta: get_test_load_meta(DatabaseProtocol::PostgreSQL),
            span_id: None,
            config_obj: crate::config::Config::test().config_obj(),
        }
    }

    #[tokio::test]
    async fn test_df_cube_scan_execute() -> Result<(), CubeError> {
        assert_eq!(std::mem::size_of::<FieldValue>(), 24);

        let scan_node = build_test_scan_node();
        let schema = scan_node.schema.clone();

        let stream = scan_node.execute(0, build_test_task_context()?).await?;
        let batches = common::collect(stream).await?;

        assert_eq!(
            batches[0],
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![
                        None,
                        Some("5"),
                        Some("5"),
                        None,
                        None
                    ])) as ArrayRef,
                    Arc::new(StringArray::from(vec![
                        None,
                        Some("5"),
                        Some("5"),
                        None,
                        None
                    ])) as ArrayRef,
                    Arc::new(Float64Array::from(vec![
                        None,
                        Some(5.05),
                        Some(5.05),
                        None,
                        None
                    ])) as ArrayRef,
                    Arc::new(TimestampNanosecondArray::from(vec![
                        None,
                        Some(1640995200000000000),
                        Some(1672531200000000000),
                        None,
                        None
                    ])) as ArrayRef,
                    Arc::new(BooleanArray::from(vec![
                        None,
                        Some(true),
                        Some(false),
                        Some(true),
                        Some(false)
                    ])) as ArrayRef,
                    Arc::new(BooleanArray::from(vec![None, None, None, None, None,])) as ArrayRef,
                    Arc::new(StringArray::from(vec![
                        Some("City 1"),
                        Some("City 2"),
                        Some("City 3"),
                        Some("City 4"),
                        None
                    ])) as ArrayRef,
                    Arc::new(Date32Array::from(vec![
                        None,
                        Some(18993),
                        Some(19358),
                        Some(2_932_896),
                        None,
                    ])) as ArrayRef,
                ],
            )?
        );

        Ok(())
    }

    /// `deserialize_columnar_chunk` must read a chunk exactly like the `serde_json::Value` path
    fn assert_chunk_matches_value_path(
        chunk: &str,
        schema: SchemaRef,
        member_fields: Vec<MemberField>,
    ) -> Result<RecordBatch, CubeError> {
        let expected = serde_json::from_str::<JsonColumnarValueObject>(chunk)
            .map_err(CubeError::from)
            .and_then(|mut value_object| {
                transform_response(&mut value_object, schema.clone(), &member_fields)
            });
        let actual =
            deserialize_columnar_chunk(chunk.as_bytes(), schema.clone(), &member_fields, 0);
        // The hint only sizes builders, whether it is short, exact or past the row count
        for rows_hint in [1, 8, 1024] {
            let hinted = deserialize_columnar_chunk(
                chunk.as_bytes(),
                schema.clone(),
                &member_fields,
                rows_hint,
            );
            match (&actual, &hinted) {
                (Ok(actual), Ok(hinted)) => assert_eq!(actual, hinted, "chunk: {}", chunk),
                (Err(actual), Err(hinted)) => assert_eq!(actual.message, hinted.message),
                _ => panic!(
                    "chunk: {}\nno hint: {:?}\nhint {}: {:?}",
                    chunk, actual, rows_hint, hinted
                ),
            }
        }

        match (&expected, &actual) {
            (Ok(expected), Ok(actual)) => assert_eq!(expected, actual, "chunk: {}", chunk),
            (Err(_), Err(_)) => {}
            _ => panic!(
                "chunk: {}\nvalue path: {:?}\ndirect: {:?}",
                chunk, expected, actual
            ),
        }

        actual
    }

    fn all_types_schema() -> (SchemaRef, Vec<MemberField>) {
        let fields = vec![
            ("c.utf8", DataType::Utf8),
            ("c.i16", DataType::Int16),
            ("c.i32", DataType::Int32),
            ("c.i64", DataType::Int64),
            ("c.u64", DataType::UInt64),
            ("c.f32", DataType::Float32),
            ("c.f64", DataType::Float64),
            ("c.bool", DataType::Boolean),
            ("c.ts_ns", DataType::Timestamp(TimeUnit::Nanosecond, None)),
            ("c.ts_ms", DataType::Timestamp(TimeUnit::Millisecond, None)),
            ("c.date", DataType::Date32),
            ("c.decimal", DataType::Decimal128(10, 2)),
            ("c.null", DataType::Null),
            ("c.interval", DataType::Interval(IntervalUnit::YearMonth)),
        ];
        let member_fields = fields
            .iter()
            .map(|(name, _)| MemberField::regular(name.to_string()))
            .collect();
        let schema = Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, data_type)| Field::new(name, data_type, true))
                .collect::<Vec<_>>(),
        ));

        (schema, member_fields)
    }

    #[test]
    fn deserialize_columnar_chunk_matches_value_path_for_all_types() {
        let (schema, member_fields) = all_types_schema();
        let chunk = r#"{
            "members": ["c.utf8", "c.i16", "c.i32", "c.i64", "c.u64", "c.f32", "c.f64", "c.bool",
                        "c.ts_ns", "c.ts_ms", "c.date", "c.decimal", "c.null", "c.interval"],
            "columns": [
                ["plain", "esc\"aped\\n", "ünïcode ✓", 1.5, true, [1, "a"], {"k": [null]}, null],
                ["1", 2, "x", 3.6, -4, null, "32767", 0],
                ["1", 2, "x", 3.4, -4, null, "-7", 0],
                ["9007199254740993", 2, "x", -3.5, 18446744073709551615, null, "-7", 0],
                ["1", 2, "x", 3.5, 7, null, "-1", 0],
                ["1.25", 2, "x", 3.5, -4, null, "1e3", 0],
                ["1.25", 2, "x", 3.5, -4, null, "1e3", 0.1],
                [true, false, "true", "1", "false", "0", "yes", null],
                ["2024-01-02T03:04:05.678", "2024-01-02", "2024-01-02T03:04:05.123456", "2024-01-02T03:04:05Z", "2024-01-02 03:04:05", null, "2024-12-31T23:59:59.999", "1970-01-01T00:00:00.000"],
                ["2024-01-02T03:04:05.678", "2024-01-02", "2024-01-02T03:04:05.123456", "2024-01-02T03:04:05Z", "2024-01-02 03:04:05", null, "2024-12-31T23:59:59.999", "1970-01-01T00:00:00.000"],
                ["2024-01-02T03:04:05.678", "2024-01-02", "nope", "2024-01-02T03:04:05Z", "2024-01-02 03:04:05", null, "2024-12-31T23:59:59.999", "1970-01-01T00:00:00.000"],
                ["1", "1.5", "12345678.12", "-3.1", "0", null, "7.", ".5"],
                [1, "a", true, null, [1], {"k": 1}, 2.5, null],
                [null, null, null, null, null, null, null, null]
            ]
        }"#;

        let batch = assert_chunk_matches_value_path(chunk, schema, member_fields).unwrap();
        assert_eq!(batch.num_rows(), 8);
        let utf8 = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(utf8.value(1), "esc\"aped\\n");
        assert_eq!(utf8.value(2), "ünïcode ✓");
        assert_eq!(utf8.value(5), r#"[1,"a"]"#);
        assert_eq!(utf8.value(6), r#"{"k":[null]}"#);
    }

    #[test]
    fn deserialize_columnar_chunk_matches_value_path_for_member_mapping() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("a_again", DataType::Utf8, true),
            Field::new("missing", DataType::Float64, true),
            Field::new("literal", DataType::Utf8, true),
            Field::new("literal_null", DataType::Null, true),
            Field::new("dup", DataType::Utf8, true),
        ]));
        let member_fields = vec![
            MemberField::regular("c.a".to_string()),
            MemberField::regular("c.a".to_string()),
            MemberField::regular("c.missing".to_string()),
            MemberField::Literal(ScalarValue::Utf8(Some("lit".to_string()))),
            MemberField::Literal(ScalarValue::Utf8(None)),
            MemberField::regular("c.dup".to_string()),
        ];

        let ok_chunks = [
            // unrequested columns, a repeated member (first one wins), a member read twice
            r#"{"members": ["c.x", "c.a", "c.dup", "c.dup"], "columns": [[{"n": 1}, 2], [1, "2"], ["first", null], ["second", "x"]]}"#,
            // keys in a different order, unknown keys
            r#"{"extra": {"a": [1]}, "columns": [[1, 2], ["d", "e"]], "members": ["c.a", "c.dup"]}"#,
            // no rows
            r#"{"members": ["c.a"], "columns": [[]]}"#,
            r#"{"members": [], "columns": []}"#,
            // members without columns are NULL columns when not requested
            r#"{"members": ["c.x"], "columns": []}"#,
            // nested values read as JSON strings, which don't parse as numbers
            r#"{"members": ["c.a", "c.dup"], "columns": [[[1], {"a": 1}], [[1], {"a": 1}]]}"#,
        ];
        for chunk in ok_chunks {
            assert_chunk_matches_value_path(chunk, schema.clone(), member_fields.clone()).unwrap();
        }

        let err_chunks = [
            // ragged columns, including an unrequested one
            r#"{"members": ["c.a", "c.dup"], "columns": [[1, 2], ["x"]]}"#,
            r#"{"members": ["c.a", "c.x"], "columns": [[1, 2], [1, 2, 3]]}"#,
            r#"{"members": ["c.a"], "columns": [[1, 2], [1]]}"#,
            // a requested member without a column
            r#"{"members": ["c.x", "c.a"], "columns": [[1, 2]]}"#,
            // values the type can't take
            r#"{"members": ["c.a"], "columns": [[true]]}"#,
            // malformed chunks
            r#"{"members": ["c.a"]}"#,
            r#"{"columns": [[1]]}"#,
            r#"{"members": ["c.a"], "members": ["c.a"], "columns": [[1]]}"#,
            r#"{"members": ["c.a"], "columns": [1]}"#,
            r#"{"members": ["c.a"], "columns": [[1]]} trailing"#,
            r#"{"members": ["c.a"], "columns": [[1]"#,
            r#"[]"#,
        ];
        for chunk in err_chunks {
            assert!(
                assert_chunk_matches_value_path(chunk, schema.clone(), member_fields.clone())
                    .is_err(),
                "chunk: {}",
                chunk
            );
        }
    }

    #[test]
    fn member_fields_not_matching_schema_is_an_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let chunk = r#"{"members": ["c.a"], "columns": [[1]]}"#;

        for member_fields in [
            vec![],
            vec![
                MemberField::regular("c.a".to_string()),
                MemberField::regular("c.b".to_string()),
            ],
        ] {
            let expected = "Response schema has 1 fields, but";
            let err =
                deserialize_columnar_chunk(chunk.as_bytes(), schema.clone(), &member_fields, 0)
                    .unwrap_err();
            assert!(err.message.starts_with(expected), "{}", err.message);

            let mut value_object = serde_json::from_str::<JsonColumnarValueObject>(chunk).unwrap();
            let err =
                transform_response(&mut value_object, schema.clone(), &member_fields).unwrap_err();
            assert!(err.message.starts_with(expected), "{}", err.message);
        }
    }

    #[test]
    fn deserialize_columnar_chunk_keeps_conversion_error_messages() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Decimal128(5, 2),
            true,
        )]));
        let member_fields = vec![MemberField::regular("c.d".to_string())];

        let err = deserialize_columnar_chunk(
            br#"{"members": ["c.d"], "columns": [["1.2.3"]]}"#,
            schema.clone(),
            &member_fields,
            0,
        )
        .unwrap_err();
        assert!(
            err.message
                .contains("Unable to parse decimal, value contains two dots: 1.2.3"),
            "{}",
            err.message
        );

        let err = deserialize_columnar_chunk(
            br#"{"members": ["c.d"], "columns": [["1"], ["1", "2"]]}"#,
            schema.clone(),
            &member_fields,
            0,
        )
        .unwrap_err();
        assert_eq!(
            err.message,
            "columnar response has ragged columns: column 1 has 2 rows, expected 1"
        );

        let err = deserialize_columnar_chunk(
            br#"{"members": ["c.d"], "columns": [["1"]"#,
            schema,
            &member_fields,
            0,
        )
        .unwrap_err();
        assert!(
            err.message.starts_with("Can't parse columnar chunk JSON: "),
            "{}",
            err.message
        );
    }
}
