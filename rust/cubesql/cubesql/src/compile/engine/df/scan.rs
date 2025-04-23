use async_trait::async_trait;
use cubeclient::models::{V1LoadRequestQuery, V1LoadResponse};
pub use datafusion::{
    arrow::{
        array::{
            ArrayRef, BooleanBuilder, Date32Builder, DecimalBuilder, Float32Builder,
            Float64Builder, Int16Builder, Int32Builder, Int64Builder, NullArray, StringBuilder,
        },
        datatypes::{DataType, SchemaRef},
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
use futures::Stream;
use log::warn;
use std::{
    any::Any,
    borrow::Cow,
    fmt,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    compile::{
        engine::df::wrapper::{CubeScanWrappedSqlNode, CubeScanWrapperNode, SqlQuery},
        test::find_cube_scans_deep_search,
    },
    config::ConfigObj,
    sql::AuthContextRef,
    transport::{CubeStreamReceiver, LoadRequestMeta, SpanId, TransportService},
    CubeError,
};
use chrono::{Datelike, NaiveDate, NaiveDateTime};
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
use serde_json::Value;

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

#[derive(Debug, Clone)]
pub struct CubeScanOptions {
    pub change_user: Option<String>,
    pub max_records: Option<usize>,
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
                        .ok_or(DataFusionError::Internal(format!(
                            "No cube scans found in wrapper node: {:?}",
                            wrapped_sql_node
                        )))?;

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

#[derive(Debug)]
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

pub trait ValueObject {
    fn len(&mut self) -> std::result::Result<usize, CubeError>;

    fn get(&mut self, index: usize, field_name: &str)
        -> std::result::Result<FieldValue, CubeError>;
}

pub struct JsonValueObject {
    rows: Vec<Value>,
}

impl JsonValueObject {
    pub fn new(rows: Vec<Value>) -> Self {
        JsonValueObject { rows }
    }
}

impl ValueObject for JsonValueObject {
    fn len(&mut self) -> std::result::Result<usize, CubeError> {
        Ok(self.rows.len())
    }

    fn get(
        &mut self,
        index: usize,
        field_name: &str,
    ) -> std::result::Result<FieldValue, CubeError> {
        let Some(as_object) = self.rows[index].as_object() else {
            return Err(CubeError::user(format!(
                "Unexpected response from Cube, row is not an object: {:?}",
                self.rows[index]
            )));
        };

        let value = as_object.get(field_name).unwrap_or(&Value::Null);

        Ok(match value {
            Value::String(s) => FieldValue::String(Cow::Borrowed(s)),
            Value::Number(n) => FieldValue::Number(n.as_f64().ok_or(
                DataFusionError::Execution(format!("Can't convert {:?} to float", n)),
            )?),
            Value::Bool(b) => FieldValue::Bool(*b),
            Value::Null => FieldValue::Null,
            x => {
                return Err(CubeError::user(format!(
                    "Expected primitive value but found: {:?}",
                    x
                )));
            }
        })
    }
}

macro_rules! build_column {
    ($data_type:expr, $builder_ty:ty, $response:expr, $field_name:expr, { $($builder_block:tt)* }, { $($scalar_block:tt)* }) => {{
        let len = $response.len()?;
        let mut builder = <$builder_ty>::new(len);

        build_column_custom_builder!($data_type, len, builder, $response, $field_name, { $($builder_block)* }, { $($scalar_block)* })
    }}
}

macro_rules! build_column_custom_builder {
    ($data_type:expr, $len:expr, $builder:expr, $response:expr, $field_name: expr, { $($builder_block:tt)* }, { $($scalar_block:tt)* }) => {{
        match $field_name {
            MemberField::Member(member) => {
                let field_name = &member.field_name;
                for i in 0..$len {
                    let value = $response.get(i, &field_name)?;
                    match (value, &mut $builder) {
                        (FieldValue::Null, builder) => builder.append_null()?,
                        $($builder_block)*
                        #[allow(unreachable_patterns)]
                        (v, _) => {
                            return Err(CubeError::user(format!(
                                "Unable to map value {:?} to {:?}",
                                v,
                                $data_type
                            )));
                        }
                    };
                }
            }
            MemberField::Literal(value) => {
                for _ in 0..$len {
                    match (value, &mut $builder) {
                        $($scalar_block)*
                        (v, _) => {
                            return Err(CubeError::user(format!(
                                "Unable to map value {:?} to {:?}",
                                v,
                                $data_type
                            )));
                        }
                    }
                }
            }
        };

        Arc::new($builder.finish()) as ArrayRef
    }}
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

        let stream_mode = match (stream_mode, self.request.limit) {
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
                )
                .await;
            let stream = result.map_err(|err| DataFusionError::Execution(err.to_string()))?;
            let main_stream = CubeScanMemoryStream::new(stream);

            return Ok(Box::pin(CubeScanStreamRouter::new(
                Some(main_stream),
                one_shot_stream,
                self.schema.clone(),
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
        one_shot_stream.data = Some(response.first().unwrap().clone());

        Ok(Box::pin(CubeScanStreamRouter::new(
            None,
            one_shot_stream,
            self.schema.clone(),
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
            Some(Some(Err(err))) => Some(Err(ArrowError::ComputeError(err.to_string()))),
            Some(None) => None,
            None => None,
        })
    }
}

struct CubeScanStreamRouter {
    main_stream: Option<CubeScanMemoryStream>,
    one_shot_stream: CubeScanOneShotStream,
    schema: SchemaRef,
}

impl CubeScanStreamRouter {
    pub fn new(
        main_stream: Option<CubeScanMemoryStream>,
        one_shot_stream: CubeScanOneShotStream,
        schema: SchemaRef,
    ) -> Self {
        Self {
            main_stream,
            one_shot_stream,
            schema,
        }
    }
}

impl Stream for CubeScanStreamRouter {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match &mut self.main_stream {
            Some(main_stream) => {
                let next = main_stream.poll_next(cx);
                if let Poll::Ready(Some(Err(ArrowError::ComputeError(err)))) = &next {
                    if err
                        .as_str()
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

                return next;
            }
            None => Poll::Ready(self.one_shot_stream.poll_next()),
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
        let mut data = Vec::new();

        for _ in 0..limit {
            data.push(serde_json::Value::Null)
        }

        let mut response = JsonValueObject::new(data);
        let rec = transform_response(&mut response, schema.clone(), &member_fields)
            .map_err(|e| DataFusionError::Execution(e.message.to_string()))?;

        rec
    } else {
        let result = transport
            .load(
                span_id,
                request,
                sql_query,
                auth_context,
                meta,
                schema,
                member_fields,
            )
            .await
            .map_err(|err| ArrowError::ComputeError(err.to_string()))?;
        let response = result.first();
        if let Some(data) = response.cloned() {
            match (options.max_records, data.num_rows()) {
                (Some(max_records), len) if len >= max_records => {
                    return Err(ArrowError::ComputeError(format!("One of the Cube queries exceeded the maximum row limit ({}). JOIN/UNION is not possible as it will produce incorrect results. Try filtering the results more precisely or moving post-processing functions to an outer query.", max_records)));
                }
                (_, _) => (),
            }

            data
        } else {
            return Err(ArrowError::ComputeError(
                "Unable to extract results from response: results is empty".to_string(),
            ));
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

pub fn transform_response<V: ValueObject>(
    response: &mut V,
    schema: SchemaRef,
    member_fields: &Vec<MemberField>,
) -> std::result::Result<RecordBatch, CubeError> {
    let mut columns = vec![];

    for (i, schema_field) in schema.fields().iter().enumerate() {
        let field_name = &member_fields[i];
        let column = match schema_field.data_type() {
            DataType::Utf8 => {
                build_column!(
                    DataType::Utf8,
                    StringBuilder,
                    response,
                    field_name,
                    {
                        (FieldValue::String(v), builder) => builder.append_value(v)?,
                        (FieldValue::Bool(v), builder) => builder.append_value(if v { "true" } else { "false" })?,
                        (FieldValue::Number(v), builder) => builder.append_value(v.to_string())?,
                    },
                    {
                        (ScalarValue::Utf8(v), builder) => builder.append_option(v.as_ref())?,
                    }
                )
            }
            DataType::Int16 => {
                build_column!(
                    DataType::Int16,
                    Int16Builder,
                    response,
                    field_name,
                    {
                        (FieldValue::Number(number), builder) => builder.append_value(number.round() as i16)?,
                        (FieldValue::String(s), builder) => match s.parse::<i16>() {
                            Ok(v) => builder.append_value(v)?,
                            Err(error) => {
                                warn!(
                                    "Unable to parse value as i16: {}",
                                    error.to_string()
                                );

                                builder.append_null()?
                            }
                        },
                    },
                    {
                        (ScalarValue::Int16(v), builder) => builder.append_option(*v)?,
                    }
                )
            }
            DataType::Int32 => {
                build_column!(
                    DataType::Int32,
                    Int32Builder,
                    response,
                    field_name,
                    {
                        (FieldValue::Number(number), builder) => builder.append_value(number.round() as i32)?,
                        (FieldValue::String(s), builder) => match s.parse::<i32>() {
                            Ok(v) => builder.append_value(v)?,
                            Err(error) => {
                                warn!(
                                    "Unable to parse value as i32: {}",
                                    error.to_string()
                                );

                                builder.append_null()?
                            }
                        },
                    },
                    {
                        (ScalarValue::Int32(v), builder) => builder.append_option(*v)?,
                    }
                )
            }
            DataType::Int64 => {
                build_column!(
                    DataType::Int64,
                    Int64Builder,
                    response,
                    field_name,
                    {
                        (FieldValue::Number(number), builder) => builder.append_value(number.round() as i64)?,
                        (FieldValue::String(s), builder)  => match s.parse::<i64>() {
                            Ok(v) => builder.append_value(v)?,
                            Err(error) => {
                                warn!(
                                    "Unable to parse value as i64: {}",
                                    error.to_string()
                                );

                                builder.append_null()?
                            }
                        },
                    },
                    {
                        (ScalarValue::Int64(v), builder) => builder.append_option(*v)?,
                    }
                )
            }
            DataType::Float32 => {
                build_column!(
                    DataType::Float32,
                    Float32Builder,
                    response,
                    field_name,
                    {
                        (FieldValue::Number(number), builder) => builder.append_value(number as f32)?,
                        (FieldValue::String(s), builder) => match s.parse::<f32>() {
                            Ok(v) => builder.append_value(v)?,
                            Err(error) => {
                                warn!(
                                    "Unable to parse value as f32: {}",
                                    error.to_string()
                                );

                                builder.append_null()?
                            }
                        },
                    },
                    {
                        (ScalarValue::Float32(v), builder) => builder.append_option(*v)?,
                    }
                )
            }
            DataType::Float64 => {
                build_column!(
                    DataType::Float64,
                    Float64Builder,
                    response,
                    field_name,
                    {
                        (FieldValue::Number(number), builder) => builder.append_value(number)?,
                        (FieldValue::String(s), builder) => match s.parse::<f64>() {
                            Ok(v) => builder.append_value(v)?,
                            Err(error) => {
                                warn!(
                                    "Unable to parse value as f64: {}",
                                    error.to_string()
                                );

                                builder.append_null()?
                            }
                        },
                    },
                    {
                        (ScalarValue::Float64(v), builder) => builder.append_option(*v)?,
                    }
                )
            }
            DataType::Boolean => {
                build_column!(
                    DataType::Boolean,
                    BooleanBuilder,
                    response,
                    field_name,
                    {
                        (FieldValue::Bool(v), builder) => builder.append_value(v)?,
                        (FieldValue::String(v), builder)  => match v.as_ref() {
                            "true" | "1" => builder.append_value(true)?,
                            "false" | "0" => builder.append_value(false)?,
                            _ => {
                                log::error!("Unable to map value {:?} to DataType::Boolean (returning null)", v);

                                builder.append_null()?
                            }
                        },
                    },
                    {
                        (ScalarValue::Boolean(v), builder) => builder.append_option(*v)?,
                    }
                )
            }
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                build_column!(
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    TimestampNanosecondBuilder,
                    response,
                    field_name,
                    {
                        (FieldValue::String(s), builder) => {
                            let timestamp = NaiveDateTime::parse_from_str(s.as_ref(), "%Y-%m-%dT%H:%M:%S%.f")
                                .or_else(|_| NaiveDateTime::parse_from_str(s.as_ref(), "%Y-%m-%d %H:%M:%S%.f"))
                                .or_else(|_| NaiveDateTime::parse_from_str(s.as_ref(), "%Y-%m-%dT%H:%M:%S"))
                                .or_else(|_| NaiveDateTime::parse_from_str(s.as_ref(), "%Y-%m-%dT%H:%M:%S%.fZ"))
                                .or_else(|_| {
                                    NaiveDate::parse_from_str(s.as_ref(), "%Y-%m-%d").map(|date| {
                                        date.and_hms_opt(0, 0, 0).unwrap()
                                    })
                                })
                                .map_err(|e| {
                                    DataFusionError::Execution(format!(
                                        "Can't parse timestamp: '{}': {}",
                                        s, e
                                    ))
                                })?;
                            // TODO switch parsing to microseconds
                            if timestamp.and_utc().timestamp_millis() > (((1i64) << 62) / 1_000_000) {
                                builder.append_null()?;
                            } else if let Some(nanos) = timestamp.and_utc().timestamp_nanos_opt() {
                                builder.append_value(nanos)?;
                            } else {
                                log::error!(
                                    "Unable to cast timestamp value to nanoseconds: {}",
                                    timestamp.to_string()
                                );
                                builder.append_null()?;
                            }
                        },
                    },
                    {
                        (ScalarValue::TimestampNanosecond(v, None), builder) => builder.append_option(*v)?,
                    }
                )
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                build_column!(
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    TimestampMillisecondBuilder,
                    response,
                    field_name,
                    {
                        (FieldValue::String(s), builder) => {
                            let timestamp = NaiveDateTime::parse_from_str(s.as_ref(), "%Y-%m-%dT%H:%M:%S%.f")
                                .or_else(|_| NaiveDateTime::parse_from_str(s.as_ref(), "%Y-%m-%d %H:%M:%S%.f"))
                                .or_else(|_| NaiveDateTime::parse_from_str(s.as_ref(), "%Y-%m-%dT%H:%M:%S"))
                                .or_else(|_| NaiveDateTime::parse_from_str(s.as_ref(), "%Y-%m-%dT%H:%M:%S%.fZ"))
                                .or_else(|_| {
                                    NaiveDate::parse_from_str(s.as_ref(), "%Y-%m-%d").map(|date| {
                                        date.and_hms_opt(0, 0, 0).unwrap()
                                    })
                                })
                                .map_err(|e| {
                                    DataFusionError::Execution(format!(
                                        "Can't parse timestamp: '{}': {}",
                                        s, e
                                    ))
                                })?;
                            // TODO switch parsing to microseconds
                            if timestamp.and_utc().timestamp_millis() > (((1 as i64) << 62) / 1_000_000) {
                                builder.append_null()?;
                            } else {
                                builder.append_value(timestamp.and_utc().timestamp_millis())?;
                            }
                        },
                    },
                    {
                        (ScalarValue::TimestampMillisecond(v, None), builder) => builder.append_option(*v)?,
                    }
                )
            }
            DataType::Date32 => {
                build_column!(
                    DataType::Date32,
                    Date32Builder,
                    response,
                    field_name,
                    {
                        (FieldValue::String(s), builder) => {
                            let date = NaiveDate::parse_from_str(s.as_ref(), "%Y-%m-%d")
                                // FIXME: temporary solution for cases when expected type is Date32
                                // but underlying data is a Timestamp
                                .or_else(|_| NaiveDate::parse_from_str(s.as_ref(), "%Y-%m-%dT00:00:00.000"))
                                .map_err(|e| {
                                    DataFusionError::Execution(format!(
                                        "Can't parse date: '{}': {}",
                                        s, e
                                    ))
                                });
                            match date {
                                Ok(date) => {
                                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                                    let days_since_epoch = date.num_days_from_ce()  - epoch.num_days_from_ce();
                                    builder.append_value(days_since_epoch)?;
                                }
                                Err(error) => {
                                    log::error!(
                                        "Unable to parse value as Date32: {}",
                                        error.to_string()
                                    );

                                    builder.append_null()?
                                }
                            }
                        }
                    },
                    {
                        (ScalarValue::Date32(v), builder) => builder.append_option(*v)?,
                    }
                )
            }
            DataType::Decimal(precision, scale) => {
                let len = response.len()?;
                let mut builder = DecimalBuilder::new(len, *precision, *scale);

                build_column_custom_builder!(
                    DataType::Decimal(*precision, *scale),
                    len,
                    builder,
                    response,
                    field_name,
                    {
                        (FieldValue::String(s), builder) => {
                            let mut parts = s.split(".");
                            match parts.next() {
                                None => builder.append_null()?,
                                Some(int_part) => {
                                    let frac_part = format!("{:0<width$}", parts.next().unwrap_or(""), width=scale);
                                    if frac_part.len() > *scale {
                                        Err(DataFusionError::Execution(format!("Decimal scale is higher than requested: expected {}, got {}", scale, frac_part.len())))?;
                                    }
                                    if let Some(_) = parts.next() {
                                        Err(DataFusionError::Execution(format!("Unable to parse decimal, value contains two dots: {}", s)))?;
                                    }
                                    let decimal_str = format!("{}{}", int_part, frac_part);
                                    if decimal_str.len() > *precision {
                                        Err(DataFusionError::Execution(format!("Decimal precision is higher than requested: expected {}, got {}", precision, decimal_str.len())))?;
                                    }
                                    if let Ok(value) = decimal_str.parse::<i128>() {
                                        builder.append_value(value)?;
                                    } else {
                                        Err(DataFusionError::Execution(format!("Unable to parse decimal as an i128: {}", decimal_str)))?;
                                    }
                                }
                            };
                        },
                    },
                    {
                        (ScalarValue::Decimal128(v, _, _), builder) => {
                            // TODO: check precision and scale, adjust accordingly
                            if let Some(v) = v {
                                builder.append_value(*v)?;
                            } else {
                                builder.append_null()?;
                            }
                        },
                    }
                )
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                build_column!(
                    DataType::Interval(IntervalUnit::YearMonth),
                    IntervalYearMonthBuilder,
                    response,
                    field_name,
                    {
                        // TODO
                    },
                    {
                        (ScalarValue::IntervalYearMonth(v), builder) => builder.append_option(*v)?,
                    }
                )
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                build_column!(
                    DataType::Interval(IntervalUnit::DayTime),
                    IntervalDayTimeBuilder,
                    response,
                    field_name,
                    {
                        // TODO
                    },
                    {
                        (ScalarValue::IntervalDayTime(v), builder) => builder.append_option(*v)?,
                    }
                )
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                build_column!(
                    DataType::Interval(IntervalUnit::MonthDayNano),
                    IntervalMonthDayNanoBuilder,
                    response,
                    field_name,
                    {
                        // TODO
                    },
                    {
                        (ScalarValue::IntervalMonthDayNano(v), builder) => builder.append_option(*v)?,
                    }
                )
            }
            DataType::Null => {
                let len = response.len()?;
                let array = NullArray::new(len);
                Arc::new(array)
            }
            t => {
                return Err(CubeError::user(format!(
                    "Type {} is not supported in response transformation from Cube",
                    t,
                )))
            }
        };

        columns.push(column);
    }

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

pub fn convert_transport_response(
    response: V1LoadResponse,
    schema: SchemaRef,
    member_fields: Vec<MemberField>,
) -> std::result::Result<Vec<RecordBatch>, CubeError> {
    response
        .results
        .into_iter()
        .map(|r| {
            let mut response = JsonValueObject::new(r.data.clone());
            transform_response(&mut response, schema.clone(), &member_fields)
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
    use cubeclient::models::V1LoadResponse;
    use datafusion::{
        arrow::{
            array::{BooleanArray, Float64Array, StringArray, TimestampNanosecondArray},
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
                        "data": [
                            {"KibanaSampleDataEcommerce.count": null, "KibanaSampleDataEcommerce.maxPrice": null, "KibanaSampleDataEcommerce.isBool": null, "KibanaSampleDataEcommerce.orderDate": null, "KibanaSampleDataEcommerce.city": "City 1"},
                            {"KibanaSampleDataEcommerce.count": 5, "KibanaSampleDataEcommerce.maxPrice": 5.05, "KibanaSampleDataEcommerce.isBool": true, "KibanaSampleDataEcommerce.orderDate": "2022-01-01 00:00:00.000", "KibanaSampleDataEcommerce.city": "City 2"},
                            {"KibanaSampleDataEcommerce.count": "5", "KibanaSampleDataEcommerce.maxPrice": "5.05", "KibanaSampleDataEcommerce.isBool": false, "KibanaSampleDataEcommerce.orderDate": "2023-01-01 00:00:00.000", "KibanaSampleDataEcommerce.city": "City 3"},
                            {"KibanaSampleDataEcommerce.count": null, "KibanaSampleDataEcommerce.maxPrice": null, "KibanaSampleDataEcommerce.isBool": "true", "KibanaSampleDataEcommerce.orderDate": "9999-12-31 00:00:00.000", "KibanaSampleDataEcommerce.city": "City 4"},
                            {"KibanaSampleDataEcommerce.count": null, "KibanaSampleDataEcommerce.maxPrice": null, "KibanaSampleDataEcommerce.isBool": "false", "KibanaSampleDataEcommerce.orderDate": null, "KibanaSampleDataEcommerce.city": null}
                        ]
                    }]
                }
                "#;

                let result: V1LoadResponse = serde_json::from_str(response).unwrap();
                convert_transport_response(result, schema.clone(), member_fields)
                    .map_err(|err| CubeError::user(err.to_string()))
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

    #[tokio::test]
    async fn test_df_cube_scan_execute() {
        assert_eq!(std::mem::size_of::<FieldValue>(), 24);

        let schema = Arc::new(Schema::new(vec![
            Field::new("KibanaSampleDataEcommerce.count", DataType::Utf8, false),
            Field::new("KibanaSampleDataEcommerce.count", DataType::Utf8, false),
            Field::new(
                "KibanaSampleDataEcommerce.maxPrice",
                DataType::Float64,
                false,
            ),
            Field::new(
                "KibanaSampleDataEcommerce.orderDate",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("KibanaSampleDataEcommerce.isBool", DataType::Boolean, false),
            Field::new(
                "KibanaSampleDataEcommerce.is_female",
                DataType::Boolean,
                false,
            ),
            Field::new("KibanaSampleDataEcommerce.city", DataType::Utf8, false),
        ]));

        let scan_node = CubeScanExecutionPlan {
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
            },
            transport: get_test_transport(),
            meta: get_test_load_meta(DatabaseProtocol::PostgreSQL),
            span_id: None,
            config_obj: crate::config::Config::test().config_obj(),
        };

        let runtime = Arc::new(
            RuntimeEnv::new(RuntimeConfig::new()).expect("Unable to create RuntimeEnv for testing"),
        );
        let task = Arc::new(TaskContext::new(
            "test".to_string(),
            "session".to_string(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            runtime,
        ));
        let stream = scan_node.execute(0, task).await.unwrap();
        let batches = common::collect(stream).await.unwrap();

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
                ],
            )
            .unwrap()
        )
    }
}
