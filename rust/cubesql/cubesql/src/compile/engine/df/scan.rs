use std::{
    any::Any,
    fmt,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use cubeclient::models::{V1LoadRequestQuery, V1LoadResult, V1LoadResultAnnotation};
use datafusion::{
    arrow::{
        array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder},
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

use crate::{
    sql::AuthContextRef,
    transport::{CubeDummyStream, CubeReadStream, LoadRequestMeta, TransportService},
};
use chrono::{TimeZone, Utc};
use datafusion::{
    arrow::{array::TimestampNanosecondBuilder, datatypes::TimeUnit},
    execution::context::TaskContext,
    scalar::ScalarValue,
};
use serde_json::json;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MemberField {
    Member(String),
    Literal(ScalarValue),
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
}

impl CubeScanNode {
    pub fn new(
        schema: DFSchemaRef,
        member_fields: Vec<MemberField>,
        request: V1LoadRequestQuery,
        auth_context: AuthContextRef,
        options: CubeScanOptions,
    ) -> Self {
        Self {
            schema,
            member_fields,
            request,
            auth_context,
            options,
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
        })
    }
}

//  Produces an execution plan where the schema is mismatched from
//  the logical plan node.
pub struct CubeScanExtensionPlanner {
    pub transport: Arc<dyn TransportService>,
    pub meta: LoadRequestMeta,
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
                    auth_context: scan_node.auth_context.clone(),
                    options: scan_node.options.clone(),
                    meta: self.meta.clone(),
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
    auth_context: AuthContextRef,
    options: CubeScanOptions,
    // Shared references which will be injected by extension planner
    transport: Arc<dyn TransportService>,
    // injected by extension planner
    meta: LoadRequestMeta,
}

macro_rules! build_column {
    ($data_type:expr, $builder_ty:ty, $response:expr, $field_name:expr, { $($builder_block:tt)* }, { $($scalar_block:tt)* }) => {{
        let mut builder = <$builder_ty>::new($response.len());

        match $field_name {
            MemberField::Member(field_name) => {
                for row in $response.iter() {
                    let as_object = row.as_object().ok_or(
                        DataFusionError::Execution(
                            format!("Unexpected response from Cube.js, row is not an object, actual: {}", row)
                        ),
                    )?;
                    let value = as_object.get(field_name).ok_or(
                        DataFusionError::Execution(
                            format!(r#"Unexpected response from Cube.js, Field "{}" doesn't exist in row"#, field_name)
                        ),
                    )?;
                    match (&value, &mut builder) {
                        (serde_json::Value::Null, builder) => builder.append_null()?,
                        $($builder_block)*
                        (v, _) => {
                            return Err(DataFusionError::Execution(format!(
                                "Unable to map value {:?} to {:?}",
                                v,
                                $data_type
                            )));
                        }
                    };
                }
            }
            MemberField::Literal(value) => {
                for _ in 0..$response.len() {
                    match (value, &mut builder) {
                        $($scalar_block)*
                        (v, _) => {
                            return Err(DataFusionError::Execution(format!(
                                "Unable to map value {:?} to {:?}",
                                v,
                                $data_type
                            )));
                        }
                    }
                }
            }
        };

        Arc::new(builder.finish()) as ArrayRef
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
        let stream_mode = std::env::var("CUBESQL_STREAM_MODE")
            .ok()
            .map(|v| v.parse::<bool>().unwrap())
            .unwrap_or(false);
        let query_limit = std::env::var("CUBEJS_DB_QUERY_LIMIT")
            .ok()
            .map(|v| v.parse::<i32>().unwrap())
            .unwrap_or(50000);

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
        );

        if stream_mode {
            let result =
                self.transport
                    .load_stream(self.request.clone(), self.auth_context.clone(), meta);
            let stream = result.map_err(|err| DataFusionError::Execution(err.to_string()))?;
            let main_stream =
                CubeScanMemoryStream::new(stream, self.schema.clone(), self.member_fields.clone());

            return Ok(Box::pin(CubeScanStreamRouter::new(
                main_stream,
                one_shot_stream,
                self.schema.clone(),
                false,
            )));
        }

        one_shot_stream.data = Some(transform_response(
            load_data(
                request,
                self.auth_context.clone(),
                self.transport.clone(),
                meta.clone(),
                self.options.clone(),
            )
            .await?
            .data,
            one_shot_stream.schema.clone(),
            &one_shot_stream.member_fields,
        )?);

        Ok(Box::pin(CubeScanStreamRouter::new(
            CubeScanMemoryStream::new(
                Arc::new(CubeDummyStream {}),
                self.schema.clone(),
                self.member_fields.clone(),
            ),
            one_shot_stream,
            self.schema.clone(),
            true,
        )))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "CubeScanExecutionPlan")
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
    fired: bool,
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
            fired: false,
        }
    }

    fn poll_next(&mut self) -> Option<ArrowResult<RecordBatch>> {
        if !self.fired {
            self.fired = true;

            if let Some(batch) = &self.data {
                return Some(Ok(batch.clone()));
            }
        }

        None
    }
}

struct CubeScanMemoryStream {
    stream: Arc<dyn CubeReadStream>,
    /// Schema representing the data
    schema: SchemaRef,
    member_fields: Vec<MemberField>,
    is_completed: bool,
}

impl CubeScanMemoryStream {
    pub fn new(
        stream: Arc<dyn CubeReadStream>,
        schema: SchemaRef,
        member_fields: Vec<MemberField>,
    ) -> Self {
        Self {
            stream,
            schema,
            member_fields,
            is_completed: false,
        }
    }

    fn poll_next(&mut self) -> Option<ArrowResult<RecordBatch>> {
        match self.stream.poll_next() {
            Ok(chunk) => {
                let chunk = parse_chunk(chunk, self.schema.clone(), &self.member_fields);
                if chunk.is_none() {
                    self.is_completed = true;
                }

                chunk
            }
            Err(err) => Some(Err(ArrowError::ComputeError(err.to_string()))),
        }
    }
}

impl Drop for CubeScanMemoryStream {
    fn drop(&mut self) {
        if !self.is_completed {
            self.stream.reject();
        }
    }
}

struct CubeScanStreamRouter {
    main_stream: CubeScanMemoryStream,
    one_shot_stream: CubeScanOneShotStream,
    schema: SchemaRef,
    one_shot_mode: bool,
}

impl CubeScanStreamRouter {
    pub fn new(
        main_stream: CubeScanMemoryStream,
        one_shot_stream: CubeScanOneShotStream,
        schema: SchemaRef,
        one_shot_mode: bool,
    ) -> Self {
        Self {
            main_stream,
            one_shot_stream,
            schema,
            one_shot_mode,
        }
    }
}

impl Stream for CubeScanStreamRouter {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if !self.one_shot_mode {
            let mut chunk = self.main_stream.poll_next();
            match &chunk {
                Some(res) => match res {
                    Err(ArrowError::ComputeError(err))
                        if err
                            .as_str()
                            .contains("streamQuery() method is not implemented yet") =>
                    {
                        warn!("{}", err);

                        self.one_shot_mode = true;

                        match load_to_stream_sync(&mut self.one_shot_stream) {
                            Ok(_) => {
                                chunk = self.one_shot_stream.poll_next();
                            }
                            Err(e) => {
                                chunk = Some(Err(e.into()));
                            }
                        }
                    }
                    _ => (),
                },
                None => (),
            }

            return Poll::Ready(chunk);
        }

        Poll::Ready(self.one_shot_stream.poll_next())
    }
}

impl RecordBatchStream for CubeScanStreamRouter {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

async fn load_data(
    request: V1LoadRequestQuery,
    auth_context: AuthContextRef,
    transport: Arc<dyn TransportService>,
    meta: LoadRequestMeta,
    options: CubeScanOptions,
) -> ArrowResult<V1LoadResult> {
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
        V1LoadResult::new(
            V1LoadResultAnnotation {
                measures: json!(Vec::<serde_json::Value>::new()),
                dimensions: json!(Vec::<serde_json::Value>::new()),
                segments: json!(Vec::<serde_json::Value>::new()),
                time_dimensions: json!(Vec::<serde_json::Value>::new()),
            },
            data,
        )
    } else {
        let result = transport.load(request, auth_context, meta).await;
        let mut response = result.map_err(|err| ArrowError::ComputeError(err.to_string()))?;
        if let Some(data) = response.results.pop() {
            match (options.max_records, data.data.len()) {
                (Some(max_records), len) if len >= max_records => {
                    return Err(ArrowError::ComputeError(format!("One of the Cube queries exceeded the maximum row limit ({}). JOIN/UNION is not possible as it will produce incorrect results. Try filtering the results more precisely or moving post-processing functions to an outer query.", max_records)));
                }
                (_, _) => (),
            }

            data
        } else {
            return Err(ArrowError::ComputeError(format!(
                "Unable to extract result from Cube.js response",
            )));
        }
    };

    Ok(result)
}

fn load_to_stream_sync(one_shot_stream: &mut CubeScanOneShotStream) -> Result<()> {
    let req = one_shot_stream.request.clone();
    let auth = one_shot_stream.auth_context.clone();
    let transport = one_shot_stream.transport.clone();
    let meta = one_shot_stream.meta.clone();
    let options = one_shot_stream.options.clone();

    let handle = tokio::runtime::Handle::current();
    let res =
        std::thread::spawn(move || handle.block_on(load_data(req, auth, transport, meta, options)))
            .join()
            .map_err(|_| DataFusionError::Execution(format!("Can't load to steram")))?;

    one_shot_stream.data = Some(transform_response(
        res.unwrap().data,
        one_shot_stream.schema.clone(),
        &one_shot_stream.member_fields,
    )?);

    Ok(())
}

fn parse_chunk(
    chunk: Option<String>,
    schema: SchemaRef,
    member_fields: &Vec<MemberField>,
) -> Option<ArrowResult<RecordBatch>> {
    let res = match chunk {
        Some(chunk) => match serde_json::from_str::<Vec<serde_json::Value>>(&chunk) {
            Ok(data) => match transform_response(data, schema, member_fields) {
                Ok(batch) => Some(Ok(batch)),
                Err(err) => Some(Err(err.into())),
            },
            Err(e) => Some(Err(e.into())),
        },
        None => None,
    };

    res
}

fn transform_response(
    response: Vec<serde_json::Value>,
    schema: SchemaRef,
    member_fields: &Vec<MemberField>,
) -> Result<RecordBatch> {
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
                        (serde_json::Value::String(v), builder) => builder.append_value(v)?,
                        (serde_json::Value::Bool(v), builder) => builder.append_value(if *v { "true" } else { "false" })?,
                        (serde_json::Value::Number(v), builder) => builder.append_value(v.to_string())?,
                    },
                    {
                        (ScalarValue::Utf8(v), builder) => builder.append_option(v.as_ref())?,
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
                        (serde_json::Value::Number(number), builder) => match number.as_i64() {
                            Some(v) => builder.append_value(v)?,
                            None => builder.append_null()?,
                        },
                        (serde_json::Value::String(s), builder) => match s.parse::<i64>() {
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
                        (ScalarValue::Int64(v), builder) => builder.append_option(v.clone())?,
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
                        (serde_json::Value::Number(number), builder) => match number.as_f64() {
                            Some(v) => builder.append_value(v)?,
                            None => builder.append_null()?,
                        },
                        (serde_json::Value::String(s), builder) => match s.parse::<f64>() {
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
                        (ScalarValue::Float64(v), builder) => builder.append_option(v.clone())?,
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
                        (serde_json::Value::Bool(v), builder) => builder.append_value(*v)?,
                        (serde_json::Value::String(v), builder) => match v.as_str() {
                            "true" | "1" => builder.append_value(true)?,
                            "false" | "0" => builder.append_value(false)?,
                            _ => {
                                log::error!("Unable to map value {:?} to DataType::Boolean (returning null)", v);

                                builder.append_null()?
                            }
                        },
                    },
                    {
                        (ScalarValue::Boolean(v), builder) => builder.append_option(v.clone())?,
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
                        (serde_json::Value::String(s), builder) => {
                            let timestamp = Utc
                                .datetime_from_str(s.as_str(), "%Y-%m-%dT%H:%M:%S.%f")
                                .map_err(|e| {
                                    DataFusionError::Execution(format!(
                                        "Can't parse timestamp: '{}': {}",
                                        s, e
                                    ))
                                })?;
                            builder.append_value(timestamp.timestamp_nanos())?;
                        },
                    },
                    {
                        (ScalarValue::TimestampNanosecond(v, None), builder) => builder.append_option(v.clone())?,
                    }
                )
            }
            t => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Type {} is not supported in response transformation from Cube.js",
                    t,
                )))
            }
        };

        columns.push(column);
    }

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        compile::MetaContext,
        sql::{session::DatabaseProtocol, HttpAuthContext},
        transport::CubeReadStream,
        CubeError,
    };
    use cubeclient::models::V1LoadResponse;
    use datafusion::{
        arrow::{
            array::{BooleanArray, Float64Array, StringArray},
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
            protocol.to_string(),
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

            // Execute load query
            async fn load(
                &self,
                _query: V1LoadRequestQuery,
                _ctx: AuthContextRef,
                _meta_fields: LoadRequestMeta,
            ) -> Result<V1LoadResponse, CubeError> {
                let response = r#"
                    {
                        "annotation": {
                            "measures": [],
                            "dimensions": [],
                            "segments": [],
                            "timeDimensions": []
                        },
                        "data": [
                            {"KibanaSampleDataEcommerce.count": null, "KibanaSampleDataEcommerce.maxPrice": null, "KibanaSampleDataEcommerce.isBool": null},
                            {"KibanaSampleDataEcommerce.count": 5, "KibanaSampleDataEcommerce.maxPrice": 5.05, "KibanaSampleDataEcommerce.isBool": true},
                            {"KibanaSampleDataEcommerce.count": "5", "KibanaSampleDataEcommerce.maxPrice": "5.05", "KibanaSampleDataEcommerce.isBool": false},
                            {"KibanaSampleDataEcommerce.count": null, "KibanaSampleDataEcommerce.maxPrice": null, "KibanaSampleDataEcommerce.isBool": "true"},
                            {"KibanaSampleDataEcommerce.count": null, "KibanaSampleDataEcommerce.maxPrice": null, "KibanaSampleDataEcommerce.isBool": "false"}
                        ]
                    }
                "#;

                let result: V1LoadResult = serde_json::from_str(response).unwrap();

                Ok(V1LoadResponse {
                    pivot_query: None,
                    slow_query: None,
                    query_type: None,
                    results: vec![result],
                })
            }

            fn load_stream(
                &self,
                _query: V1LoadRequestQuery,
                _ctx: AuthContextRef,
                _meta_fields: LoadRequestMeta,
            ) -> Result<Arc<dyn CubeReadStream>, CubeError> {
                panic!("It's a fake transport");
            }
        }

        Arc::new(TestConnectionTransport {})
    }

    #[tokio::test]
    async fn test_df_cube_scan_execute() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("KibanaSampleDataEcommerce.count", DataType::Utf8, false),
            Field::new(
                "KibanaSampleDataEcommerce.maxPrice",
                DataType::Float64,
                false,
            ),
            Field::new("KibanaSampleDataEcommerce.isBool", DataType::Boolean, false),
            Field::new(
                "KibanaSampleDataEcommerce.is_female",
                DataType::Boolean,
                false,
            ),
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
                        MemberField::Member(f.name().to_string())
                    }
                })
                .collect(),
            request: V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                ]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.isBool".to_string()]),
                segments: None,
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            },
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
                    Arc::new(Float64Array::from(vec![
                        None,
                        Some(5.05),
                        Some(5.05),
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
                ],
            )
            .unwrap()
        )
    }
}
