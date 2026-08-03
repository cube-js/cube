use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    datasource::{self, TableProvider},
    error::DataFusionError,
    execution::context::SessionState as DFSessionState,
    logical_plan::Expr,
    physical_plan::{udaf::AggregateUDF, udf::ScalarUDF, udtf::TableUDF, ExecutionPlan},
    sql::planner::ContextProvider,
};

use crate::{
    compile::DatabaseProtocolDetails,
    sql::{ColumnType, SessionManager, SessionState},
    transport::{CubeMeta, MetaContext, V1CubeMetaExt},
    CubeError,
};

#[derive(Clone)]
pub struct CubeContext {
    /// Internal state for the context (default)
    pub state: Arc<DFSessionState>,
    /// References
    pub meta: Arc<MetaContext>,
    pub sessions: Arc<SessionManager>,
    pub session_state: Arc<SessionState>,
}

impl CubeContext {
    pub fn new(
        state: Arc<DFSessionState>,
        meta: Arc<MetaContext>,
        sessions: Arc<SessionManager>,
        session_state: Arc<SessionState>,
    ) -> Self {
        Self {
            state,
            meta,
            sessions,
            session_state,
        }
    }

    pub fn table_name_by_table_provider(
        &self,
        table_provider: Arc<dyn datasource::TableProvider>,
    ) -> Result<String, CubeError> {
        self.session_state
            .protocol
            .table_name_by_table_provider(table_provider)
    }

    pub fn get_function<T>(&self, name: &str, udfs: &HashMap<String, Arc<T>>) -> Option<Arc<T>> {
        if name.starts_with("pg_catalog.") {
            return udfs.get(&format!("{}", &name[11..name.len()])).cloned();
        }

        udfs.get(name).cloned()
    }
}

impl ContextProvider for CubeContext {
    fn get_table_provider(
        &self,
        tr: datafusion::catalog::TableReference,
    ) -> Option<Arc<dyn TableProvider>> {
        return self.session_state.protocol.get_provider(&self.clone(), tr);
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.get_function(name, &self.state.scalar_functions)
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.get_function(name, &self.state.aggregate_functions)
    }

    fn get_table_function_meta(&self, name: &str) -> Option<Arc<TableUDF>> {
        self.get_function(name, &self.state.table_functions)
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        Some(DataType::Utf8)
    }
}

pub trait TableName {
    fn table_name(&self) -> &str;
}

pub struct CubeTableProvider {
    cube: CubeMeta,
}

impl CubeTableProvider {
    pub fn new(cube: CubeMeta) -> Self {
        Self { cube }
    }
}

impl TableName for CubeTableProvider {
    fn table_name(&self) -> &str {
        &self.cube.name
    }
}

#[async_trait]
impl TableProvider for CubeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(
            self.cube
                .get_columns()
                .into_iter()
                .map(|c| {
                    Field::new(
                        c.get_name(),
                        match c.get_column_type() {
                            ColumnType::Date(large) => {
                                if large {
                                    DataType::Date64
                                } else {
                                    DataType::Date32
                                }
                            }
                            ColumnType::Interval(unit) => DataType::Interval(unit),
                            ColumnType::String => DataType::Utf8,
                            ColumnType::VarStr => DataType::Utf8,
                            ColumnType::Boolean => DataType::Boolean,
                            ColumnType::Double => DataType::Float64,
                            ColumnType::Int8 => DataType::Int64,
                            ColumnType::Int32 => DataType::Int64,
                            ColumnType::Int64 => DataType::Int64,
                            ColumnType::Blob => DataType::Utf8,
                            ColumnType::Decimal(p, s) => DataType::Decimal(p, s),
                            ColumnType::List(field) => DataType::List(field.clone()),
                            ColumnType::Timestamp => {
                                DataType::Timestamp(TimeUnit::Nanosecond, None)
                            }
                        },
                        true,
                    )
                })
                .collect(),
        ))
    }

    async fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::Plan(format!(
            "Not rewritten table scan node for '{}' cube",
            self.cube.name
        )))
    }
}
