use crate::queryplanner::project_schema;
use crate::sql::cache::{sql_result_cache_sizeof, SqlResultCache};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int64Builder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, Partitioning, PlanProperties,
};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct InfoSchemaQueryCacheTableProvider {
    cache: Arc<SqlResultCache>,
}

impl InfoSchemaQueryCacheTableProvider {
    pub fn new(cache: Arc<SqlResultCache>) -> Self {
        Self { cache }
    }
}

fn get_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("sql", DataType::Utf8, false),
        Field::new("size", DataType::Int64, false),
    ]))
}

impl Debug for InfoSchemaQueryCacheTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "InfoSchemaQueryCacheTableProvider")
    }
}

#[async_trait]
impl TableProvider for InfoSchemaQueryCacheTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        get_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let schema = project_schema(&self.schema(), projection.cloned().as_deref());
        let exec = InfoSchemaQueryCacheTableExec {
            cache: self.cache.clone(),
            projection: projection.cloned(),
            projected_schema: schema.clone(),
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
        };

        Ok(Arc::new(exec))
    }
}

struct InfoSchemaQueryCacheBuilder {
    sql: StringBuilder,
    size: Int64Builder,
}

impl InfoSchemaQueryCacheBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            sql: StringBuilder::new(),
            size: Int64Builder::new(),
        }
    }

    fn add_row(&mut self, sql: impl AsRef<str> + Clone, size: i64) {
        self.sql.append_value(sql);
        self.size.append_value(size);
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.sql.finish()));
        columns.push(Arc::new(self.size.finish()));

        columns
    }
}

#[derive(Clone)]
pub struct InfoSchemaQueryCacheTableExec {
    cache: Arc<SqlResultCache>,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    properties: PlanProperties,
}

impl std::fmt::Debug for InfoSchemaQueryCacheTableExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str(&format!(
            "MetaTabular(cache: hidden, projected_schema: {:?})",
            self.projected_schema
        ))
    }
}

impl DisplayAs for InfoSchemaQueryCacheTableExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "InfoSchemaQueryCacheTableExec")
    }
}

#[async_trait]
impl ExecutionPlan for InfoSchemaQueryCacheTableExec {
    fn name(&self) -> &str {
        "InfoSchemaQueryCacheTableExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let mut builder = InfoSchemaQueryCacheBuilder::new(self.cache.entry_count() as usize);

        for (k, v) in self.cache.iter() {
            builder.add_row(
                k.get_query(),
                sql_result_cache_sizeof(&k, &v)
                    .try_into()
                    .unwrap_or(i64::MAX),
            );
        }

        let data = builder.finish();
        let batch = RecordBatch::try_new(get_schema(), data.to_vec())?;

        // TODO: Please migrate to real streaming, if we are going to expose query results
        let mem_exec =
            MemoryExec::try_new(&vec![vec![batch]], self.schema(), self.projection.clone())?;
        mem_exec.execute(partition, context)
    }
}
