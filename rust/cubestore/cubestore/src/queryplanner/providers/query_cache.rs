use crate::queryplanner::project_schema;
use crate::sql::cache::{sql_result_cache_sizeof, SqlResultCache};
use arrow::array::{Array, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::datasource::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use std::any::Any;
use std::fmt;
use std::fmt::Formatter;
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

impl TableProvider for InfoSchemaQueryCacheTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        get_schema()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let exec = InfoSchemaQueryCacheTableExec {
            cache: self.cache.clone(),
            projection: projection.clone(),
            projected_schema: project_schema(&self.schema(), projection.as_deref()),
        };

        Ok(Arc::new(exec))
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
        }
    }
}

struct InfoSchemaQueryCacheBuilder {
    sql: StringBuilder,
    size: Int64Builder,
}

impl InfoSchemaQueryCacheBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            sql: StringBuilder::new(capacity),
            size: Int64Builder::new(capacity),
        }
    }

    fn add_row(&mut self, sql: impl AsRef<str> + Clone, size: i64) {
        self.sql.append_value(sql).unwrap();
        self.size.append_value(size).unwrap();
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
}

impl std::fmt::Debug for InfoSchemaQueryCacheTableExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str(&format!(
            "MetaTabular(cache: hidden, projected_schema: {:?})",
            self.projected_schema
        ))
    }
}

#[async_trait]
impl ExecutionPlan for InfoSchemaQueryCacheTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(self.clone()))
    }

    async fn execute(
        &self,
        partition: usize,
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
        mem_exec.execute(partition).await
    }
}
