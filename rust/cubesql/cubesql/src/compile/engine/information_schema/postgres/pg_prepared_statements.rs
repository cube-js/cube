use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use crate::sql::{extended::PreparedStatement, SessionState};
use datafusion::{
    arrow::{
        array::{
            Array, BooleanBuilder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
            TimestampNanosecondBuilder,
        },
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgPreparedStatementsBuilder {
    name: StringBuilder,
    statement: StringBuilder,
    prepare_time: TimestampNanosecondBuilder,
    parameter_types: ListBuilder<Int32Builder>,
    from_sql: BooleanBuilder,
    generic_plans: Int64Builder,
    custom_plans: Int64Builder,
}

impl PgPreparedStatementsBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            name: StringBuilder::with_capacity(capacity, capacity),
            statement: StringBuilder::with_capacity(capacity, capacity),
            prepare_time: TimestampNanosecondBuilder::with_capacity(capacity),
            parameter_types: ListBuilder::new(Int32Builder::with_capacity(capacity)),
            from_sql: BooleanBuilder::with_capacity(capacity),
            generic_plans: Int64Builder::with_capacity(capacity),
            custom_plans: Int64Builder::with_capacity(capacity),
        }
    }

    fn add_prepared_statement(&mut self, name: &str, statement: &PreparedStatement) {
        self.name.append_value(name);
        self.prepare_time
            .append_value(statement.get_created().timestamp_nanos_opt().unwrap());

        if let Some(parameters) = statement.get_parameters() {
            for param in parameters {
                self.parameter_types.values().append_value(*param as i32);
            }
        }

        self.parameter_types.append(true);
        self.statement.append_value(statement.get_query_as_string());
        self.from_sql.append_value(statement.get_from_sql());
        // TODO: Stats
        self.generic_plans.append_value(0);
        self.custom_plans.append_value(0);
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.name.finish()),
            Arc::new(self.statement.finish()),
            Arc::new(self.prepare_time.finish()),
            Arc::new(self.parameter_types.finish()),
            Arc::new(self.from_sql.finish()),
            Arc::new(self.generic_plans.finish()),
            Arc::new(self.custom_plans.finish()),
        ];

        columns
    }
}

pub struct PgPreparedStatementsProvider {
    session: Arc<SessionState>,
}

impl PgPreparedStatementsProvider {
    pub fn new(session: Arc<SessionState>) -> Self {
        Self { session }
    }
}

#[async_trait]
impl TableProvider for PgPreparedStatementsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("statement", DataType::Utf8, false),
            Field::new(
                "prepare_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "parameter_types",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                false,
            ),
            Field::new("from_sql", DataType::Boolean, false),
            Field::new("generic_plans", DataType::Int64, false),
            Field::new("custom_plans", DataType::Int64, false),
        ]))
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let statements = self.session.statements.read().await;
        let mut builder = PgPreparedStatementsBuilder::new(statements.len());

        for (name, statement) in statements.iter() {
            builder.add_prepared_statement(name, statement);
        }

        let batch = RecordBatch::try_new(self.schema(), builder.finish())?;

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            self.schema(),
            projection.clone(),
        )?))
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}
