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
            name: StringBuilder::new(capacity),
            statement: StringBuilder::new(capacity),
            prepare_time: TimestampNanosecondBuilder::new(capacity),
            parameter_types: ListBuilder::new(Int32Builder::new(capacity)),
            from_sql: BooleanBuilder::new(capacity),
            generic_plans: Int64Builder::new(capacity),
            custom_plans: Int64Builder::new(capacity),
        }
    }

    fn add_prepared_statement(&mut self, name: &str, statement: &Option<PreparedStatement>) {
        self.name.append_value(name).unwrap();

        let now = chrono::offset::Utc::now();

        // TODO: We need to drop this option for PreparedStatement (we use it for empty query) and store all information
        if let Some(st) = statement {
            self.statement.append_value(st.query.to_string()).unwrap();
            self.prepare_time
                .append_value(now.timestamp_nanos())
                .unwrap();

            for param in &st.parameters.parameters {
                self.parameter_types
                    .values()
                    .append_value(param.clone() as i32)
                    .unwrap();
            }

            self.parameter_types.append(true).unwrap();
        } else {
            self.statement.append_value("").unwrap();
            self.prepare_time
                .append_value(now.timestamp_nanos())
                .unwrap();
            self.parameter_types.append(true).unwrap();
        }

        // TODO: from_sql
        self.from_sql.append_value(false).unwrap();
        // TODO: Stats
        self.generic_plans.append_value(0).unwrap();
        self.custom_plans.append_value(0).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        columns.push(Arc::new(self.name.finish()));
        columns.push(Arc::new(self.statement.finish()));
        columns.push(Arc::new(self.prepare_time.finish()));
        columns.push(Arc::new(self.parameter_types.finish()));
        columns.push(Arc::new(self.from_sql.finish()));
        columns.push(Arc::new(self.generic_plans.finish()));
        columns.push(Arc::new(self.custom_plans.finish()));

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
                DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
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
            builder.add_prepared_statement(&name, statement);
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
