use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use crate::sql::{session::SessionStatActivity, SessionManager};
use datafusion::{
    arrow::{
        array::{Array, Int64Builder, StringBuilder, TimestampNanosecondBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgStatActivityBuilder {
    oid: UInt32Builder,
    datname: StringBuilder,
    pid: UInt32Builder,
    leader_pid: UInt32Builder,
    usesysid: UInt32Builder,
    usename: StringBuilder,
    application_name: StringBuilder,
    client_addr: StringBuilder,
    client_hostname: StringBuilder,
    client_port: StringBuilder,
    backend_start: TimestampNanosecondBuilder,
    xact_start: TimestampNanosecondBuilder,
    query_start: TimestampNanosecondBuilder,
    state_change: TimestampNanosecondBuilder,
    wait_event_type: StringBuilder,
    wait_event: StringBuilder,
    state: StringBuilder,
    backend_xid: UInt32Builder,
    backend_xmin: UInt32Builder,
    query_id: Int64Builder,
    query: StringBuilder,
    backend_type: StringBuilder,
}

impl PgStatActivityBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            oid: UInt32Builder::with_capacity(capacity),
            datname: StringBuilder::with_capacity(capacity, capacity),
            pid: UInt32Builder::with_capacity(capacity),
            leader_pid: UInt32Builder::with_capacity(capacity),
            usesysid: UInt32Builder::with_capacity(capacity),
            usename: StringBuilder::with_capacity(capacity, capacity),
            application_name: StringBuilder::with_capacity(capacity, capacity),
            client_addr: StringBuilder::with_capacity(capacity, capacity),
            client_hostname: StringBuilder::with_capacity(capacity, capacity),
            client_port: StringBuilder::with_capacity(capacity, capacity),
            backend_start: TimestampNanosecondBuilder::with_capacity(capacity),
            xact_start: TimestampNanosecondBuilder::with_capacity(capacity),
            query_start: TimestampNanosecondBuilder::with_capacity(capacity),
            state_change: TimestampNanosecondBuilder::with_capacity(capacity),
            wait_event_type: StringBuilder::with_capacity(capacity, capacity),
            wait_event: StringBuilder::with_capacity(capacity, capacity),
            state: StringBuilder::with_capacity(capacity, capacity),
            backend_xid: UInt32Builder::with_capacity(capacity),
            backend_xmin: UInt32Builder::with_capacity(capacity),
            query_id: Int64Builder::with_capacity(capacity),
            query: StringBuilder::with_capacity(capacity, capacity),
            backend_type: StringBuilder::with_capacity(capacity, capacity),
        }
    }

    fn add_session(&mut self, session: SessionStatActivity) {
        self.oid.append_value(session.oid);
        self.datname.append_option(session.datname);
        self.pid.append_value(session.pid);
        self.leader_pid.append_option(session.leader_pid);
        self.usesysid.append_option(session.usesysid);
        self.usename.append_option(session.usename);
        self.application_name
            .append_option(session.application_name);
        self.client_addr.append_value(session.client_addr);
        self.client_hostname.append_option(session.client_hostname);
        self.client_port
            .append_value(session.client_port.to_string());
        self.backend_start.append_null();
        self.xact_start.append_null();
        self.query_start.append_null();
        self.state_change.append_null();
        self.wait_event_type.append_null();
        self.wait_event.append_null();
        self.state.append_null();
        self.backend_xid.append_null();
        self.backend_xmin.append_null();
        self.query_id.append_null();
        self.query.append_option(session.query);

        self.backend_type.append_value(&"client backend");
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.datname.finish()),
            Arc::new(self.pid.finish()),
            Arc::new(self.leader_pid.finish()),
            Arc::new(self.usesysid.finish()),
            Arc::new(self.usename.finish()),
            Arc::new(self.application_name.finish()),
            Arc::new(self.client_addr.finish()),
            Arc::new(self.client_hostname.finish()),
            Arc::new(self.client_port.finish()),
            Arc::new(self.backend_start.finish()),
            Arc::new(self.xact_start.finish()),
            Arc::new(self.query_start.finish()),
            Arc::new(self.state_change.finish()),
            Arc::new(self.wait_event_type.finish()),
            Arc::new(self.wait_event.finish()),
            Arc::new(self.state.finish()),
            Arc::new(self.backend_xid.finish()),
            Arc::new(self.backend_xmin.finish()),
            Arc::new(self.query_id.finish()),
            Arc::new(self.query.finish()),
            Arc::new(self.backend_type.finish()),
        ];

        columns
    }
}

pub struct PgCatalogStatActivityProvider {
    sessions: Arc<SessionManager>,
}

impl PgCatalogStatActivityProvider {
    pub fn new(sessions: Arc<SessionManager>) -> Self {
        Self { sessions }
    }
}

#[async_trait]
impl TableProvider for PgCatalogStatActivityProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("datname", DataType::Utf8, true),
            Field::new("pid", DataType::UInt32, false),
            Field::new("leader_pid", DataType::UInt32, true),
            Field::new("usesysid", DataType::UInt32, true),
            Field::new("usename", DataType::Utf8, true),
            Field::new("application_name", DataType::Utf8, true),
            Field::new("client_addr", DataType::Utf8, true),
            Field::new("client_hostname", DataType::Utf8, true),
            Field::new("client_port", DataType::Utf8, true),
            Field::new(
                "backend_start",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "xact_start",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "query_start",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "state_change",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("wait_event_type", DataType::Utf8, true),
            Field::new("wait_event", DataType::Utf8, true),
            // // active: The backend is executing a query.
            // // idle: The backend is waiting for a new client command.
            // // idle in transaction: The backend is in a transaction, but is not currently executing a query.
            // // idle in transaction (aborted): This state is similar to idle in transaction, except one of the statements in the transaction caused an error.
            // // fastpath function call: The backend is executing a fast-path function.
            // // disabled: This state is reported if track_activities is disabled in this backend.
            Field::new("state", DataType::Utf8, true),
            Field::new("backend_xid", DataType::UInt32, true),
            Field::new("backend_xmin", DataType::UInt32, true),
            Field::new("query_id", DataType::Int64, true),
            Field::new("query", DataType::Utf8, true),
            Field::new("backend_type", DataType::Utf8, false),
        ]))
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let sessions = self.sessions.map_sessions::<SessionStatActivity>().await;
        let mut builder = PgStatActivityBuilder::new(sessions.len());

        for session in sessions {
            builder.add_session(session)
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
