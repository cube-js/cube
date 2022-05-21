use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use crate::sql::{session::SessionStatActivity, SessionManager};
use datafusion::{
    arrow::{
        array::{Array, Int32Builder, Int64Builder, StringBuilder, TimestampNanosecondBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

use super::utils::{ExtDataType, OidBuilder, XidBuilder};

struct PgStatActivityBuilder {
    oid: OidBuilder,
    datname: StringBuilder,
    pid: Int32Builder,
    leader_pid: Int32Builder,
    usesysid: OidBuilder,
    usename: StringBuilder,
    application_name: StringBuilder,
    // TODO: type inet?
    client_addr: StringBuilder,
    client_hostname: StringBuilder,
    client_port: Int32Builder,
    backend_start: TimestampNanosecondBuilder,
    xact_start: TimestampNanosecondBuilder,
    query_start: TimestampNanosecondBuilder,
    state_change: TimestampNanosecondBuilder,
    wait_event_type: StringBuilder,
    wait_event: StringBuilder,
    state: StringBuilder,
    backend_xid: XidBuilder,
    backend_xmin: XidBuilder,
    query_id: Int64Builder,
    query: StringBuilder,
    backend_type: StringBuilder,
}

impl PgStatActivityBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            oid: OidBuilder::new(capacity),
            datname: StringBuilder::new(capacity),
            pid: Int32Builder::new(capacity),
            leader_pid: Int32Builder::new(capacity),
            usesysid: OidBuilder::new(capacity),
            usename: StringBuilder::new(capacity),
            application_name: StringBuilder::new(capacity),
            client_addr: StringBuilder::new(capacity),
            client_hostname: StringBuilder::new(capacity),
            client_port: Int32Builder::new(capacity),
            backend_start: TimestampNanosecondBuilder::new(capacity),
            xact_start: TimestampNanosecondBuilder::new(capacity),
            query_start: TimestampNanosecondBuilder::new(capacity),
            state_change: TimestampNanosecondBuilder::new(capacity),
            wait_event_type: StringBuilder::new(capacity),
            wait_event: StringBuilder::new(capacity),
            state: StringBuilder::new(capacity),
            backend_xid: XidBuilder::new(capacity),
            backend_xmin: XidBuilder::new(capacity),
            query_id: Int64Builder::new(capacity),
            query: StringBuilder::new(capacity),
            backend_type: StringBuilder::new(capacity),
        }
    }

    fn add_session(&mut self, session: SessionStatActivity) {
        self.oid.append_value(session.oid).unwrap();
        self.datname.append_option(session.datname).unwrap();
        self.pid.append_value(session.pid).unwrap();
        self.leader_pid.append_null().unwrap();
        self.usesysid.append_null().unwrap();
        self.usename.append_option(session.usename).unwrap();
        self.application_name
            .append_value(session.application_name.unwrap_or("".to_string()))
            .unwrap();
        self.client_addr.append_option(session.client_addr).unwrap();
        self.client_hostname
            .append_option(session.client_hostname)
            .unwrap();
        self.client_port.append_option(session.client_port).unwrap();
        // TODO: non-nullable. Save the time when session began and append it here
        self.backend_start.append_null().unwrap();
        self.xact_start.append_null().unwrap();
        self.query_start.append_null().unwrap();
        self.state_change.append_null().unwrap();
        self.wait_event_type.append_null().unwrap();
        self.wait_event.append_null().unwrap();
        self.state.append_value("active").unwrap();
        self.backend_xid.append_null().unwrap();
        self.backend_xmin.append_value(1).unwrap();
        self.query_id.append_null().unwrap();
        self.query.append_option(session.query).unwrap();
        self.backend_type.append_value("client backend").unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        columns.push(Arc::new(self.oid.finish()));
        columns.push(Arc::new(self.datname.finish()));
        columns.push(Arc::new(self.pid.finish()));
        columns.push(Arc::new(self.leader_pid.finish()));
        columns.push(Arc::new(self.usesysid.finish()));
        columns.push(Arc::new(self.usename.finish()));
        columns.push(Arc::new(self.application_name.finish()));
        columns.push(Arc::new(self.client_addr.finish()));
        columns.push(Arc::new(self.client_hostname.finish()));
        columns.push(Arc::new(self.client_port.finish()));
        columns.push(Arc::new(self.backend_start.finish()));
        columns.push(Arc::new(self.xact_start.finish()));
        columns.push(Arc::new(self.query_start.finish()));
        columns.push(Arc::new(self.state_change.finish()));
        columns.push(Arc::new(self.wait_event_type.finish()));
        columns.push(Arc::new(self.wait_event.finish()));
        columns.push(Arc::new(self.state.finish()));
        columns.push(Arc::new(self.backend_xid.finish()));
        columns.push(Arc::new(self.backend_xmin.finish()));
        columns.push(Arc::new(self.query_id.finish()));
        columns.push(Arc::new(self.query.finish()));
        columns.push(Arc::new(self.backend_type.finish()));

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
            Field::new("oid", ExtDataType::Oid.into(), true),
            Field::new("datname", DataType::Utf8, true),
            Field::new("pid", DataType::Int32, false),
            Field::new("leader_pid", DataType::Int32, true),
            Field::new("usesysid", ExtDataType::Oid.into(), true),
            Field::new("usename", DataType::Utf8, true),
            Field::new("application_name", DataType::Utf8, false),
            Field::new("client_addr", DataType::Utf8, true),
            Field::new("client_hostname", DataType::Utf8, true),
            Field::new("client_port", DataType::Int32, true),
            Field::new(
                "backend_start",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                // TODO: non-nullable
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
            Field::new("backend_xid", ExtDataType::Xid.into(), true),
            Field::new("backend_xmin", ExtDataType::Xid.into(), true),
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
        let sessions = self.sessions.stat_activity();
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
