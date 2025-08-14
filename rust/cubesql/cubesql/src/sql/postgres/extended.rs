use crate::{
    compile::QueryPlan,
    sql::{
        dataframe::{batches_to_dataframe, DataFrame, TableValue},
        statement::PostgresStatementParamsBinder,
        temp_tables::TempTable,
        writer::BatchWriter,
    },
    CubeError,
};
use chrono::{DateTime, Utc};
use datafusion::arrow::record_batch::RecordBatch;
use pg_srv::{protocol, BindValue, PgTypeId, ProtocolError};
use sqlparser::ast;
use std::{fmt, pin::Pin, sync::Arc};

use crate::sql::shim::{ConnectionError, QueryPlanExt};
use datafusion::{
    arrow::array::Array, dataframe::DataFrame as DFDataFrame,
    physical_plan::SendableRecordBatchStream,
};
use futures::{FutureExt, Stream, StreamExt};
use pg_srv::protocol::{CommandComplete, PortalCompletion, PortalSuspended};

use crate::transport::SpanId;
use async_stream::stream;

#[derive(Debug)]
pub struct Cursor {
    pub query: ast::Statement,
    // WITH HOLD specifies that the cursor can continue to be used after the transaction that created it successfully commits.
    // WITHOUT HOLD specifies that the cursor cannot be used outside of the transaction that created it.
    pub hold: bool,
    // What format will be used for Cursor
    pub format: protocol::Format,
}

#[derive(Debug)]
pub enum PreparedStatement {
    // Postgres allows to define prepared statement on empty query: "",
    // then it requires special handling in the protocol
    Empty {
        /// Prepared statement can be declared from SQL or protocol (Parser)
        from_sql: bool,
        created: DateTime<Utc>,
        span_id: Option<Arc<SpanId>>,
    },
    Query {
        /// Prepared statement can be declared from SQL or protocol (Parser)
        from_sql: bool,
        created: DateTime<Utc>,
        query: ast::Statement,
        parameters: protocol::ParameterDescription,
        /// Fields which will be returned to the client, It can be None if server doesnt return any field
        /// for example BEGIN
        description: Option<protocol::RowDescription>,
        span_id: Option<Arc<SpanId>>,
    },
    Error {
        /// Prepared statement can be declared from SQL or protocol (Parser)
        from_sql: bool,
        sql: String,
        created: DateTime<Utc>,
        span_id: Option<Arc<SpanId>>,
    },
}

impl PreparedStatement {
    pub fn get_created(&self) -> &DateTime<Utc> {
        match self {
            PreparedStatement::Empty { created, .. } => created,
            PreparedStatement::Query { created, .. } => created,
            PreparedStatement::Error { created, .. } => created,
        }
    }

    /// Format parser ast::Statement as String
    pub fn get_query_as_string(&self) -> String {
        match self {
            PreparedStatement::Empty { .. } => "".to_string(),
            PreparedStatement::Query { query, .. } => query.to_string(),
            PreparedStatement::Error { sql, .. } => sql.clone(),
        }
    }

    pub fn get_from_sql(&self) -> bool {
        match self {
            PreparedStatement::Empty { from_sql, .. } => *from_sql,
            PreparedStatement::Query { from_sql, .. } => *from_sql,
            PreparedStatement::Error { from_sql, .. } => *from_sql,
        }
    }

    pub fn get_parameters(&self) -> Option<&Vec<PgTypeId>> {
        match self {
            PreparedStatement::Empty { .. } => None,
            PreparedStatement::Query { parameters, .. } => Some(&parameters.parameters),
            PreparedStatement::Error { .. } => None,
        }
    }

    pub fn bind(&self, values: Vec<BindValue>) -> Result<ast::Statement, ConnectionError> {
        match self {
            PreparedStatement::Empty { .. } => Err(CubeError::internal(
                "It's not possible bind empty prepared statement (it's a bug)".to_string(),
            )
            .into()),
            PreparedStatement::Query { query, .. } => {
                let binder = PostgresStatementParamsBinder::new(values);
                let mut statement = query.clone();
                binder.bind(&mut statement)?;

                Ok(statement)
            }
            PreparedStatement::Error { .. } => Err(CubeError::internal(
                "It's not possible to bind errored prepared statements (it's a bug)".to_string(),
            )
            .into()),
        }
    }

    pub fn span_id(&self) -> Option<Arc<SpanId>> {
        match self {
            PreparedStatement::Empty { span_id, .. } => span_id.clone(),
            PreparedStatement::Query { span_id, .. } => span_id.clone(),
            PreparedStatement::Error { span_id, .. } => span_id.clone(),
        }
    }
}

#[derive(Debug)]
pub struct PreparedState {
    plan: QueryPlan,
}

#[derive(Debug)]
pub struct FinishedState {
    description: Option<protocol::RowDescription>,
}

#[derive(Debug)]
pub struct InExecutionFrameState {
    // Format which is used to return data
    batch: DataFrame,
    description: Option<protocol::RowDescription>,
}

impl InExecutionFrameState {
    fn new(batch: DataFrame, description: Option<protocol::RowDescription>) -> Self {
        Self { batch, description }
    }
}

pub struct InExecutionStreamState {
    stream: SendableRecordBatchStream,
    // DF return batch with which unknown size what we cannot control, but client can send max_rows
    // < then batch size and we need to persist somewhere unused part of RecordBatch
    unused: Option<RecordBatch>,
    description: Option<protocol::RowDescription>,
}

impl InExecutionStreamState {
    fn new(
        stream: SendableRecordBatchStream,
        description: Option<protocol::RowDescription>,
    ) -> Self {
        Self {
            stream,
            description,
            unused: None,
        }
    }
}

impl fmt::Debug for InExecutionStreamState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("stream: hidden, ")?;

        if let Some(batch) = &self.unused {
            f.write_str(&format!("unused: Some(num_rows: {})", batch.num_rows()))
        } else {
            f.write_str("unused: None")
        }
    }
}

#[derive(Debug)]
pub enum PortalState {
    Empty,
    Prepared(PreparedState),
    #[allow(dead_code)]
    InExecutionFrame(InExecutionFrameState),
    InExecutionStream(InExecutionStreamState),
    Finished(FinishedState),
}

#[derive(Debug, PartialEq)]
pub enum PortalFrom {
    Simple,
    Fetch,
    Extended,
}

#[derive(Debug)]
pub enum PortalBatch {
    Description(protocol::RowDescription),
    Rows(BatchWriter),
    Completion(protocol::PortalCompletion),
}

#[derive(Debug)]
pub struct Portal {
    // Format which is used to return data
    format: protocol::Format,
    from: PortalFrom,
    // State which holds corresponding data for each step. Option is used for dereferencing
    state: Option<PortalState>,
    span_id: Option<Arc<SpanId>>,
}

unsafe impl Send for Portal {}
unsafe impl Sync for Portal {}

fn split_record_batch(batch: RecordBatch, mid: usize) -> (RecordBatch, Option<RecordBatch>) {
    if batch.num_rows() <= mid {
        return (batch, None);
    }

    let schema = batch.schema();
    let mut left = Vec::with_capacity(schema.fields().len());
    let mut right = Vec::with_capacity(schema.fields().len());

    for column in batch.columns() {
        left.push(column.slice(0, mid));
        right.push(column.slice(mid, column.len() - mid));
    }

    (
        RecordBatch::try_new(schema.clone(), left).unwrap(),
        Some(RecordBatch::try_new(schema, right).unwrap()),
    )
}

impl Portal {
    pub fn new(
        plan: QueryPlan,
        format: protocol::Format,
        from: PortalFrom,
        span_id: Option<Arc<SpanId>>,
    ) -> Self {
        Self {
            format,
            from,
            span_id,
            state: Some(PortalState::Prepared(PreparedState { plan })),
        }
    }

    pub fn new_empty(
        format: protocol::Format,
        from: PortalFrom,
        span_id: Option<Arc<SpanId>>,
    ) -> Self {
        Self {
            format,
            from,
            span_id,
            state: Some(PortalState::Empty),
        }
    }

    pub fn get_description(&self) -> Result<Option<protocol::RowDescription>, ConnectionError> {
        match &self.state {
            Some(PortalState::Prepared(state)) => state.plan.to_row_description(self.format),
            Some(PortalState::InExecutionFrame(state)) => Ok(state.description.clone()),
            Some(PortalState::InExecutionStream(state)) => Ok(state.description.clone()),
            Some(PortalState::Finished(state)) => Ok(state.description.clone()),
            Some(PortalState::Empty) => Err(ConnectionError::Cube(
                CubeError::internal(
                    "Unable to get description on empty Portal. It's a bug.".to_string(),
                ),
                None,
            )),
            None => Err(ConnectionError::Cube(
                CubeError::internal(
                    "Unable to get description on Portal without state. It's a bug.".to_string(),
                ),
                None,
            )),
        }
    }

    pub fn is_empty(&self) -> bool {
        match &self.state {
            Some(PortalState::Empty) => true,
            _ => false,
        }
    }

    pub fn get_format(&self) -> protocol::Format {
        self.format
    }

    fn hand_execution_frame_state<'a>(
        &'a mut self,
        frame_state: InExecutionFrameState,
        max_rows: usize,
    ) -> impl Stream<Item = Result<PortalBatch, ConnectionError>> + 'a {
        stream! {
            let rows_read = frame_state.batch.len();
            if max_rows > 0 && rows_read > 0 && rows_read > max_rows {
                return yield Err(protocol::ErrorResponse::error(
                    protocol::ErrorCode::FeatureNotSupported,
                    format!(
                        "Cursor with limited max_rows: {} for DataFrame is not supported",
                        max_rows
                    ),
                )
                .into());
            } else {
                let writer = self.dataframe_to_writer(frame_state.batch)?;
                let num_rows = writer.num_rows() as u32;

                if let Some(description) = &frame_state.description {
                    yield Ok(PortalBatch::Description(description.clone()));
                }

                yield Ok(PortalBatch::Rows(writer));

                self.state = Some(PortalState::Finished(FinishedState {
                    description: frame_state.description,
                }));

                return yield Ok(PortalBatch::Completion(self.new_portal_completion(num_rows, false)));
            }
        }
    }

    pub fn new_portal_completion(&self, rows: u32, has_more: bool) -> protocol::PortalCompletion {
        match self.from {
            PortalFrom::Simple => {
                protocol::PortalCompletion::Complete(protocol::CommandComplete::Select(rows))
            }
            PortalFrom::Fetch => {
                protocol::PortalCompletion::Complete(protocol::CommandComplete::Fetch(rows))
            }
            PortalFrom::Extended => {
                if has_more {
                    protocol::PortalCompletion::Suspended(PortalSuspended::new())
                } else {
                    protocol::PortalCompletion::Complete(protocol::CommandComplete::Select(rows))
                }
            }
        }
    }

    fn dataframe_to_writer(&self, frame: DataFrame) -> Result<BatchWriter, ProtocolError> {
        let mut writer = BatchWriter::new(self.get_format());

        for row in frame.to_rows().into_iter() {
            for value in row.to_values() {
                match value {
                    TableValue::Null => writer.write_value::<Option<String>>(None)?,
                    TableValue::String(v) => writer.write_value(v)?,
                    TableValue::Int16(v) => writer.write_value(v)?,
                    TableValue::Int32(v) => writer.write_value(v)?,
                    TableValue::Int64(v) => writer.write_value(v)?,
                    TableValue::Boolean(v) => writer.write_value(v)?,
                    TableValue::Float32(v) => writer.write_value(v)?,
                    TableValue::Float64(v) => writer.write_value(v)?,
                    TableValue::List(v) => writer.write_value(v)?,
                    TableValue::Timestamp(v) => writer.write_value(v)?,
                    TableValue::Date(v) => writer.write_value(v)?,
                    TableValue::Decimal128(v) => writer.write_value(v)?,
                    TableValue::Interval(v) => writer.write_value(v)?,
                };
            }

            writer.end_row()?;
        }

        Ok(writer)
    }

    fn iterate_stream_batch(
        &self,
        batch: RecordBatch,
        max_rows: usize,
        left: &mut usize,
    ) -> Result<(Option<RecordBatch>, BatchWriter), ConnectionError> {
        let mut unused: Option<RecordBatch> = None;

        let batch_for_write = if max_rows == 0 {
            batch
        } else {
            if batch.num_rows() > *left {
                let (batch, right) = split_record_batch(batch, *left);
                unused = right;
                *left = 0;

                batch
            } else {
                *left -= batch.num_rows();
                batch
            }
        };

        let frame = batches_to_dataframe(batch_for_write.schema().as_ref(), vec![batch_for_write])?;

        Ok((unused, self.dataframe_to_writer(frame)?))
    }

    fn hand_execution_stream_state<'a>(
        &'a mut self,
        mut stream_state: InExecutionStreamState,
        max_rows: usize,
    ) -> impl Stream<Item = Result<PortalBatch, ConnectionError>> + 'a {
        stream! {
            let mut left: usize = max_rows;
            let mut num_of_rows = 0;

            if let Some(description) = &stream_state.description {
                yield Ok(PortalBatch::Description(description.clone()));
            }

            if let Some(unused_batch) = stream_state.unused.take() {
                let (usused_batch, batch_writer) = self.iterate_stream_batch(unused_batch, max_rows, &mut left)?;
                stream_state.unused = usused_batch;
                num_of_rows = batch_writer.num_rows() as u32;

                yield Ok(PortalBatch::Rows(batch_writer));
            }

            if max_rows > 0 && left == 0 {
                self.state = Some(PortalState::InExecutionStream(stream_state));

                return yield Ok(PortalBatch::Completion(self.new_portal_completion(num_of_rows, true)));
            }

            loop {
                match stream_state.stream.next().await {
                    None => {
                        self.state = Some(PortalState::Finished(FinishedState {
                            description: stream_state.description,
                        }));

                        return yield Ok(PortalBatch::Completion(self.new_portal_completion(num_of_rows, false)));
                    }
                    Some(res) => match res {
                        Ok(batch) => {
                            let (unused_batch, writer) = self.iterate_stream_batch(batch, max_rows, &mut left)?;

                            num_of_rows += writer.num_rows() as u32;

                            yield Ok(PortalBatch::Rows(writer));

                            if max_rows > 0 && left == 0 {
                                stream_state.unused = unused_batch;

                                self.state = Some(PortalState::InExecutionStream(stream_state));

                                return yield Ok(PortalBatch::Completion(self.new_portal_completion(num_of_rows, true)));
                            }
                        }
                        Err(err) => return yield Err(err.into()),
                    },
                }
            }
        }
    }

    #[tracing::instrument(level = "trace")]
    pub fn execute<'a>(
        self: &'a mut Pin<&'a mut Self>,
        max_rows: usize,
    ) -> impl Stream<Item = Result<PortalBatch, ConnectionError>> + 'a {
        stream! {
            let state = self
                .state
                .take()
                .ok_or_else(|| CubeError::internal("Unable to take portal state".to_string()))?;

            match state {
                PortalState::Empty => {
                    self.state = Some(PortalState::Empty);

                    return yield Err(
                        CubeError::internal("Unable to execute empty portal, it's a bug".to_string())
                            .into(),
                    );
                }
                PortalState::Prepared(state) => {
                    let description = state.plan.to_row_description(self.format)?;
                    match state.plan {
                        QueryPlan::MetaOk(_, completion) => {
                            self.state = Some(PortalState::Finished(FinishedState { description }));

                            return yield Ok(PortalBatch::Completion(PortalCompletion::Complete(
                                completion.clone().to_pg_command(),
                            )));
                        }
                        QueryPlan::MetaTabular(_, batch) => {
                            let stream = self.hand_execution_frame_state(InExecutionFrameState::new(*batch, description), max_rows);
                            for await value in stream {
                                yield value;
                            }

                            return;
                        }
                        QueryPlan::DataFusionSelect(plan, ctx) => {
                            let df = DFDataFrame::new(ctx.state.clone(), &plan);
                            let safe_stream = async move {
                                std::panic::AssertUnwindSafe(df.execute_stream())
                                    .catch_unwind()
                                    .await
                            };
                            match safe_stream.await {
                                Ok(sendable_batch) => {
                                    let stream = self.hand_execution_stream_state(InExecutionStreamState::new(sendable_batch?, description), max_rows);
                                    for await value in stream {
                                        yield value;
                                    }

                                    return;
                                }
                                Err(err) => return yield Err(CubeError::panic(err).into()),
                            }
                        }
                        QueryPlan::CreateTempTable(plan, ctx, name, temp_tables) => {
                            let df = DFDataFrame::new(ctx.state.clone(), &plan);
                            let record_batch = df.collect();
                            let row_count = match record_batch.await {
                                Ok(record_batch) => {
                                    let row_count: u32 = record_batch.iter().map(|batch| batch.num_rows() as u32).sum();
                                    let temp_table = TempTable::new(Arc::clone(plan.schema()), vec![record_batch]);
                                    let save_result = tokio::task::spawn_blocking(move || {
                                        temp_tables.save(&name.to_ascii_lowercase(), temp_table)
                                    }).await?;
                                    if let Err(err) = save_result {
                                        return yield Err(err.into())
                                    };
                                    row_count
                                }
                                Err(err) => return yield Err(CubeError::panic(Box::new(err)).into()),
                            };

                            self.state = Some(PortalState::Finished(FinishedState { description }));

                            return yield Ok(PortalBatch::Completion(PortalCompletion::Complete(
                                CommandComplete::Select(row_count),
                            )));
                        }
                    }
                }
                PortalState::InExecutionFrame(frame_state) => {
                    let stream = self.hand_execution_frame_state(frame_state, max_rows);
                    for await value in stream {
                        yield value;
                    }

                    return;
                }
                PortalState::InExecutionStream(stream_state) => {
                    let stream = self.hand_execution_stream_state(stream_state, max_rows);
                    for await value in stream {
                        yield value;
                    }

                    return;
                }
                PortalState::Finished(finish_state) => {
                    self.state = Some(PortalState::Finished(finish_state));

                    if let Some(description) = self.get_description()? {
                        yield Ok(PortalBatch::Description(description.clone()));
                    }

                    return yield Ok(PortalBatch::Completion(self.new_portal_completion(0, false)));
                }
            }
        }
    }

    pub fn span_id(&self) -> Option<Arc<SpanId>> {
        self.span_id.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        compile::engine::information_schema::postgres::InfoSchemaTestingDatasetProvider,
        sql::{
            dataframe::{Column, DataFrame, Row, TableValue},
            extended::{InExecutionFrameState, Portal, PortalState},
            ColumnFlags, ColumnType,
        },
    };
    use pg_srv::protocol::Format;

    use crate::sql::{extended::PortalFrom, shim::ConnectionError};
    use datafusion::{
        arrow::{
            array::{ArrayRef, StringArray},
            datatypes::{DataType, Field, Schema},
        },
        prelude::SessionContext,
    };
    use futures::StreamExt;
    use std::pin::pin;
    use std::sync::Arc;

    fn generate_testing_data_frame(cnt: usize) -> DataFrame {
        let mut rows = vec![];

        for _i in 0..cnt {
            rows.push(Row::new(vec![TableValue::String("Row1".to_string())]));
        }

        DataFrame::new(
            vec![Column::new(
                "Col1".to_string(),
                ColumnType::String,
                ColumnFlags::empty(),
            )],
            rows,
        )
    }

    #[test]
    fn test_split_record_batch() -> Result<(), ConnectionError> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "KibanaSampleDataEcommerce.count",
            DataType::Utf8,
            false,
        )]));
        let column1 = Arc::new(StringArray::from(vec![
            Some("1"),
            Some("2"),
            Some("3"),
            Some("4"),
            Some("5"),
            Some("6"),
        ])) as ArrayRef;

        // 0
        {
            let (left, right) = split_record_batch(
                RecordBatch::try_new(schema.clone(), vec![column1.clone()])?,
                0,
            );
            assert_eq!(left.num_rows(), 0);
            assert_eq!(right.unwrap().num_rows(), 6);
        }

        // 3
        {
            let (left, right) = split_record_batch(
                RecordBatch::try_new(schema.clone(), vec![column1.clone()])?,
                3,
            );
            assert_eq!(left.num_rows(), 3);

            let left_column = left
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(left_column.value(0), "1");
            assert_eq!(left_column.value(1), "2");
            assert_eq!(left_column.value(2), "3");

            let right = right.unwrap();
            assert_eq!(right.num_rows(), 3);

            let right_column = right
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(right_column.value(0), "4");
            assert_eq!(right_column.value(1), "5");
            assert_eq!(right_column.value(2), "6");
        }

        // 6
        {
            let (left, right) =
                split_record_batch(RecordBatch::try_new(schema.clone(), vec![column1])?, 6);
            assert_eq!(left.num_rows(), 6);
            assert_eq!(right, None);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_portal_legacy_dataframe_limited_more() -> Result<(), ConnectionError> {
        let mut p = Portal {
            format: Format::Binary,
            from: PortalFrom::Extended,
            state: Some(PortalState::InExecutionFrame(InExecutionFrameState::new(
                generate_testing_data_frame(3),
                None,
            ))),
            span_id: None,
        };

        let mut portal = Pin::new(&mut p);
        let stream = portal.execute(10);
        let mut stream = pin!(stream);

        let response = stream.next().await.unwrap()?;
        match response {
            PortalBatch::Rows(writer) => {
                assert_eq!(3, writer.num_rows());
            }
            _ => panic!("must be rows here"),
        }

        let response = stream.next().await.unwrap()?;
        match response {
            PortalBatch::Completion(_) => (),
            _ => panic!("must be Completion here"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_portal_legacy_dataframe_limited_less() -> Result<(), ConnectionError> {
        let mut p = Portal {
            format: Format::Binary,
            from: PortalFrom::Extended,
            state: Some(PortalState::InExecutionFrame(InExecutionFrameState::new(
                generate_testing_data_frame(3),
                None,
            ))),
            span_id: None,
        };

        let mut portal = Pin::new(&mut p);
        let stream = portal.execute(1);
        let mut stream = pin!(stream);

        let response = stream.next().await.unwrap();
        match response {
            Ok(_) => panic!("must panic"),
            Err(e) => assert_eq!(
                e.to_string(),
                "ProtocolError: Error: Cursor with limited max_rows: 1 for DataFrame is not supported"
            ),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_portal_legacy_dataframe_unlimited() -> Result<(), ConnectionError> {
        let mut p = Portal {
            format: Format::Binary,
            from: PortalFrom::Extended,
            state: Some(PortalState::InExecutionFrame(InExecutionFrameState::new(
                generate_testing_data_frame(3),
                Some(protocol::RowDescription::new(vec![])),
            ))),
            span_id: None,
        };

        let mut portal = Pin::new(&mut p);
        let stream = portal.execute(0);
        let mut stream = pin!(stream);

        let response = stream.next().await.unwrap()?;
        match response {
            PortalBatch::Description(_) => (),
            _ => panic!("must be Description here"),
        }

        let response = stream.next().await.unwrap()?;
        match response {
            PortalBatch::Rows(writer) => assert_eq!(3, writer.num_rows()),
            _ => panic!("must be rows here"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_portal_df_stream_single_batch() -> Result<(), ConnectionError> {
        let ctx = SessionContext::new();
        let table = Arc::new(InfoSchemaTestingDatasetProvider::new(1, 250));
        let stream = ctx.read_table(table)?.execute_stream().await?;

        let mut portal = Portal {
            format: Format::Binary,
            from: PortalFrom::Extended,
            state: Some(PortalState::InExecutionStream(InExecutionStreamState::new(
                stream,
                Some(protocol::RowDescription::new(vec![])),
            ))),
            span_id: None,
        };

        execute_portal_single_batch(&mut portal, 1, 1).await?;
        execute_portal_single_batch(&mut portal, 5, 5).await?;
        execute_portal_single_batch(&mut portal, 1000, 244).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_portal_df_stream_small_batches() -> Result<(), ConnectionError> {
        let ctx = SessionContext::new();
        let table = Arc::new(InfoSchemaTestingDatasetProvider::new(10, 15));
        let stream = ctx.read_table(table)?.execute_stream().await?;

        let mut portal = Portal {
            format: Format::Binary,
            from: PortalFrom::Extended,
            state: Some(PortalState::InExecutionStream(InExecutionStreamState::new(
                stream,
                Some(protocol::RowDescription::new(vec![])),
            ))),
            span_id: None,
        };

        // use 1 batch
        execute_portal(&mut portal, 10, 10).await?;

        // use 2 batch
        execute_portal(&mut portal, 20, 20).await?;

        // use 0.5 batch
        execute_portal(&mut portal, 5, 5).await?;

        execute_portal(&mut portal, 15, 15).await?;

        // use 7 batches
        execute_portal(&mut portal, 1000, 100).await?;

        Ok(())
    }

    async fn execute_portal_single_batch(
        portal: &mut Portal,
        max_rows: usize,
        expecting_rows: u32,
    ) -> Result<(), ConnectionError> {
        let mut p = Pin::new(portal);
        let stream = p.execute(max_rows);
        let mut stream = pin!(stream);

        match stream.next().await.unwrap()? {
            PortalBatch::Description(_) => (),
            _ => panic!("must be Description here"),
        }

        match stream.next().await.unwrap()? {
            PortalBatch::Rows(writer) => assert_eq!(expecting_rows, writer.num_rows()),
            _ => panic!("must be Rows here"),
        }

        match stream.next().await.unwrap()? {
            PortalBatch::Completion(_) => (),
            _ => panic!("must be Completion here"),
        }

        Ok(())
    }

    async fn execute_portal(
        portal: &mut Portal,
        max_rows: usize,
        expecting_rows: u32,
    ) -> Result<(), ConnectionError> {
        let mut p = Pin::new(portal);
        let stream = p.execute(max_rows);
        let mut stream = pin!(stream);

        let mut total_rows = 0;
        while let Some(batch) = stream.next().await {
            match batch? {
                PortalBatch::Rows(writer) => total_rows += writer.num_rows(),
                _ => (),
            }
        }

        assert_eq!(expecting_rows, total_rows);

        Ok(())
    }
}
