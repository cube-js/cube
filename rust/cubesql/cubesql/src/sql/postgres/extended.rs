use crate::{
    compile::QueryPlan,
    sql::{
        dataframe::{batch_to_dataframe, DataFrame, TableValue},
        statement::PostgresStatementParamsBinder,
        writer::BatchWriter,
    },
    CubeError,
};
use chrono::{DateTime, Utc};
use datafusion::arrow::record_batch::RecordBatch;
use pg_srv::{protocol, BindValue, PgTypeId, ProtocolError};
use sqlparser::ast;
use std::{fmt, pin::Pin};

use crate::sql::shim::{ConnectionError, QueryPlanExt};
use datafusion::{
    arrow::array::Array, dataframe::DataFrame as DFDataFrame,
    physical_plan::SendableRecordBatchStream,
};
use futures::*;
use pg_srv::protocol::{PortalCompletion, PortalSuspended};

use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::stream::StreamExt;

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
    },
}

impl PreparedStatement {
    pub fn get_created(&self) -> &DateTime<Utc> {
        match self {
            PreparedStatement::Empty { created, .. } => created,
            PreparedStatement::Query { created, .. } => created,
        }
    }

    /// Format parser ast::Statement as String
    pub fn get_query_as_string(&self) -> String {
        match self {
            PreparedStatement::Empty { .. } => "".to_string(),
            PreparedStatement::Query { query, .. } => query.to_string(),
        }
    }

    pub fn get_from_sql(&self) -> bool {
        match self {
            PreparedStatement::Empty { from_sql, .. } => from_sql.clone(),
            PreparedStatement::Query { from_sql, .. } => from_sql.clone(),
        }
    }

    pub fn get_parameters(&self) -> Option<&Vec<PgTypeId>> {
        match self {
            PreparedStatement::Empty { .. } => None,
            PreparedStatement::Query { parameters, .. } => Some(&parameters.parameters),
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

pub struct PortalBatch {
    pub writer: Option<BatchWriter>,
    pub description: Option<protocol::RowDescription>,
}

pub enum PortalResponse {
    Batch(PortalBatch),
    Completion(protocol::PortalCompletion),
}

#[derive(Debug)]
pub struct Portal {
    // Format which is used to return data
    format: protocol::Format,
    from: PortalFrom,
    // State which holds corresponding data for each step. Option is used for dereferencing
    state: Option<PortalState>,
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
    pub fn new(plan: QueryPlan, format: protocol::Format, from: PortalFrom) -> Self {
        Self {
            format,
            from,
            state: Some(PortalState::Prepared(PreparedState { plan })),
        }
    }

    pub fn new_empty(format: protocol::Format, from: PortalFrom) -> Self {
        Self {
            format,
            from,
            state: Some(PortalState::Empty),
        }
    }

    pub fn get_description(&self) -> Result<Option<protocol::RowDescription>, ConnectionError> {
        match &self.state {
            Some(PortalState::Prepared(state)) => state.plan.to_row_description(self.format),
            Some(PortalState::InExecutionFrame(state)) => Ok(state.description.clone()),
            Some(PortalState::InExecutionStream(state)) => Ok(state.description.clone()),
            Some(PortalState::Finished(state)) => Ok(state.description.clone()),
            Some(PortalState::Empty) => Err(ConnectionError::Cube(CubeError::internal(
                "Unable to get description on empty Portal. It's a bug.".to_string(),
            ))),
            None => Err(ConnectionError::Cube(CubeError::internal(
                "Unable to get description on Portal without state. It's a bug.".to_string(),
            ))),
        }
    }

    pub fn is_empty(&self) -> bool {
        match &self.state {
            Some(PortalState::Empty) => true,
            _ => false,
        }
    }

    pub fn get_format(&self) -> protocol::Format {
        self.format.clone()
    }

    fn hand_execution_frame_state<'a>(
        &'a mut self,
        frame_state: InExecutionFrameState,
        max_rows: usize,
    ) -> impl Stream<Item = Result<PortalResponse, ConnectionError>> + 'a {
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

                yield Ok(PortalResponse::Batch(self.new_batch(Some(writer), frame_state.description.clone())?));

                self.state = Some(PortalState::Finished(FinishedState {
                    description: frame_state.description,
                }));

                return yield Ok(PortalResponse::Completion(self.new_portal_completion(num_rows, false)));
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
                    TableValue::Null => writer.write_value::<Option<bool>>(None)?,
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
                *left = *left - batch.num_rows();
                batch
            }
        };

        let frame = batch_to_dataframe(batch_for_write.schema().as_ref(), &vec![batch_for_write])?;

        Ok((unused, self.dataframe_to_writer(frame)?))
    }

    fn hand_execution_stream_state<'a>(
        &'a mut self,
        mut stream_state: InExecutionStreamState,
        max_rows: usize,
    ) -> impl Stream<Item = Result<PortalResponse, ConnectionError>> + 'a {
        stream! {
            let mut left: usize = max_rows;
            let mut writer: Option<BatchWriter> = None;

            if let Some(unused_batch) = stream_state.unused.take() {
                let (usused_batch, batch_writer) = self.iterate_stream_batch(unused_batch, max_rows, &mut left)?;
                stream_state.unused = usused_batch;
                writer = Some(batch_writer);
            }

            if max_rows > 0 && left == 0 {
                self.state = Some(PortalState::InExecutionStream(stream_state));

                let num_rows = match &writer {
                    Some(writer) => writer.num_rows() as u32,
                    None => 0,
                };

                yield Ok(PortalResponse::Batch(self.new_batch(writer, self.get_description()?)?));
                yield Ok(PortalResponse::Completion(self.new_portal_completion(num_rows, true)));

                return;
            }

            let mut num_of_rows = 0;
            let mut description = stream_state.description.clone();

            loop {
                match stream_state.stream.next().await {
                    None => {
                        self.state = Some(PortalState::Finished(FinishedState {
                            description: stream_state.description,
                        }));

                        return yield Ok(PortalResponse::Completion(self.new_portal_completion(0, false)));
                    }
                    Some(res) => match res {
                        Ok(batch) => {
                            let (unused_batch, writer) = self.iterate_stream_batch(batch, max_rows, &mut left)?;

                            num_of_rows += writer.num_rows() as u32;

                            yield Ok(PortalResponse::Batch(self.new_batch(Some(writer), description)?));

                            description = None;

                            if max_rows > 0 && left == 0 {
                                stream_state.unused = unused_batch;

                                self.state = Some(PortalState::InExecutionStream(stream_state));

                                return yield Ok(PortalResponse::Completion(self.new_portal_completion(num_of_rows, true)));
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
    ) -> impl Stream<Item = Result<PortalResponse, ConnectionError>> + 'a {
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

                            return yield Ok(PortalResponse::Completion(PortalCompletion::Complete(
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
                        QueryPlan::DataFusionSelect(_, plan, ctx) => {
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

                    return yield Ok(PortalResponse::Completion(self.new_portal_completion(0, false)));
                }
            }
        }
    }

    fn new_batch(
        &self,
        writer: Option<BatchWriter>,
        description: Option<protocol::RowDescription>,
    ) -> Result<PortalBatch, ConnectionError> {
        Ok(PortalBatch {
            writer,
            description,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::{
        dataframe::{Column, DataFrame, Row, TableValue},
        extended::{InExecutionFrameState, Portal, PortalState},
        ColumnFlags, ColumnType,
    };
    use pg_srv::protocol::Format;

    use crate::sql::{extended::PortalFrom, shim::ConnectionError};
    use datafusion::arrow::{
        array::{ArrayRef, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use futures_util::stream::StreamExt;
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
        };

        let mut portal = Pin::new(&mut p);
        let stream = portal.execute(10);
        pin_mut!(stream);

        let response = stream.next().await.unwrap()?;
        match response {
            PortalResponse::Batch(batch) => {
                assert_eq!(3, batch.writer.unwrap().num_rows());
            }
            PortalResponse::Completion(_) => panic!("must be batch here"),
        }

        let response = stream.next().await.unwrap()?;
        match response {
            PortalResponse::Batch(_) => panic!("must be Completion here"),
            PortalResponse::Completion(_) => (),
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
        };

        let mut portal = Pin::new(&mut p);
        let stream = portal.execute(1);
        pin_mut!(stream);

        let response = stream.next().await.unwrap();
        match response {
            Ok(_) => panic!("must panic"),
            Err(e) => assert_eq!(
                e.to_string(),
                "Error: Cursor with limited max_rows: 1 for DataFrame is not supported"
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
                None,
            ))),
        };

        let mut portal = Pin::new(&mut p);
        let stream = portal.execute(0);
        pin_mut!(stream);

        let response = stream.next().await.unwrap()?;
        match response {
            PortalResponse::Batch(batch) => assert_eq!(3, batch.writer.unwrap().num_rows()),
            PortalResponse::Completion(_) => panic!("must be Batch here"),
        }

        Ok(())
    }

    // #[tokio::test]
    // async fn test_portal_df_stream_single_batch() -> Result<(), ConnectionError> {
    //     let mut writer = BatchWriter::new(Format::Binary);

    //     let ctx = SessionContext::new();
    //     let table = Arc::new(InfoSchemaTestingDatasetProvider::new(1, 250));
    //     let stream = ctx.read_table(table)?.execute_stream().await?;

    //     let mut portal = Portal {
    //         format: Format::Binary,
    //         from: PortalFrom::Extended,
    //         state: Some(PortalState::InExecutionStream(InExecutionStreamState::new(
    //             stream, None,
    //         ))),
    //     };

    //     let completion = portal.execute(&mut writer, 1).await?;
    //     // batch 1 will be spited to 250 -1 (unused) and 1
    //     assert_eq!(1, writer.num_rows());
    //     assert_eq!(
    //         completion,
    //         PortalCompletion::Suspended(PortalSuspended::new())
    //     );

    //     // usage of unused batch, 249 - 6 (unused) and 6
    //     let completion = portal.execute(&mut writer, 5).await?;
    //     assert_eq!(6, writer.num_rows());
    //     assert_eq!(
    //         completion,
    //         PortalCompletion::Suspended(PortalSuspended::new())
    //     );

    //     // usage of unused batch
    //     let completion = portal.execute(&mut writer, 1000).await?;
    //     assert_eq!(250, writer.num_rows());
    //     assert_eq!(
    //         completion,
    //         PortalCompletion::Complete(CommandComplete::Select(250))
    //     );

    //     Ok(())
    // }

    // #[tokio::test]
    // async fn test_portal_df_stream_small_batches() -> Result<(), ConnectionError> {
    //     let mut writer = BatchWriter::new(Format::Binary);

    //     let ctx = SessionContext::new();
    //     let table = Arc::new(InfoSchemaTestingDatasetProvider::new(10, 15));
    //     let stream = ctx.read_table(table)?.execute_stream().await?;

    //     let mut portal = Portal {
    //         format: Format::Binary,
    //         from: PortalFrom::Extended,
    //         state: Some(PortalState::InExecutionStream(InExecutionStreamState::new(
    //             stream, None,
    //         ))),
    //     };

    //     // use 1 batch
    //     let completion = portal.execute(&mut writer, 10).await.unwrap();
    //     assert_eq!(10, writer.num_rows());
    //     assert_eq!(
    //         completion,
    //         PortalCompletion::Suspended(PortalSuspended::new())
    //     );

    //     // use 2 batch
    //     let completion = portal.execute(&mut writer, 20).await.unwrap();
    //     assert_eq!(30, writer.num_rows());
    //     assert_eq!(
    //         completion,
    //         PortalCompletion::Suspended(PortalSuspended::new())
    //     );

    //     // use 0.5 batch
    //     portal.execute(&mut writer, 5).await.unwrap();
    //     assert_eq!(35, writer.num_rows());

    //     portal.execute(&mut writer, 15).await.unwrap();
    //     assert_eq!(50, writer.num_rows());

    //     // use 7 batches
    //     let completion = portal.execute(&mut writer, 1000).await.unwrap();
    //     assert_eq!(150, writer.num_rows());
    //     assert_eq!(
    //         completion,
    //         PortalCompletion::Complete(CommandComplete::Select(150))
    //     );

    //     Ok(())
    // }
}
