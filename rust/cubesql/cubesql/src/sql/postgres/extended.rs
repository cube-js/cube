use crate::{
    compile::QueryPlan,
    sql::{
        dataframe::{batch_to_dataframe, DataFrame, TableValue},
        statement::StatementParamsBinder,
        writer::BatchWriter,
    },
    CubeError,
};
use datafusion::arrow::record_batch::RecordBatch;
use pg_srv::{protocol, BindValue, ProtocolError};
use sqlparser::ast;
use std::fmt;

use crate::sql::shim::{ConnectionError, QueryPlanExt};
use datafusion::{dataframe::DataFrame as DFDataFrame, physical_plan::SendableRecordBatchStream};
use futures::StreamExt;

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
pub struct PreparedStatement {
    pub query: ast::Statement,
    pub parameters: protocol::ParameterDescription,
    // Fields which will be returned to the client, It can be None if server doesnt return any field
    // for example BEGIN
    pub description: Option<protocol::RowDescription>,
}

impl PreparedStatement {
    pub fn bind(&self, values: Vec<BindValue>) -> ast::Statement {
        let binder = StatementParamsBinder::new(values);
        let mut statement = self.query.clone();
        binder.bind(&mut statement);

        statement
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
    Prepared(PreparedState),
    #[allow(dead_code)]
    InExecutionFrame(InExecutionFrameState),
    InExecutionStream(InExecutionStreamState),
    Finished(FinishedState),
}

#[derive(Debug)]
pub struct Portal {
    // Format which is used to return data
    format: protocol::Format,
    // State which holds corresponding data for each step. Option is used for dereferencing
    state: Option<PortalState>,
}

unsafe impl Send for Portal {}
unsafe impl Sync for Portal {}

impl Portal {
    pub fn new(plan: QueryPlan, format: protocol::Format) -> Self {
        Self {
            format,
            state: Some(PortalState::Prepared(PreparedState { plan })),
        }
    }

    pub fn get_description(&self) -> Result<Option<protocol::RowDescription>, ConnectionError> {
        match &self.state {
            Some(PortalState::Prepared(state)) => state.plan.to_row_description(self.format),
            Some(PortalState::InExecutionFrame(state)) => Ok(state.description.clone()),
            Some(PortalState::InExecutionStream(state)) => Ok(state.description.clone()),
            Some(PortalState::Finished(state)) => Ok(state.description.clone()),
            _ => Err(ConnectionError::Cube(CubeError::internal(
                "Unable to get description on Portal without state. It's a bug.".to_string(),
            ))),
        }
    }

    pub fn get_format(&self) -> protocol::Format {
        self.format.clone()
    }

    async fn hand_execution_frame_state(
        &mut self,
        writer: &mut BatchWriter,
        frame_state: InExecutionFrameState,
        max_rows: usize,
    ) -> Result<(PortalState, protocol::CommandComplete), ProtocolError> {
        let rows_read = frame_state.batch.len();
        if max_rows > 0 && rows_read > 0 && rows_read > max_rows {
            Err(protocol::ErrorResponse::error(
                protocol::ErrorCode::FeatureNotSupported,
                format!(
                    "Cursor with limited max_rows: {} for DataFrame is not supported",
                    max_rows
                ),
            )
            .into())
        } else {
            self.write_dataframe_to_writer(
                writer,
                frame_state.batch,
                if max_rows == 0 { rows_read } else { max_rows },
            )?;

            Ok((
                PortalState::Finished(FinishedState {
                    description: frame_state.description,
                }),
                protocol::CommandComplete::Select(writer.num_rows() as u32),
            ))
        }
    }

    fn write_dataframe_to_writer(
        &self,
        writer: &mut BatchWriter,
        frame: DataFrame,
        rows_to_read: usize,
    ) -> Result<(), ProtocolError> {
        for (idx, row) in frame.get_rows().iter().enumerate() {
            // TODO: It's a hack, because we dont limit batch_to_dataframe by number of expected rows
            if idx >= rows_to_read {
                break;
            }

            for value in row.values() {
                match value {
                    TableValue::Null => writer.write_value::<Option<bool>>(None)?,
                    TableValue::String(v) => writer.write_value(v.clone())?,
                    TableValue::Int16(v) => writer.write_value(*v)?,
                    TableValue::Int32(v) => writer.write_value(*v)?,
                    TableValue::Int64(v) => writer.write_value(*v)?,
                    TableValue::Boolean(v) => writer.write_value(*v)?,
                    TableValue::Float32(v) => writer.write_value(*v)?,
                    TableValue::Float64(v) => writer.write_value(*v)?,
                    TableValue::List(v) => writer.write_value(v.clone())?,
                    TableValue::Timestamp(v) => writer.write_value(v.clone())?,
                };
            }

            writer.end_row()?;
        }

        Ok(())
    }

    fn iterate_stream_batch(
        &mut self,
        writer: &mut BatchWriter,
        batch: RecordBatch,
        max_rows: usize,
        left: &mut usize,
    ) -> Result<Option<RecordBatch>, ConnectionError> {
        let mut unused: Option<RecordBatch> = None;

        let (batch_for_write, rows_to_read) = if max_rows == 0 {
            let batch_num_rows = batch.num_rows();
            (batch, batch_num_rows)
        } else {
            if batch.num_rows() > *left {
                let unused_batch = batch.slice(*left, batch.num_rows() - *left);
                unused = Some(unused_batch);

                let r = (batch, *left);
                *left = 0;

                r
            } else {
                *left = *left - batch.num_rows();
                let batch_num_rows = batch.num_rows();
                (batch, batch_num_rows)
            }
        };

        // TODO: Split doesn't split batches, it copy the part, lets dont convert whole batch to dataframe
        let frame = batch_to_dataframe(&vec![batch_for_write])?;
        self.write_dataframe_to_writer(writer, frame, rows_to_read)?;

        Ok(unused)
    }

    async fn hand_execution_stream_state(
        &mut self,
        writer: &mut BatchWriter,
        mut stream_state: InExecutionStreamState,
        max_rows: usize,
    ) -> Result<(PortalState, protocol::CommandComplete), ConnectionError> {
        let mut left: usize = max_rows;

        if let Some(unused_batch) = stream_state.unused.take() {
            stream_state.unused =
                self.iterate_stream_batch(writer, unused_batch, max_rows, &mut left)?;
        };

        if max_rows > 0 && left == 0 {
            return Ok((
                PortalState::InExecutionStream(stream_state),
                protocol::CommandComplete::Select(writer.num_rows() as u32),
            ));
        }

        loop {
            match stream_state.stream.next().await {
                None => {
                    return Ok((
                        PortalState::Finished(FinishedState {
                            description: stream_state.description,
                        }),
                        protocol::CommandComplete::Select(writer.num_rows() as u32),
                    ))
                }
                Some(res) => match res {
                    Ok(batch) => {
                        stream_state.unused =
                            self.iterate_stream_batch(writer, batch, max_rows, &mut left)?;

                        if max_rows > 0 && left == 0 {
                            return Ok((
                                PortalState::InExecutionStream(stream_state),
                                protocol::CommandComplete::Select(writer.num_rows() as u32),
                            ));
                        }
                    }
                    Err(err) => {
                        return Err(err.into());
                    }
                },
            }
        }
    }

    pub async fn execute(
        &mut self,
        writer: &mut BatchWriter,
        max_rows: usize,
    ) -> Result<protocol::CommandComplete, ConnectionError> {
        if let Some(state) = self.state.take() {
            match state {
                PortalState::Prepared(state) => {
                    let description = state.plan.to_row_description(self.format)?;
                    match state.plan {
                        QueryPlan::MetaOk(_, completion) => {
                            self.state = Some(PortalState::Finished(FinishedState { description }));

                            Ok(completion.clone().to_pg_command())
                        }
                        QueryPlan::MetaTabular(_, batch) => {
                            let new_state = InExecutionFrameState::new(*batch, description);
                            let (next_state, complete) = self
                                .hand_execution_frame_state(writer, new_state, max_rows)
                                .await?;

                            self.state = Some(next_state);

                            Ok(complete)
                        }
                        QueryPlan::DataFusionSelect(_, plan, ctx) => {
                            let df = DFDataFrame::new(ctx.state.clone(), &plan);
                            let stream = df.execute_stream().await?;

                            let new_state = InExecutionStreamState::new(stream, description);
                            let (next_state, complete) = self
                                .hand_execution_stream_state(writer, new_state, max_rows)
                                .await?;
                            self.state = Some(next_state);

                            Ok(complete)
                        }
                    }
                }
                PortalState::InExecutionFrame(frame_state) => {
                    let (next_state, complete) = self
                        .hand_execution_frame_state(writer, frame_state, max_rows)
                        .await?;

                    self.state = Some(next_state);

                    Ok(complete)
                }
                PortalState::InExecutionStream(stream_state) => {
                    let (next_state, complete) = self
                        .hand_execution_stream_state(writer, stream_state, max_rows)
                        .await?;

                    self.state = Some(next_state);

                    Ok(complete)
                }
                PortalState::Finished(finish_state) => {
                    self.state = Some(PortalState::Finished(finish_state));

                    Ok(protocol::CommandComplete::Select(0))
                }
            }
        } else {
            unreachable!();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        compile::engine::information_schema::postgres::testing_dataset::InfoSchemaTestingDatasetProvider,
        sql::{
            dataframe::{Column, DataFrame, Row, TableValue},
            extended::{InExecutionFrameState, InExecutionStreamState, Portal, PortalState},
            writer::BatchWriter,
            ColumnFlags, ColumnType,
        },
    };
    use pg_srv::protocol::Format;

    use crate::sql::shim::ConnectionError;
    use datafusion::prelude::SessionContext;
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

    #[tokio::test]
    async fn test_portal_legacy_dataframe_limited_more() -> Result<(), ConnectionError> {
        let mut writer = BatchWriter::new(Format::Binary);

        let mut portal = Portal {
            format: Format::Binary,
            state: Some(PortalState::InExecutionFrame(InExecutionFrameState::new(
                generate_testing_data_frame(3),
                None,
            ))),
        };

        portal.execute(&mut writer, 10).await?;
        // Batch will not be split, because clients wants more rows then in batch
        assert_eq!(3, writer.num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn test_portal_legacy_dataframe_limited_less() -> Result<(), ConnectionError> {
        let mut writer = BatchWriter::new(Format::Binary);

        let mut portal = Portal {
            format: Format::Binary,
            state: Some(PortalState::InExecutionFrame(InExecutionFrameState::new(
                generate_testing_data_frame(3),
                None,
            ))),
        };

        let res = portal.execute(&mut writer, 1).await;
        match res {
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
        let mut writer = BatchWriter::new(Format::Binary);

        let mut portal = Portal {
            format: Format::Binary,
            state: Some(PortalState::InExecutionFrame(InExecutionFrameState::new(
                generate_testing_data_frame(3),
                None,
            ))),
        };

        portal.execute(&mut writer, 0).await?;
        assert_eq!(3, writer.num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn test_portal_df_stream_single_batch() -> Result<(), ConnectionError> {
        let mut writer = BatchWriter::new(Format::Binary);

        let ctx = SessionContext::new();
        let table = Arc::new(InfoSchemaTestingDatasetProvider::new(1, 250));
        let stream = ctx.read_table(table)?.execute_stream().await?;

        let mut portal = Portal {
            format: Format::Binary,
            state: Some(PortalState::InExecutionStream(InExecutionStreamState::new(
                stream, None,
            ))),
        };

        portal.execute(&mut writer, 1).await?;
        // batch 1 will be spited to 250 -1 (unused) and 1
        assert_eq!(1, writer.num_rows());

        // usage of unused batch, 249 - 6 (unused) and 6
        portal.execute(&mut writer, 5).await?;
        assert_eq!(6, writer.num_rows());

        // usage of unused batch
        portal.execute(&mut writer, 1000).await?;
        assert_eq!(250, writer.num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn test_portal_df_stream_small_batches() -> Result<(), ConnectionError> {
        let mut writer = BatchWriter::new(Format::Binary);

        let ctx = SessionContext::new();
        let table = Arc::new(InfoSchemaTestingDatasetProvider::new(10, 15));
        let stream = ctx.read_table(table)?.execute_stream().await?;

        let mut portal = Portal {
            format: Format::Binary,
            state: Some(PortalState::InExecutionStream(InExecutionStreamState::new(
                stream, None,
            ))),
        };

        // use 1 batch
        portal.execute(&mut writer, 10).await.unwrap();
        assert_eq!(10, writer.num_rows());

        // use 2 batch
        portal.execute(&mut writer, 20).await.unwrap();
        assert_eq!(30, writer.num_rows());

        // use 0.5 batch
        portal.execute(&mut writer, 5).await.unwrap();
        assert_eq!(35, writer.num_rows());

        portal.execute(&mut writer, 15).await.unwrap();
        assert_eq!(50, writer.num_rows());

        // use 7 batches
        portal.execute(&mut writer, 1000).await.unwrap();
        assert_eq!(150, writer.num_rows());

        Ok(())
    }
}
