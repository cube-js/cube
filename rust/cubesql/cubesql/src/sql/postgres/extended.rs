use crate::{
    compile::QueryPlan,
    sql::dataframe::{batch_to_dataframe, DataFrame, TableValue},
    sql::protocol,
    sql::protocol::{CommandComplete, Format, ParameterDescription, RowDescription},
    sql::statement::{BindValue, StatementParamsBinder},
    sql::writer::BatchWriter,
    CubeError,
};
use datafusion::arrow::record_batch::RecordBatch;
use sqlparser::ast;
use std::fmt;

use datafusion::dataframe::DataFrame as DFDataFrame;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;

#[derive(Debug)]
pub struct PreparedStatement {
    pub query: ast::Statement,
    pub parameters: ParameterDescription,
    // Fields which will be returned to the client, It can be None if server doesnt return any field
    // for example BEGIN
    pub description: Option<RowDescription>,
}

impl PreparedStatement {
    pub fn bind(&self, values: Vec<BindValue>) -> ast::Statement {
        let binder = StatementParamsBinder::new(values);
        let mut statement = self.query.clone();
        binder.bind(&mut statement);

        statement
    }
}

pub struct PreparedState {
    plan: QueryPlan,
    description: Option<protocol::RowDescription>,
}

impl fmt::Debug for PreparedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("plan: hidden")
    }
}

#[derive(Debug)]
pub struct InExecutionFrameState {
    // Format which is used to return data
    batch: DataFrame,
}

pub struct InExecutionStreamState {
    stream: SendableRecordBatchStream,
    // DF return batch with which unknown size what we cannot control, but client can send max_rows
    // < then batch size and we need to persist somewhere unused part of RecordBatch
    unused: Option<RecordBatch>,
}

impl InExecutionStreamState {
    fn new(stream: SendableRecordBatchStream) -> Self {
        Self {
            stream,
            unused: None,
        }
    }
}

impl fmt::Debug for InExecutionStreamState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("stream: hidden")
    }
}

#[derive(Debug)]
pub enum PortalState {
    Prepared(PreparedState),
    #[allow(dead_code)]
    InExecutionFrame(InExecutionFrameState),
    InExecutionStream(InExecutionStreamState),
    Finished,
}

#[derive(Debug)]
pub struct Portal {
    // Format which is used to return data
    format: Format,
    // State which holds corresponding data for each step. Option is used for dereferencing
    state: Option<PortalState>,
}

unsafe impl Send for Portal {}
unsafe impl Sync for Portal {}

impl Portal {
    pub fn new(
        plan: QueryPlan,
        format: Format,
        description: Option<protocol::RowDescription>,
    ) -> Self {
        Self {
            format,
            state: Some(PortalState::Prepared(PreparedState { plan, description })),
        }
    }

    pub fn get_description(&self) -> Option<protocol::RowDescription> {
        match &self.state {
            Some(PortalState::Prepared(state)) => state.description.clone(),
            _ => None,
        }
    }

    pub fn get_format(&self) -> Format {
        self.format.clone()
    }

    async fn hand_execution_frame_state(
        &mut self,
        writer: &mut BatchWriter,
        frame_state: InExecutionFrameState,
        max_rows: usize,
    ) -> Result<(PortalState, CommandComplete), CubeError> {
        let rows_read = frame_state.batch.len();
        if max_rows > 0 && rows_read > 0 && rows_read > max_rows {
            Err(CubeError::internal(format!(
                "Cursor with limited max_rows: {} for DataFrame is not supported",
                max_rows
            )))
        } else {
            self.write_dataframe_to_writer(
                writer,
                frame_state.batch,
                if max_rows == 0 { rows_read } else { max_rows },
            )?;

            Ok((
                PortalState::Finished,
                CommandComplete::Select(writer.num_rows() as u32),
            ))
        }
    }

    fn write_dataframe_to_writer(
        &self,
        writer: &mut BatchWriter,
        frame: DataFrame,
        rows_to_read: usize,
    ) -> Result<(), CubeError> {
        for (idx, row) in frame.get_rows().iter().enumerate() {
            // TODO: It's a hack, because we dont limit batch_to_dataframe by number of expected rows
            if idx >= rows_to_read {
                break;
            }

            for value in row.values() {
                match value {
                    TableValue::Null => writer.write_value::<Option<bool>>(None)?,
                    TableValue::String(v) => writer.write_value(v.clone())?,
                    TableValue::Int64(v) => writer.write_value(*v)?,
                    TableValue::Boolean(v) => writer.write_value(*v)?,
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
    ) -> Result<Option<RecordBatch>, CubeError> {
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
    ) -> Result<(PortalState, CommandComplete), CubeError> {
        let mut left: usize = max_rows;

        if let Some(unused_batch) = stream_state.unused.take() {
            stream_state.unused =
                self.iterate_stream_batch(writer, unused_batch, max_rows, &mut left)?;
        };

        if max_rows > 0 && left == 0 {
            return Ok((
                PortalState::InExecutionStream(stream_state),
                CommandComplete::Select(writer.num_rows() as u32),
            ));
        }

        loop {
            match stream_state.stream.next().await {
                None => {
                    return Ok((
                        PortalState::Finished,
                        CommandComplete::Select(writer.num_rows() as u32),
                    ))
                }
                Some(res) => match res {
                    Ok(batch) => {
                        stream_state.unused =
                            self.iterate_stream_batch(writer, batch, max_rows, &mut left)?;

                        if max_rows > 0 && left == 0 {
                            return Ok((
                                PortalState::InExecutionStream(stream_state),
                                CommandComplete::Select(writer.num_rows() as u32),
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
    ) -> Result<CommandComplete, CubeError> {
        if let Some(state) = self.state.take() {
            match state {
                PortalState::Prepared(state) => match state.plan {
                    QueryPlan::MetaOk(_, completion) => {
                        self.state = Some(PortalState::Finished);

                        Ok(completion.clone().to_pg_command())
                    }
                    QueryPlan::MetaTabular(_, batch) => {
                        let new_state = InExecutionFrameState { batch: *batch };
                        let (next_state, complete) = self
                            .hand_execution_frame_state(writer, new_state, max_rows)
                            .await?;

                        self.state = Some(next_state);

                        Ok(complete)
                    }
                    QueryPlan::DataFusionSelect(_, plan, ctx) => {
                        let df = DFDataFrame::new(ctx.state.clone(), &plan);
                        let stream = df.execute_stream().await?;

                        let new_state = InExecutionStreamState::new(stream);
                        let (next_state, complete) = self
                            .hand_execution_stream_state(writer, new_state, max_rows)
                            .await?;
                        self.state = Some(next_state);

                        Ok(complete)
                    }
                },
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
                PortalState::Finished => Ok(CommandComplete::Select(0)),
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
        sql::dataframe::{Column, DataFrame, Row, TableValue},
        sql::extended::{InExecutionFrameState, InExecutionStreamState, Portal, PortalState},
        sql::protocol::Format,
        sql::writer::BatchWriter,
        sql::{ColumnFlags, ColumnType},
        CubeError,
    };

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
    async fn test_portal_legacy_dataframe_limited_more() -> Result<(), CubeError> {
        let mut writer = BatchWriter::new(Format::Binary);

        let mut portal = Portal {
            format: Format::Binary,
            state: Some(PortalState::InExecutionFrame(InExecutionFrameState {
                batch: generate_testing_data_frame(3),
            })),
        };

        portal.execute(&mut writer, 10).await?;
        // Batch will not be split, because clients wants more rows then in batch
        assert_eq!(3, writer.num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn test_portal_legacy_dataframe_limited_less() -> Result<(), CubeError> {
        let mut writer = BatchWriter::new(Format::Binary);

        let mut portal = Portal {
            format: Format::Binary,
            state: Some(PortalState::InExecutionFrame(InExecutionFrameState {
                batch: generate_testing_data_frame(3),
            })),
        };

        let res = portal.execute(&mut writer, 1).await;
        match res {
            Ok(_) => panic!("must panic"),
            Err(e) => assert_eq!(
                e.message,
                "Cursor with limited max_rows: 1 for DataFrame is not supported"
            ),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_portal_legacy_dataframe_unlimited() -> Result<(), CubeError> {
        let mut writer = BatchWriter::new(Format::Binary);

        let mut portal = Portal {
            format: Format::Binary,
            state: Some(PortalState::InExecutionFrame(InExecutionFrameState {
                batch: generate_testing_data_frame(3),
            })),
        };

        portal.execute(&mut writer, 0).await?;
        assert_eq!(3, writer.num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn test_portal_df_stream_single_batch() -> Result<(), CubeError> {
        let mut writer = BatchWriter::new(Format::Binary);

        let ctx = SessionContext::new();
        let table = Arc::new(InfoSchemaTestingDatasetProvider::new(1, 250));
        let stream = ctx.read_table(table)?.execute_stream().await?;

        let mut portal = Portal {
            format: Format::Binary,
            state: Some(PortalState::InExecutionStream(InExecutionStreamState {
                stream,
                unused: None,
            })),
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
    async fn test_portal_df_stream_small_batches() -> Result<(), CubeError> {
        let mut writer = BatchWriter::new(Format::Binary);

        let ctx = SessionContext::new();
        let table = Arc::new(InfoSchemaTestingDatasetProvider::new(10, 15));
        let stream = ctx.read_table(table)?.execute_stream().await?;

        let mut portal = Portal {
            format: Format::Binary,
            state: Some(PortalState::InExecutionStream(InExecutionStreamState {
                stream,
                unused: None,
            })),
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
