use crate::{
    sql::{dataframe, temp_tables::TempTableManager},
    CubeError,
};
use bitflags::bitflags;
use core::fmt;
use datafusion::dataframe::DataFrame;
use std::{fmt::Formatter, pin::Pin, sync::Arc};

use datafusion::{
    execution::context::SessionContext as DFSessionContext,
    logical_plan::LogicalPlan,
    physical_plan::{ExecutionPlan, RecordBatchStream},
};
use futures_util::FutureExt;

bitflags! {
    pub struct StatusFlags: u8 {
        const SERVER_STATE_CHANGED = 0b00000001;
        const AUTOCOMMIT           = 0b00000010;
    }
}

#[derive(Debug, Clone)]
pub enum CommandCompletion {
    Begin,
    Prepare,
    Commit,
    Use,
    Rollback,
    Set,
    Select(u32),
    DeclareCursor,
    CloseCursor,
    CloseCursorAll,
    Deallocate,
    DeallocateAll,
    Discard(String),
    DropTable,
}

pub enum QueryPlan {
    // Meta will not be executed in DF,
    // we already knows how respond to it
    MetaOk(StatusFlags, CommandCompletion),
    MetaTabular(StatusFlags, Box<dataframe::DataFrame>),
    // Query will be executed via Data Fusion
    DataFusionSelect(LogicalPlan, DFSessionContext),
    // Query will be executed via DataFusion and saved to session
    CreateTempTable(LogicalPlan, DFSessionContext, String, Arc<TempTableManager>),
}

impl fmt::Debug for QueryPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryPlan::MetaOk(flags, completion) => {
                f.write_str(&format!(
                    "MetaOk(StatusFlags: {:?}, CommandCompletion: {:?})", flags, completion
                ))
            },
            QueryPlan::MetaTabular(flags, _) => {
                f.write_str(&format!(
                    "MetaTabular(StatusFlags: {:?}, DataFrame: hidden)",
                    flags
                ))
            },
            QueryPlan::DataFusionSelect(_, _) => {
                f.write_str(&"DataFusionSelect(LogicalPlan: hidden, DFSessionContext: hidden)")
            },
            QueryPlan::CreateTempTable(_, _, name, _) => {
                f.write_str(&format!(
                    "CreateTempTable(LogicalPlan: hidden, DFSessionContext: hidden, Name: {}, SessionState: hidden",
                    name
                ))
            },
        }
    }
}

impl QueryPlan {
    pub fn as_logical_plan(&self) -> LogicalPlan {
        match self {
            QueryPlan::DataFusionSelect(plan, _) | QueryPlan::CreateTempTable(plan, _, _, _) => {
                plan.clone()
            }
            QueryPlan::MetaOk(_, _) | QueryPlan::MetaTabular(_, _) => {
                panic!("This query doesnt have a plan, because it already has values for response")
            }
        }
    }

    pub async fn as_physical_plan(&self) -> Result<Arc<dyn ExecutionPlan>, CubeError> {
        match self {
            QueryPlan::DataFusionSelect(plan, ctx)
            | QueryPlan::CreateTempTable(plan, ctx, _, _) => {
                DataFrame::new(ctx.state.clone(), plan)
                    .create_physical_plan()
                    .await
                    .map_err(|e| CubeError::user(e.to_string()))
            }
            QueryPlan::MetaOk(_, _) | QueryPlan::MetaTabular(_, _) => {
                panic!("This query doesnt have a plan, because it already has values for response")
            }
        }
    }

    pub fn print(&self, pretty: bool) -> Result<String, CubeError> {
        match self {
            QueryPlan::DataFusionSelect(plan, _) | QueryPlan::CreateTempTable(plan, _, _, _) => {
                if pretty {
                    Ok(plan.display_indent().to_string())
                } else {
                    Ok(plan.display().to_string())
                }
            }
            QueryPlan::MetaOk(_, _) | QueryPlan::MetaTabular(_, _) => Ok(
                "This query doesnt have a plan, because it already has values for response"
                    .to_string(),
            ),
        }
    }
}

pub async fn get_df_batches(
    plan: &QueryPlan,
) -> Result<Pin<Box<dyn RecordBatchStream + Send>>, CubeError> {
    match plan {
        QueryPlan::DataFusionSelect(plan, ctx) => {
            let df = DataFrame::new(ctx.state.clone(), &plan);
            let safe_stream = async move {
                std::panic::AssertUnwindSafe(df.execute_stream())
                    .catch_unwind()
                    .await
            };
            match safe_stream.await {
                Ok(sendable_batch) => {
                    match sendable_batch {
                        Ok(stream) => {
                            return Ok(stream);
                        }
                        Err(err) => return Err(err.into()),
                    };
                }
                Err(err) => return Err(CubeError::panic(err)),
            }
        }
        _ => Err(CubeError::user(
            "Only SELECT queries are supported for Cube SQL over HTTP".to_string(),
        )),
    }
}
