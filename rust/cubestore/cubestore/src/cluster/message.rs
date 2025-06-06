use crate::metastore::{MetaStoreRpcMethodCall, MetaStoreRpcMethodResult};
use crate::queryplanner::query_executor::SerializedRecordBatchStream;
use crate::queryplanner::serialized_plan::SerializedPlan;
use crate::CubeError;
use datafusion::arrow::datatypes::SchemaRef;
use serde::{Deserialize, Serialize};
use std::io::ErrorKind;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::cluster::WorkerPlanningParams;

#[derive(Serialize, Deserialize, Debug)]
pub enum NetworkMessage {
    /// Route subqueries to other nodes and collect results.
    RouterSelect(SerializedPlan),

    /// Partial select on the worker.
    Select(SerializedPlan, WorkerPlanningParams),
    SelectResult(Result<(SchemaRef, Vec<SerializedRecordBatchStream>), CubeError>),

    //Perform explain analyze of worker query part and return it pretty printed physical plan
    ExplainAnalyze(SerializedPlan, WorkerPlanningParams),
    ExplainAnalyzeResult(Result<String, CubeError>),

    /// Select that sends results in batches. The immediate response is [SelectResultSchema],
    /// followed by a stream of [SelectResultBatch].
    SelectStart(SerializedPlan, WorkerPlanningParams),
    /// Response to [SelectStart].
    SelectResultSchema(Result<SchemaRef, CubeError>),
    /// [None] indicates the end of the stream.
    SelectResultBatch(Result<Option<SerializedRecordBatchStream>, CubeError>),

    WarmupDownload(/*remote_path*/ String, Option<u64>),
    WarmupDownloadResult(Result<(), CubeError>),

    AddMemoryChunk {
        chunk_name: String,
        data: SerializedRecordBatchStream,
    },
    AddMemoryChunkResult(Result<(), CubeError>),

    FreeMemoryChunk {
        chunk_name: String,
    },
    FreeMemoryChunkResult(Result<(), CubeError>),

    FreeDeletedMemoryChunks(Vec<String>),
    FreeDeletedMemoryChunksResult(Result<(), CubeError>),

    MetaStoreCall(MetaStoreRpcMethodCall),
    MetaStoreCallResult(MetaStoreRpcMethodResult),

    NotifyJobListeners,
    NotifyJobListenersSuccess,
}

const MAGIC: u32 = 94107;

const NETWORK_MESSAGE_VERSION: u32 = 1;

impl NetworkMessage {
    pub fn is_streaming_request(&self) -> bool {
        match self {
            NetworkMessage::SelectStart(..) => true,
            _ => false,
        }
    }

    /// Returns true iff the client accepted the message.
    pub async fn maybe_send(&self, socket: &mut TcpStream) -> Result<bool, CubeError> {
        match self.send_impl(socket).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn send(&self, socket: &mut TcpStream) -> Result<(), CubeError> {
        Ok(self.send_impl(socket).await?)
    }

    async fn send_impl(&self, socket: &mut TcpStream) -> Result<(), std::io::Error> {
        let mut ser = flexbuffers::FlexbufferSerializer::new();
        self.serialize(&mut ser).unwrap();
        let message_buffer = ser.take_buffer();
        let len = message_buffer.len() as u64;
        if MAX_NETWORK_MSG_LEN < len {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                format!("network message too large, {} bytes", len),
            ));
        }
        // magic number
        socket.write_u32(MAGIC).await?;
        socket.write_u32(NETWORK_MESSAGE_VERSION).await?;
        socket.write_u64(len).await?;
        socket.write_all(message_buffer.as_slice()).await?;
        Ok(())
    }

    pub async fn receive(socket: &mut TcpStream) -> Result<Self, CubeError> {
        match Self::maybe_receive(socket).await? {
            Some(m) => Ok(m),
            None => Err(CubeError::user("Connection closed unexpectedly. Please check your worker and meta connection environment variables.".to_string())),
        }
    }

    /// Either receives a message or waits for the connection to close.
    pub async fn maybe_receive(socket: &mut TcpStream) -> Result<Option<Self>, CubeError> {
        let magic = socket.read_u32().await;
        if let Err(e) = &magic {
            // TODO: corner case with `0 < n < 8` read bytes.
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
        };
        let magic = magic?;
        if magic != MAGIC {
            return Err(CubeError::user("Corrupted message received. Please check your worker and meta connection environment variables.".to_string()));
        }
        let ver = socket.read_u32().await?;
        if ver != NETWORK_MESSAGE_VERSION {
            return Err(CubeError::user(format!("Network protocol version mismatch. Expected {} but received {}. It seems multiple versions of Cube Store images running within the same cluster.", NETWORK_MESSAGE_VERSION, ver)));
        }
        let len = socket.read_u64().await?;

        if MAX_NETWORK_MSG_LEN < len {
            // Common misconfig of CubeJS can cause it to send HTTP message to the metastore port.
            // The constant is the numeric value of the 'GET /ws ' string.
            if len == 5135603447297962784 {
                return Err(CubeError::internal(format!(
                    "HTTP message on metastore port"
                )));
            }
            return Err(CubeError::internal(format!(
                "invalid metastore message: declared length is too large, {} bytes",
                len
            )));
        }

        let mut buffer = Vec::with_capacity(len as usize);
        socket.take(len).read_to_end(&mut buffer).await?;
        let r = flexbuffers::Reader::get_root(&buffer)?;
        Ok(Some(Self::deserialize(r)?))
    }
}

// Anything larger is considered to be an invalid message.
const MAX_NETWORK_MSG_LEN: u64 = 20 * 1024 * 1024 * 1024; // 20GiB
