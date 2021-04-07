use crate::metastore::{MetaStoreRpcMethodCall, MetaStoreRpcMethodResult};
use crate::queryplanner::query_executor::SerializedRecordBatchStream;
use crate::queryplanner::serialized_plan::SerializedPlan;
use crate::CubeError;
use arrow::datatypes::SchemaRef;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Serialize, Deserialize, Debug)]
pub enum NetworkMessage {
    Select(SerializedPlan),
    SelectResult(Result<Vec<SerializedRecordBatchStream>, CubeError>),

    /// Select that sends results in batches.
    SelectStart(SerializedPlan),
    /// Response to [SelectStart].
    SelectResultSchema(Result<SchemaRef, CubeError>),
    SelectNextBatch,
    /// Response to [SelectNextBatch].
    /// [None] indicates the end of the stream, no further requests should be made.
    SelectResultBatch(Result<Option<SerializedRecordBatchStream>, CubeError>),

    WarmupDownload(/*remote_path*/ String),
    WarmupDownloadResult(Result<(), CubeError>),

    MetaStoreCall(MetaStoreRpcMethodCall),
    MetaStoreCallResult(MetaStoreRpcMethodResult),

    NotifyJobListeners,
    NotifyJobListenersSuccess,
}

impl NetworkMessage {
    pub fn is_streaming_request(&self) -> bool {
        match self {
            NetworkMessage::SelectStart(..) => true,
            _ => false,
        }
    }

    pub async fn send(&self, socket: &mut TcpStream) -> Result<(), CubeError> {
        let mut ser = flexbuffers::FlexbufferSerializer::new();
        self.serialize(&mut ser).unwrap();
        let message_buffer = ser.take_buffer();
        socket.write_u64(message_buffer.len() as u64).await?;
        socket.write_all(message_buffer.as_slice()).await?;
        Ok(())
    }

    pub async fn receive(socket: &mut TcpStream) -> Result<Self, CubeError> {
        match Self::maybe_receive(socket).await? {
            Some(m) => Ok(m),
            None => Err(CubeError::internal("connection closed".to_string())),
        }
    }

    /// Either receives a message or waits for the connection to close.
    pub async fn maybe_receive(socket: &mut TcpStream) -> Result<Option<Self>, CubeError> {
        let len = socket.read_u64().await;
        if let Err(e) = &len {
            // TODO: corner case with `0 < n < 8` read bytes.
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
        };
        let len = len?;

        let mut buffer = Vec::with_capacity(len as usize);
        socket.take(len).read_to_end(&mut buffer).await?;
        let r = flexbuffers::Reader::get_root(&buffer)?;
        Ok(Some(Self::deserialize(r)?))
    }
}
