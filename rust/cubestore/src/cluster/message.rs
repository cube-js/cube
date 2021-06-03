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
    /// Route subqueries to other nodes and collect results.
    RouterSelect(SerializedPlan),

    /// Partial select on the worker.
    Select(SerializedPlan),
    SelectResult(Result<(SchemaRef, Vec<SerializedRecordBatchStream>), CubeError>),

    /// Select that sends results in batches. The immediate response is [SelectResultSchema],
    /// followed by a stream of [SelectResultBatch].
    SelectStart(SerializedPlan),
    /// Response to [SelectStart].
    SelectResultSchema(Result<SchemaRef, CubeError>),
    /// [None] indicates the end of the stream.
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
