use crate::metastore::{MetaStoreRpcMethodCall, MetaStoreRpcMethodResult};
use crate::queryplanner::query_executor::SerializedRecordBatchStream;
use crate::queryplanner::serialized_plan::SerializedPlan;
use crate::trace::{MainTrace, WorkerTrace};
use crate::util::metrics;
use crate::{app_metrics, CubeError};
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

    /// Detailed-trace mirror of [RouterSelect]: the entry node asks a main worker to
    /// run the full router plan for real and return the assembled `MainTrace`.
    RouterSelectDetailed(SerializedPlan),
    RouterSelectDetailedResult(Result<MainTrace, CubeError>),

    /// Detailed-trace mirror of [Select]: a worker runs its part for real and returns
    /// both the result rows (for the main to merge) and its `WorkerTrace`.
    SelectDetailed(SerializedPlan, WorkerPlanningParams),
    SelectDetailedResult(
        Result<(SchemaRef, Vec<SerializedRecordBatchStream>, WorkerTrace), CubeError>,
    ),

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
    /// The failure a response message carries. A metastore call result is not looked into.
    pub fn error(&self) -> Option<&CubeError> {
        match self {
            NetworkMessage::SelectResult(Err(e))
            | NetworkMessage::ExplainAnalyzeResult(Err(e))
            | NetworkMessage::RouterSelectDetailedResult(Err(e))
            | NetworkMessage::SelectDetailedResult(Err(e))
            | NetworkMessage::SelectResultSchema(Err(e))
            | NetworkMessage::SelectResultBatch(Err(e))
            | NetworkMessage::WarmupDownloadResult(Err(e))
            | NetworkMessage::AddMemoryChunkResult(Err(e))
            | NetworkMessage::FreeMemoryChunkResult(Err(e))
            | NetworkMessage::FreeDeletedMemoryChunksResult(Err(e)) => Some(e),
            _ => None,
        }
    }

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
            count_decode_error("version_mismatch");
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
        // A peer that goes away mid-message leaves a short buffer: a dropped connection, not a
        // message this build cannot decode.
        if (buffer.len() as u64) < len {
            return Err(CubeError::internal(format!(
                "Connection closed after {} of {} message bytes",
                buffer.len(),
                len
            )));
        }
        let decoded = flexbuffers::Reader::get_root(&buffer)
            .map_err(CubeError::from)
            .and_then(|r| Ok(Self::deserialize(r)?));
        if decoded.is_err() {
            count_decode_error("undecodable");
        }
        Ok(Some(decoded?))
    }
}

fn count_decode_error(reason: &'static str) {
    app_metrics::RPC_DECODE_ERRORS
        .add_with_tags(1, Some(&vec![metrics::format_tag("reason", reason)]));
}

// Anything larger is considered to be an invalid message.
const MAX_NETWORK_MSG_LEN: u64 = 20 * 1024 * 1024 * 1024; // 20GiB

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    async fn receive_bytes(write: impl FnOnce(&mut Vec<u8>)) -> Result<NetworkMessage, CubeError> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut bytes = Vec::new();
        write(&mut bytes);
        let writer = tokio::spawn(async move {
            let mut socket = TcpStream::connect(addr).await.unwrap();
            socket.write_all(&bytes).await.unwrap();
        });
        let (mut socket, _) = listener.accept().await.unwrap();
        writer.await.unwrap();
        NetworkMessage::receive(&mut socket).await
    }

    fn header(bytes: &mut Vec<u8>, version: u32, len: u64) {
        bytes.extend_from_slice(&MAGIC.to_be_bytes());
        bytes.extend_from_slice(&version.to_be_bytes());
        bytes.extend_from_slice(&len.to_be_bytes());
    }

    #[tokio::test]
    async fn message_cut_short_is_a_closed_connection_not_a_decode_error() {
        crate::util::metrics::test_capture::watch("cs.rpc.decode_errors");
        let before = decode_errors("undecodable");
        let err = receive_bytes(|bytes| {
            header(bytes, NETWORK_MESSAGE_VERSION, 100);
            bytes.extend_from_slice(&[0; 10]);
        })
        .await
        .unwrap_err();
        assert!(
            err.message
                .contains("Connection closed after 10 of 100 message bytes"),
            "{}",
            err.message
        );
        assert_eq!(decode_errors("undecodable"), before);
    }

    fn decode_errors(reason: &str) -> usize {
        let tags = vec![format!("reason:{}", reason)];
        crate::util::metrics::test_capture::samples("cs.rpc.decode_errors")
            .iter()
            .filter(|(_, t)| *t == tags)
            .count()
    }

    #[tokio::test]
    async fn decode_errors_are_counted_by_reason() {
        crate::util::metrics::test_capture::watch("cs.rpc.decode_errors");

        let before = decode_errors("undecodable");
        let err = receive_bytes(|bytes| {
            header(bytes, NETWORK_MESSAGE_VERSION, 4);
            bytes.extend_from_slice(&[0xff; 4]);
        })
        .await;
        assert!(err.is_err());
        assert!(decode_errors("undecodable") > before);

        let before = decode_errors("version_mismatch");
        let err = receive_bytes(|bytes| header(bytes, NETWORK_MESSAGE_VERSION + 1, 0)).await;
        assert!(err.unwrap_err().message.contains("version mismatch"));
        assert!(decode_errors("version_mismatch") > before);
    }
}
