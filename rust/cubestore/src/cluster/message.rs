use crate::queryplanner::serialized_plan::SerializedPlan;
use serde::{Serialize, Deserialize};
use tokio::net::TcpStream;
use crate::CubeError;
use crate::queryplanner::query_executor::SerializedRecordBatchStream;
use tokio::io::{AsyncWriteExt, AsyncReadExt};

#[derive(Serialize, Deserialize, Debug)]
pub enum NetworkMessage {
    Select(SerializedPlan),
    SelectResult(Result<SerializedRecordBatchStream, CubeError>)
}

impl NetworkMessage {
    pub async fn send(&self, socket: &mut TcpStream) -> Result<(), CubeError> {
        let mut ser = flexbuffers::FlexbufferSerializer::new();
        self.serialize(&mut ser).unwrap();
        let message_buffer = ser.take_buffer();
        socket.write_u64(message_buffer.len() as u64).await?;
        socket.write_all(message_buffer.as_slice()).await?;
        Ok(())
    }

    pub async fn receive(socket: &mut TcpStream) -> Result<Self, CubeError> {
        let len = socket.read_u64().await?;
        let mut buffer = Vec::with_capacity(len as usize);
        socket.take(len).read_to_end(&mut buffer).await?;
        let r = flexbuffers::Reader::get_root(&buffer)?;
        let message = Self::deserialize(r)?;
        Ok(message)
    }
}