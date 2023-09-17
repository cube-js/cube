use crate::CubeError;
use crate::util::cancellation_token_guard::CancellationGuard;
use async_trait::async_trait;
use datafusion::cube_ext;
use ipc_channel::ipc::{IpcReceiver, IpcSender};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;


#[async_trait]
pub trait WorkerProcessing {
    type Config: Sync + Send + Clone + 'static;
    type Request: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type Response: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type ServicesServer: ServicesServerDef;

    async fn configure() -> Result<Self::Config, CubeError>;

    fn spawn_background_processes(config: Self::Config) -> Result<(), CubeError>;

    async fn process(config: &Self::Config, args: Self::Request) -> Result<Self::Response, CubeError>;

    fn process_titile() -> String;
}


pub trait ServicesServerDef {
    type Request: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type Response: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
}

#[async_trait]
pub trait ServicesServerProcessor {
    type Request: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type Response: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    async fn process(&self, request: Self::Request) -> Self::Response;
}

pub trait WorkerServicesDef {
    type Processor: ServicesServerProcessor + Send + Sync + 'static;
    type Server: ServicesServer<Self::Processor> + Send + Sync + 'static;
}

#[derive(Debug)]
pub struct DefaultWorkerServicesDef;

impl WorkerServicesDef for DefaultWorkerServicesDef {
    type Processor = DefaultServicesServerProcessor;
    type Server = ServicesServerImpl<Self::Processor>;
}


#[derive(Debug)]
pub struct DefaultServicesServerProcessor;

impl DefaultServicesServerProcessor {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl ServicesServerProcessor for DefaultServicesServerProcessor {
    type Request = ();
    type Response = ();
    async fn process(&self, _request: ()) -> () {
        ()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RequestMessage<S: ServicesServerProcessor + Debug + ?Sized> {
    pub message_id: u64,
    pub payload: S::Request,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResponseMessage<S: ServicesServerProcessor + Debug + ?Sized> {
    pub message_id: u64,
    pub payload: S::Response,
}

pub trait ServicesServer<P: ServicesServerProcessor + Send + Sync + 'static> {
    type IpcRequest: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type IpcResponse: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;

    fn start(
        reciever: IpcReceiver<Self::IpcRequest>,
        sender: IpcSender<Self::IpcResponse>,
        processor: Arc<P>,
    ) -> Self;

    fn stop(&self);

}

pub struct ServicesServerImpl<
    P: ServicesServerProcessor + Debug + Send + Sync + 'static,
> {
    join_handle: JoinHandle<()>,
    processor: PhantomData<P>,
}

impl<P: ServicesServerProcessor + Debug + Send + Sync + 'static> ServicesServer<P> for ServicesServerImpl<P> {
    type IpcRequest = RequestMessage<P>;
    type IpcResponse = ResponseMessage<P>;

    fn start(
        reciever: IpcReceiver<Self::IpcRequest>,
        sender: IpcSender<Self::IpcResponse>,
        processor: Arc<P>,
    ) -> Self {
        let join_handle = Self::processing_loop(reciever, sender, processor);
        Self {
            join_handle,
            processor: PhantomData,
        }
    }

    fn stop(&self) {
        self.join_handle.abort();
    }
}

impl<
        P: ServicesServerProcessor + Debug + Send + Sync + 'static,
    > ServicesServerImpl<P>
{

    fn processing_loop(
        reciever: IpcReceiver<RequestMessage<P>>,
        sender: IpcSender<ResponseMessage<P>>,
        processor: Arc<P>,
    ) -> JoinHandle<()> {
        cube_ext::spawn_blocking(move || loop {
            println!("##########");
            let req = reciever.recv();
            println!("req: {:?}", req);
            println!("111111111");

            let RequestMessage {
                message_id,
                payload,
            } = match req {
                Ok(message) => message,
                Err(e) => {
                    log::error!("Error while reading ipc service request: {:?}", e);
                    break;
                }
            };

            let processor_to_move = processor.clone();
            let sender_to_move = sender.clone();

            cube_ext::spawn(async move {
                let res = processor_to_move.process(payload).await;
                match sender_to_move.send(ResponseMessage {
                    message_id,
                    payload: res,
                }) {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("Error while sending IPC response: {:?}", e);
                    }
                }
            });
        })
    }
}

impl<
    P: ServicesServerProcessor + Debug + Send + Sync + 'static,
> Drop for ServicesServerImpl<P> {
    fn drop(&mut self) {
        self.stop();
    }
}
