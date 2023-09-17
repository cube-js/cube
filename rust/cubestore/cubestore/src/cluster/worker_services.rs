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

pub trait ServicesServerDef {
    type Request: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type Response: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DefaultServicesServerDef;

impl ServicesServerDef for DefaultServicesServerDef {
    type Request = ();
    type Response = ();
}

#[async_trait]
pub trait ServicesServerProcessor<S: ServicesServerDef> {
    type Request: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type Response: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    async fn process(&self, request: S::Request) -> S::Response;
}

pub struct DefaultServicesServerProcessor;

impl DefaultServicesServerProcessor {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl ServicesServerProcessor<DefaultServicesServerDef> for DefaultServicesServerProcessor {
    type Request = ();
    type Response = ();
    async fn process(&self, _request: ()) -> () {
        ()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RequestMessage<S: ServicesServerDef + ?Sized> {
    pub message_id: u64,
    pub payload: S::Request,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResponseMessage<S: ServicesServerDef + ?Sized> {
    pub message_id: u64,
    pub payload: S::Response,
}

pub struct ServicesServer<
    S: ServicesServerDef + Debug + Send + Sync + 'static,
    P: ServicesServerProcessor<S> + Send + Sync + 'static,
> {
    join_handle: JoinHandle<()>,
    server_def: PhantomData<S>,
    processor: PhantomData<P>,
}

impl<
        S: ServicesServerDef + Debug + Send + Sync + 'static,
        P: ServicesServerProcessor<S> + Send + Sync + 'static,
    > ServicesServer<S, P>
{
    pub fn start(
        reciever: IpcReceiver<RequestMessage<S>>,
        sender: IpcSender<ResponseMessage<S>>,
        processor: Arc<P>,
    ) -> Self {
        let join_handle = Self::processing_loop(reciever, sender, processor);
        Self {
            join_handle,
            server_def: PhantomData,
            processor: PhantomData,
        }
    }

    pub fn stop(&self) {
        self.join_handle.abort();
    }

    fn processing_loop(
        reciever: IpcReceiver<RequestMessage<S>>,
        sender: IpcSender<ResponseMessage<S>>,
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
    S: ServicesServerDef + Debug + Send + Sync + 'static,
    P: ServicesServerProcessor<S> + Send + Sync + 'static,
> Drop for ServicesServer<S, P> {
    fn drop(&mut self) {
        self.stop();
    }
}
