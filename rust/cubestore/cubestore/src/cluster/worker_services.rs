use crate::util::cancellation_token_guard::CancellationGuard;
use crate::CubeError;
use async_trait::async_trait;
use datafusion::cube_ext;
use deadqueue::unlimited;
use ipc_channel::ipc::{IpcReceiver, IpcSender};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, oneshot, Notify, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait WorkerProcessing {
    type Config: Sync + Send + Clone + 'static;
    type Request: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type Response: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type Services: WorkerServicesDef;

    async fn configure(
        services_client: Arc<<Self::Services as WorkerServicesDef>::Client>,
    ) -> Result<Self::Config, CubeError>;

    fn spawn_background_processes(config: Self::Config) -> Result<(), CubeError>;

    async fn process(
        config: &Self::Config,
        args: Self::Request,
    ) -> Result<Self::Response, CubeError>;

    fn process_titile() -> String;
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
    type Client: ServicesClient<Self::Processor, Self::Server> + Send + Sync + 'static;
}

#[derive(Debug)]
pub struct DefaultWorkerServicesDef;

impl WorkerServicesDef for DefaultWorkerServicesDef {
    type Processor = DefaultServicesServerProcessor;
    type Server = ServicesServerImpl<Self::Processor>;
    type Client = ServicesClientImpl<Self::Processor>;
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

pub struct ServicesServerImpl<P: ServicesServerProcessor + Debug + Send + Sync + 'static> {
    join_handle: JoinHandle<()>,
    processor: PhantomData<P>,
}

impl<P: ServicesServerProcessor + Debug + Send + Sync + 'static> ServicesServer<P>
    for ServicesServerImpl<P>
{
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

impl<P: ServicesServerProcessor + Debug + Send + Sync + 'static> ServicesServerImpl<P> {
    fn processing_loop(
        reciever: IpcReceiver<RequestMessage<P>>,
        sender: IpcSender<ResponseMessage<P>>,
        processor: Arc<P>,
    ) -> JoinHandle<()> {
        cube_ext::spawn_blocking(move || loop {
            let req = reciever.recv();

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

impl<P: ServicesServerProcessor + Debug + Send + Sync + 'static> Drop for ServicesServerImpl<P> {
    fn drop(&mut self) {
        self.stop();
    }
}

#[async_trait]
pub trait ServicesClient<
    P: ServicesServerProcessor + Send + Sync + 'static,
    S: ServicesServer<P> + Send + Sync + 'static,
>
{
    fn connect(
        sender: IpcSender<S::IpcRequest>,
        reciever: IpcReceiver<S::IpcResponse>,
    ) -> Arc<Self>;
    async fn send(&self, request: P::Request) -> Result<P::Response, CubeError>;
    fn stop(&self);
}

struct ServicesClientMessage<
    T: Debug + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: Serialize + DeserializeOwned + Sync + Send + 'static,
> {
    message: T,
    result_sender: oneshot::Sender<Result<R, CubeError>>,
}

pub struct ServicesClientImpl<P: ServicesServerProcessor + Debug + Send + Sync + 'static> {
    queue: Arc<unlimited::Queue<ServicesClientMessage<P::Request, P::Response>>>,
    handle: JoinHandle<()>,
    processor: PhantomData<P>,
}

#[async_trait]
impl<P: ServicesServerProcessor + Debug + Send + Sync + 'static>
    ServicesClient<P, ServicesServerImpl<P>> for ServicesClientImpl<P>
{
    fn connect(
        sender: IpcSender<RequestMessage<P>>,
        reciever: IpcReceiver<ResponseMessage<P>>,
    ) -> Arc<Self> {
        let queue = Arc::new(unlimited::Queue::new());
        let handle = Self::processing_loop(sender, reciever, queue.clone());
        Arc::new(Self {
            processor: PhantomData,
            handle,
            queue,
        })
    }
    async fn send(&self, request: P::Request) -> Result<P::Response, CubeError> {
        let (tx, rx) = oneshot::channel();
        self.queue.push(ServicesClientMessage {
            message: request,
            result_sender: tx,
        });
        rx.await?
    }
    fn stop(&self) {
        self.handle.abort();
    }
}

impl<P: ServicesServerProcessor + Debug + Send + Sync + 'static> ServicesClientImpl<P> {
    fn processing_loop(
        sender: IpcSender<RequestMessage<P>>,
        reciever: IpcReceiver<ResponseMessage<P>>,
        queue: Arc<unlimited::Queue<ServicesClientMessage<P::Request, P::Response>>>,
    ) -> JoinHandle<()> {
        let (message_broadcast_tx, _) = broadcast::channel(10000);

        let message_broadcast_tx_to_move = message_broadcast_tx.clone();

        let recieve_loop = cube_ext::spawn_blocking(move || loop {
            let res = reciever.recv();
            match res {
                Ok(ResponseMessage {
                    message_id,
                    payload,
                }) => {
                    if let Err(e) = message_broadcast_tx_to_move
                        .send((message_id, Arc::new(RwLock::new(Some(payload)))))
                    {
                        log::error!(
                            "Worker broadcasting processed message id {}: {}",
                            message_id,
                            e
                        );
                    }
                }
                Err(e) => {
                    log::error!("Error while reading ipc service response: {:?}", e);
                    break;
                }
            }
        });

        cube_ext::spawn(async move {
            let mut id_counter = 0;
            loop {
                let ServicesClientMessage {
                    message,
                    result_sender,
                } = queue.pop().await;

                let message_id = id_counter;
                id_counter += 1;

                let ipc_message = RequestMessage {
                    message_id,
                    payload: message,
                };

                if let Err(e) = sender.send(ipc_message) {
                    log::error!("Error while sending ipc service request: {:?}", e);
                    break;
                }

                let mut broadcast_rx = message_broadcast_tx.subscribe();

                cube_ext::spawn(async move {
                    loop {
                        let broadcast_message = tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(5)) => { //TODO! config
                                Err(CubeError::internal(format!(
                                    "Worker service timeout for message id: {}",
                                    message_id
                                )))
                            }
                            msg = broadcast_rx.recv() => {
                                Ok(msg)
                            }
                        };

                        let res = match broadcast_message {
                            Ok(r) => match r {
                                Ok((id, res)) => {
                                    if id == message_id {
                                        let mut option = res.write().await;
                                        if let Some(res) = option.take() {
                                            Some(Ok(res))
                                        } else {
                                            Some(Err(CubeError::internal(format!(
                                                "Worker service result consumed by another listener for message id {}",
                                                message_id
                                            ))))
                                        }
                                    } else {
                                        None
                                    }
                                }
                                Err(e) => Some(Err(CubeError::internal(format!(
                                    "Worker service processing error for message id {}: {}",
                                    message_id, e
                                )))),
                            },
                            Err(e) => {
                                log::error!("Worker service read from broadcast error for message id {}: {}", message_id, e);
                                Some(Err(CubeError::internal(format!(
                                    "Worker service read from broadcast error for message id {}: {}",
                                    e,
                                    message_id
                                ))))
                            }
                        };

                        if let Some(res) = res {
                            if let Err(_) = result_sender.send(res) {
                                log::error!(
                                    "Worker service send result error for message id {}",
                                    message_id
                                );
                            }
                            break;
                        }
                    }
                });
            }
        });

        recieve_loop
    }
}

impl<P: ServicesServerProcessor + Debug + Send + Sync + 'static> Drop for ServicesClientImpl<P> {
    fn drop(&mut self) {
        self.stop();
    }
}
