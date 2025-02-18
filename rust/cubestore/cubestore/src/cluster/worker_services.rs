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
use tokio::runtime::Runtime;
use tokio::sync::{broadcast, oneshot, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait Callable: Send + Sync + 'static {
    type Request: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type Response: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    async fn call(&self, req: Self::Request) -> Result<Self::Response, CubeError>;
}

#[async_trait]
pub trait Configurator: Send + Sync + 'static {
    type Config: Sync + Send + Clone + 'static;
    type ServicesRequest: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type ServicesResponse: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;

    fn setup(runtime: &Runtime);

    async fn configure(
        services_client: Arc<
            dyn Callable<Request = Self::ServicesRequest, Response = Self::ServicesResponse>,
        >,
    ) -> Result<Self::Config, CubeError>;

    fn teardown();
}

#[async_trait]
pub trait WorkerProcessing: Send + Sync + 'static {
    type Config: Sync + Send + Clone + 'static;
    type Request: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type Response: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;

    fn spawn_background_processes(config: Self::Config) -> Result<(), CubeError>;

    async fn process(
        config: &Self::Config,
        args: Self::Request,
    ) -> Result<Self::Response, CubeError>;

    fn is_single_job_process() -> bool;

    fn process_titile() -> String;
}

pub trait ServicesTransport {
    type Request: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type Response: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type TransportRequest: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type TransportResponse: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;

    type Server: ServicesServer<
        Request = Self::Request,
        Response = Self::Response,
        TransportRequest = Self::TransportRequest,
        TransportResponse = Self::TransportResponse,
    >;
    type Client: ServicesClient<
        Request = Self::Request,
        Response = Self::Response,
        TransportRequest = Self::TransportRequest,
        TransportResponse = Self::TransportResponse,
    >;

    fn start_server(
        reciever: IpcReceiver<Self::TransportRequest>,
        sender: IpcSender<Self::TransportResponse>,
        processor: Arc<dyn Callable<Request = Self::Request, Response = Self::Response>>,
        cancel_token: CancellationToken,
    ) -> Self::Server {
        Self::Server::start(reciever, sender, processor, cancel_token)
    }

    fn connect(
        sender: IpcSender<Self::TransportRequest>,
        reciever: IpcReceiver<Self::TransportResponse>,
        timeout: Duration,
    ) -> Arc<Self::Client> {
        Self::Client::connect(sender, reciever, timeout)
    }
}

#[derive(Debug)]
pub struct DefaultServicesServerProcessor;

impl DefaultServicesServerProcessor {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl Callable for DefaultServicesServerProcessor {
    type Request = ();
    type Response = ();

    async fn call(&self, _request: ()) -> Result<(), CubeError> {
        Ok(())
    }
}

pub struct DefaultServicesTransport<P: Callable> {
    processor: PhantomData<P>,
}

impl<P: Callable> ServicesTransport for DefaultServicesTransport<P> {
    type Request = P::Request;
    type Response = P::Response;
    type TransportRequest = TransportMessage<P::Request>;
    type TransportResponse = TransportMessage<Result<P::Response, CubeError>>;

    type Server = ServicesServerImpl<P>;
    type Client = ServicesClientImpl<P>;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TransportMessage<T: Debug + Sync + Send + 'static> {
    pub message_id: u64,
    pub payload: T,
}

#[async_trait]
pub trait ServicesClient: Callable {
    type TransportRequest: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type TransportResponse: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    fn connect(
        sender: IpcSender<Self::TransportRequest>,
        reciever: IpcReceiver<Self::TransportResponse>,
        timeout: Duration,
    ) -> Arc<Self>;

    fn stop(&self);
}

struct ServicesClientMessage<
    T: Debug + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: Debug + Serialize + DeserializeOwned + Sync + Send + 'static,
> {
    message: T,
    result_sender: oneshot::Sender<Result<R, CubeError>>,
}

pub struct ServicesClientImpl<P: Callable> {
    queue: Arc<unlimited::Queue<ServicesClientMessage<P::Request, P::Response>>>,
    handle: JoinHandle<()>,
    processor: PhantomData<P>,
}

#[async_trait]
impl<P: Callable> ServicesClient for ServicesClientImpl<P> {
    type TransportRequest = TransportMessage<Self::Request>;
    type TransportResponse = TransportMessage<Result<Self::Response, CubeError>>;

    fn connect(
        sender: IpcSender<Self::TransportRequest>,
        reciever: IpcReceiver<Self::TransportResponse>,
        timeout: Duration,
    ) -> Arc<Self> {
        let queue = Arc::new(unlimited::Queue::new());
        let handle = Self::processing_loop(sender, reciever, queue.clone(), timeout);
        Arc::new(Self {
            processor: PhantomData,
            handle,
            queue,
        })
    }

    fn stop(&self) {
        self.handle.abort();
    }
}

#[async_trait]
impl<P: Callable> Callable for ServicesClientImpl<P> {
    type Request = P::Request;
    type Response = P::Response;

    async fn call(&self, request: Self::Request) -> Result<Self::Response, CubeError> {
        let (tx, rx) = oneshot::channel();
        self.queue.push(ServicesClientMessage {
            message: request,
            result_sender: tx,
        });
        rx.await?
    }
}

impl<P: Callable> ServicesClientImpl<P> {
    fn processing_loop(
        sender: IpcSender<<Self as ServicesClient>::TransportRequest>,
        reciever: IpcReceiver<<Self as ServicesClient>::TransportResponse>,
        queue: Arc<
            unlimited::Queue<
                ServicesClientMessage<<Self as Callable>::Request, <Self as Callable>::Response>,
            >,
        >,
        timeout: Duration,
    ) -> JoinHandle<()> {
        let (message_broadcast_tx, _) = broadcast::channel(10000);

        let message_broadcast_tx_to_move = message_broadcast_tx.clone();

        let recieve_loop = cube_ext::spawn_blocking(move || loop {
            let res = reciever.recv();
            match res {
                Ok(TransportMessage {
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

                let ipc_message = TransportMessage {
                    message_id,
                    payload: message,
                };

                let mut broadcast_rx = message_broadcast_tx.subscribe();

                if let Err(e) = sender.send(ipc_message) {
                    log::error!("Error while sending ipc service request: {:?}", e);
                    break;
                }

                cube_ext::spawn(async move {
                    loop {
                        let broadcast_message = tokio::select! {
                            _ = tokio::time::sleep(timeout) => {
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
                                            Some(res)
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

impl<P: Callable> Drop for ServicesClientImpl<P> {
    fn drop(&mut self) {
        self.stop();
    }
}

pub trait ServicesServer {
    type Request: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type Response: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type TransportRequest: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;
    type TransportResponse: Debug + Serialize + DeserializeOwned + Sync + Send + 'static;

    fn start(
        reciever: IpcReceiver<Self::TransportRequest>,
        sender: IpcSender<Self::TransportResponse>,
        processor: Arc<dyn Callable<Request = Self::Request, Response = Self::Response>>,
        cancel_token: CancellationToken,
    ) -> Self;

    fn stop(&self);
}

pub struct ServicesServerImpl<P: Callable> {
    join_handle: JoinHandle<()>,
    processor: PhantomData<P>,
}

impl<P: Callable> ServicesServer for ServicesServerImpl<P> {
    type Request = P::Request;
    type Response = P::Response;
    type TransportRequest = TransportMessage<Self::Request>;
    type TransportResponse = TransportMessage<Result<Self::Response, CubeError>>;

    fn start(
        reciever: IpcReceiver<Self::TransportRequest>,
        sender: IpcSender<Self::TransportResponse>,
        processor: Arc<dyn Callable<Request = Self::Request, Response = Self::Response>>,
        cancel_token: CancellationToken,
    ) -> Self {
        let join_handle = Self::processing_loop(reciever, sender, processor, cancel_token);
        Self {
            join_handle,
            processor: PhantomData,
        }
    }

    fn stop(&self) {
        self.join_handle.abort();
    }
}

impl<P: Callable> ServicesServerImpl<P> {
    fn processing_loop(
        reciever: IpcReceiver<<Self as ServicesServer>::TransportRequest>,
        sender: IpcSender<<Self as ServicesServer>::TransportResponse>,
        processor: Arc<
            dyn Callable<
                Request = <Self as ServicesServer>::Request,
                Response = <Self as ServicesServer>::Response,
            >,
        >,
        cancel_token: CancellationToken,
    ) -> JoinHandle<()> {
        cube_ext::spawn_blocking(move || loop {
            let req = reciever.recv();

            let TransportMessage {
                message_id,
                payload,
            } = match req {
                Ok(message) => message,
                Err(_) => {
                    if !cancel_token.is_cancelled() {
                        cancel_token.cancel();
                    }
                    break;
                }
            };

            let processor_to_move = processor.clone();
            let sender_to_move = sender.clone();

            cube_ext::spawn(async move {
                let res = processor_to_move.call(payload).await;
                match sender_to_move.send(TransportMessage {
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

impl<P: Callable> Drop for ServicesServerImpl<P> {
    fn drop(&mut self) {
        self.stop();
    }
}
