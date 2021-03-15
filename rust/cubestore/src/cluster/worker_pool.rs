use crate::sys::malloc::trim_allocs;
use crate::CubeError;
use async_trait::async_trait;
use deadqueue::unlimited;
use futures::future::join_all;
use ipc_channel::ipc;
use ipc_channel::ipc::{IpcReceiver, IpcSender};
use log::error;
use procspawn::JoinHandle;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::panic;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::oneshot::Sender;
use tokio::sync::{oneshot, watch, Notify, RwLock};

pub struct WorkerPool<
    T: Debug + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: Serialize + DeserializeOwned + Sync + Send + 'static,
    P: MessageProcessor<T, R> + Sync + Send + 'static,
> {
    queue: Arc<unlimited::Queue<Message<T, R>>>,
    stopped_tx: watch::Sender<bool>,
    workers: Vec<Arc<WorkerProcess<T, R, P>>>,
    processor: PhantomData<P>,
}

pub struct Message<
    T: Debug + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: Serialize + DeserializeOwned + Sync + Send + 'static,
> {
    message: T,
    sender: Sender<Result<R, CubeError>>,
}

#[async_trait]
pub trait MessageProcessor<
    T: Debug + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: Serialize + DeserializeOwned + Sync + Send + 'static,
>
{
    async fn process(args: T) -> Result<R, CubeError>;
}

impl<
        T: Debug + Serialize + DeserializeOwned + Sync + Send + 'static,
        R: Serialize + DeserializeOwned + Sync + Send + 'static,
        P: MessageProcessor<T, R> + Sync + Send + 'static,
    > WorkerPool<T, R, P>
{
    pub fn new(num: usize, timeout: Duration) -> WorkerPool<T, R, P> {
        let queue = Arc::new(unlimited::Queue::new());
        let (stopped_tx, stopped_rx) = watch::channel(false);

        let mut workers = Vec::new();

        for _ in 0..num {
            let process = Arc::new(WorkerProcess::<T, R, P>::new(
                queue.clone(),
                timeout.clone(),
                stopped_rx.clone(),
            ));
            workers.push(process.clone());
        }

        WorkerPool {
            stopped_tx,
            queue,
            workers,
            processor: PhantomData,
        }
    }

    pub async fn wait_processing_loops(&self) {
        let futures = self
            .workers
            .iter()
            .map(|w| w.processing_loop())
            .collect::<Vec<_>>();
        join_all(futures).await;
    }

    pub async fn process(&self, message: T) -> Result<R, CubeError> {
        let (tx, rx) = oneshot::channel();
        self.queue.push(Message {
            message,
            sender: tx,
        });
        Ok(rx.await??)
    }

    pub async fn stop_workers(&self) -> Result<(), CubeError> {
        self.stopped_tx.send(true)?;
        Ok(())
    }
}

pub struct WorkerProcess<
    T: Debug + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: Serialize + DeserializeOwned + Sync + Send + 'static,
    P: MessageProcessor<T, R> + Sync + Send + 'static,
> {
    queue: Arc<unlimited::Queue<Message<T, R>>>,
    timeout: Duration,
    processor: PhantomData<P>,
    stopped_rx: RwLock<watch::Receiver<bool>>,
    finished_notify: Arc<Notify>,
}

impl<
        T: Debug + Serialize + DeserializeOwned + Sync + Send + 'static,
        R: Serialize + DeserializeOwned + Sync + Send + 'static,
        P: MessageProcessor<T, R> + Sync + Send + 'static,
    > WorkerProcess<T, R, P>
{
    fn new(
        queue: Arc<unlimited::Queue<Message<T, R>>>,
        timeout: Duration,
        stopped_rx: watch::Receiver<bool>,
    ) -> Self {
        WorkerProcess {
            queue,
            timeout,
            stopped_rx: RwLock::new(stopped_rx),
            finished_notify: Arc::new(Notify::new()),
            processor: PhantomData,
        }
    }

    async fn processing_loop(&self) {
        loop {
            let process = self.spawn_process();

            match process {
                Ok((mut args_tx, mut res_rx, mut handle)) => {
                    scopeguard::defer!(<WorkerProcess<T, R, P>>::kill(&mut handle));
                    loop {
                        let mut stopped_rx = self.stopped_rx.write().await;
                        let Message { message, sender } = tokio::select! {
                            res = stopped_rx.changed() => {
                                if res.is_err() || *stopped_rx.borrow() {
                                    self.finished_notify.notify_waiters();
                                    return;
                                }
                                continue;
                            }
                            message = self.queue.pop() => {
                                message
                            }
                        };
                        let process_message_res_timeout = tokio::time::timeout(
                            self.timeout,
                            self.process_message(message, args_tx, res_rx),
                        )
                        .await;
                        let process_message_res = match process_message_res_timeout {
                            Ok(r) => r,
                            Err(e) => Err(CubeError::internal(format!(
                                "Timed out after waiting for {}",
                                e
                            ))),
                        };
                        match process_message_res {
                            Ok((res, a, r)) => {
                                if sender.send(Ok(res)).is_err() {
                                    error!("Error during worker message processing: Send Error");
                                }
                                args_tx = a;
                                res_rx = r;
                            }
                            Err(e) => {
                                error!("Error during worker message processing: {}", e);
                                if sender.send(Err(e.clone())).is_err() {
                                    error!("Error during worker message processing: Send Error");
                                }
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Can't start process: {}", e);
                }
            }
        }
    }

    fn kill(handle: &mut JoinHandle<()>) {
        if let Err(e) = handle.kill() {
            error!("Error during kill: {:?}", e);
        }
    }

    async fn process_message(
        &self,
        message: T,
        args_tx: IpcSender<T>,
        res_rx: IpcReceiver<Result<R, CubeError>>,
    ) -> Result<(R, IpcSender<T>, IpcReceiver<Result<R, CubeError>>), CubeError> {
        args_tx.send(message)?;
        let (res, res_rx) = tokio::task::spawn_blocking(move || (res_rx.recv(), res_rx)).await?;
        Ok((res??, args_tx, res_rx))
    }

    fn spawn_process(
        &self,
    ) -> Result<
        (
            IpcSender<T>,
            IpcReceiver<Result<R, CubeError>>,
            JoinHandle<()>,
        ),
        CubeError,
    > {
        let (args_tx, args_rx) = ipc::channel()?;
        let (res_tx, res_rx) = ipc::channel()?;

        let handle = procspawn::spawn((args_rx, res_tx), |(rx, tx)| {
            let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
            loop {
                let res = rx.recv();
                match res {
                    Ok(args) => {
                        scopeguard::defer!(trim_allocs());
                        let send_res = tx.send(runtime.block_on(P::process(args)));
                        if let Err(e) = send_res {
                            error!("Worker message send error: {:?}", e);
                            return;
                        }
                    }
                    Err(e) => {
                        error!("Worker message receive error: {:?}", e);
                        return;
                    }
                }
            }
        });
        Ok((args_tx, res_rx, handle))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::cluster::worker_pool::{MessageProcessor, WorkerPool};
    use crate::queryplanner::serialized_plan::SerializedLogicalPlan;
    use crate::CubeError;
    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;
    use datafusion::logical_plan::ToDFSchema;
    use futures_timer::Delay;
    use procspawn::{self};
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use tokio::runtime::Builder;

    // Code from procspawn::enable_test_support!();
    #[procspawn::testsupport::ctor]
    fn __procspawn_test_support_init() {
        // strip the crate name from the module path
        let module_path = std::module_path!().splitn(2, "::").nth(1);
        procspawn::testsupport::enable(module_path);
    }

    #[test]
    fn procspawn_test_helper() {
        procspawn::init();
    }

    #[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
    pub enum Message {
        Delay(u64),
    }

    #[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
    pub enum Response {
        Foo(u64),
    }

    pub struct Processor;

    #[async_trait]
    impl MessageProcessor<Message, Response> for Processor {
        async fn process(args: Message) -> Result<Response, CubeError> {
            match args {
                Message::Delay(x) => {
                    Delay::new(Duration::from_millis(x)).await;
                    Ok(Response::Foo(x))
                }
            }
        }
    }

    #[test]
    fn test_basic() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();

        runtime.block_on(async move {
            let pool = Arc::new(WorkerPool::<Message, Response, Processor>::new(
                4,
                Duration::from_millis(1000),
            ));
            let pool_to_move = pool.clone();
            tokio::spawn(async move { pool_to_move.wait_processing_loops().await });
            assert_eq!(
                pool.process(Message::Delay(100)).await.unwrap(),
                Response::Foo(100)
            );
            pool.stop_workers().await.unwrap();
        });
    }

    #[test]
    fn test_concurrent() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();

        runtime.block_on(async move {
            let pool = Arc::new(WorkerPool::<Message, Response, Processor>::new(
                4,
                Duration::from_millis(1000),
            ));
            let pool_to_move = pool.clone();
            tokio::spawn(async move { pool_to_move.wait_processing_loops().await });
            let mut futures = Vec::new();
            for i in 0..10 {
                futures.push((i, pool.process(Message::Delay(i * 100))));
            }
            for (i, f) in futures {
                println!("Testing {} future", i);
                assert_eq!(f.await.unwrap(), Response::Foo(i * 100));
            }
            pool.stop_workers().await.unwrap();
        });
    }

    #[test]
    fn test_timeout() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();

        runtime.block_on(async move {
            let pool = Arc::new(WorkerPool::<Message, Response, Processor>::new(
                4,
                Duration::from_millis(450),
            ));
            let pool_to_move = pool.clone();
            tokio::spawn(async move { pool_to_move.wait_processing_loops().await });
            let mut futures = Vec::new();
            for i in 0..5 {
                futures.push((i, pool.process(Message::Delay(i * 300))));
            }
            for (i, f) in futures {
                println!("Testing {} future", i);
                if i > 1 {
                    assert_eq!(f.await.is_err(), true);
                } else {
                    assert_eq!(f.await.unwrap(), Response::Foo(i * 300));
                }
            }
            pool.stop_workers().await.unwrap();
        });
    }

    #[tokio::test]
    async fn serialize_plan() -> Result<(), CubeError> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int64, false),
            Field::new("c2", DataType::Utf8, false),
        ]);
        let plan = SerializedLogicalPlan::EmptyRelation {
            produce_one_row: false,
            schema: schema.to_dfschema_ref()?,
        };
        let bytes = bincode::serialize(&plan)?;
        bincode::deserialize::<SerializedLogicalPlan>(bytes.as_slice())?;
        Ok(())
    }
}
