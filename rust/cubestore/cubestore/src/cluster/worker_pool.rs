use std::fmt::Debug;
use std::marker::PhantomData;
use std::panic;
use std::process::{Child, ExitStatus};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use deadqueue::unlimited;
use futures::future::join_all;
use ipc_channel::ipc;
use ipc_channel::ipc::{IpcReceiver, IpcSender};
use log::error;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::oneshot::Sender;
use tokio::sync::{oneshot, watch, Notify, RwLock};
use tracing::{instrument, Instrument};
use tracing_futures::WithSubscriber;

use crate::config::{Config, WorkerServices};
use crate::util::respawn::respawn;
use crate::CubeError;
use datafusion::cube_ext;
use datafusion::cube_ext::catch_unwind::async_try_with_catch_unwind;

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
    span: tracing::Span,
    dispatcher: tracing::dispatcher::Dispatch,
}

#[async_trait]
pub trait MessageProcessor<
    T: Debug + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: Serialize + DeserializeOwned + Sync + Send + 'static,
>
{
    async fn process(services: &WorkerServices, args: T) -> Result<R, CubeError>;
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

        for i in 1..=num {
            let process = Arc::new(WorkerProcess::<T, R, P>::new(
                format!("sel{}", i),
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
            span: tracing::Span::current(),
            dispatcher: tracing::dispatcher::get_default(|d| d.clone()),
        });
        Ok(rx.await??)
    }

    pub async fn stop_workers(&self) -> Result<(), CubeError> {
        self.stopped_tx.send(true)?;
        Ok(())
    }
}

struct ProcessHandleGuard {
    handle: Child,
}

impl ProcessHandleGuard {
    pub fn new(handle: Child) -> Self {
        Self { handle }
    }
    pub fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        self.handle.try_wait()
    }
    pub fn is_alive(&mut self) -> bool {
        self.handle.try_wait().map_or(false, |r| r.is_none())
    }
    pub fn kill(&mut self) {
        if let Err(e) = self.handle.kill() {
            error!("Error during kill: {:?}", e);
        }
    }
}

impl Drop for ProcessHandleGuard {
    fn drop(&mut self) {
        if self.is_alive() {
            self.kill();
        }
    }
}

pub struct WorkerProcess<
    T: Debug + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: Serialize + DeserializeOwned + Sync + Send + 'static,
    P: MessageProcessor<T, R> + Sync + Send + 'static,
> {
    name: String,
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
        name: String,
        queue: Arc<unlimited::Queue<Message<T, R>>>,
        timeout: Duration,
        stopped_rx: watch::Receiver<bool>,
    ) -> Self {
        WorkerProcess {
            name,
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
                Ok((mut args_tx, mut res_rx, handle)) => {
                    let mut handle_guard = ProcessHandleGuard::new(handle);
                    loop {
                        let mut stopped_rx = self.stopped_rx.write().await;
                        let Message {
                            message,
                            sender,
                            span,
                            dispatcher,
                        } = tokio::select! {
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
                        //Check if child process is killed
                        match handle_guard.try_wait() {
                            Ok(Some(_)) => {
                                error!(
                                    "Worker process is killed, reshedule message in another process"
                                    );
                                self.queue.push(Message {
                                    message,
                                    sender,
                                    span,
                                    dispatcher,
                                });
                                break;
                            }
                            Ok(None) => {}
                            Err(_) => {
                                error!(
                                    "Can't read worker process status, reshedule message in another process"
                                    );
                                self.queue.push(Message {
                                    message,
                                    sender,
                                    span,
                                    dispatcher,
                                });
                                break;
                            }
                        }

                        let process_message_res_timeout = tokio::time::timeout(
                            self.timeout,
                            self.process_message(message, args_tx, res_rx),
                        )
                        .instrument(span)
                        .with_subscriber(dispatcher)
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
                                error!(
                                    "Error during worker message processing: {}",
                                    e.display_with_backtrace()
                                );
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

    #[instrument(level = "trace", skip(self, message, args_tx, res_rx))]
    async fn process_message(
        &self,
        message: T,
        args_tx: IpcSender<T>,
        res_rx: IpcReceiver<Result<R, CubeError>>,
    ) -> Result<(R, IpcSender<T>, IpcReceiver<Result<R, CubeError>>), CubeError> {
        args_tx.send(message)?;
        let (res, res_rx) = cube_ext::spawn_blocking(move || (res_rx.recv(), res_rx)).await?;
        Ok((res??, args_tx, res_rx))
    }

    fn spawn_process(
        &self,
    ) -> Result<(IpcSender<T>, IpcReceiver<Result<R, CubeError>>, Child), CubeError> {
        let (args_tx, args_rx) = ipc::channel()?;
        let (res_tx, res_rx) = ipc::channel()?;

        let mut ctx = std::env::var("CUBESTORE_LOG_CONTEXT")
            .ok()
            .unwrap_or("".to_string());
        if !ctx.is_empty() {
            ctx += " ";
        }
        ctx += &self.name;

        let handle = respawn(
            WorkerProcessArgs {
                args: args_rx,
                results: res_tx,
                processor: PhantomData::<P>::default(),
            },
            &["--sel-worker".to_string()],
            &[("CUBESTORE_LOG_CONTEXT".to_string(), ctx)],
        )?;
        Ok((args_tx, res_rx, handle))
    }
}

#[derive(Serialize, Deserialize)]
pub struct WorkerProcessArgs<T, R, P: ?Sized> {
    args: IpcReceiver<T>,
    results: IpcSender<Result<R, CubeError>>,
    processor: PhantomData<P>,
}

pub fn worker_main<T, R, P>(a: WorkerProcessArgs<T, R, P>) -> i32
where
    T: Debug + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: Serialize + DeserializeOwned + Sync + Send + 'static,
    P: MessageProcessor<T, R>,
{
    let (rx, tx) = (a.args, a.results);
    let mut tokio_builder = Builder::new_multi_thread();
    tokio_builder.enable_all();
    tokio_builder.thread_name("cubestore-worker");
    if let Ok(var) = std::env::var("CUBESTORE_EVENT_LOOP_WORKER_THREADS") {
        tokio_builder.worker_threads(var.parse().unwrap());
    }
    let runtime = tokio_builder.build().unwrap();
    worker_setup(&runtime);
    runtime.block_on(async move {
        let config = Config::default();
        config.configure_injector().await;
        let services = config.worker_services().await;

        loop {
            let res = rx.recv();
            match res {
                Ok(args) => {
                    let result =
                        match async_try_with_catch_unwind(P::process(&services, args)).await {
                            Ok(result) => result,
                            Err(panic) => Err(CubeError::from(panic)),
                        };
                    let send_res = tx.send(result);
                    if let Err(e) = send_res {
                        error!("Worker message send error: {:?}", e);
                        return 0;
                    }
                }
                Err(e) => {
                    error!("Worker message receive error: {:?}", e);
                    return 0;
                }
            }
        }
    })
}

fn worker_setup(runtime: &Runtime) {
    let startup = SELECT_WORKER_SETUP.read().unwrap();
    if startup.is_some() {
        startup.as_ref().unwrap()(runtime);
    }
}

lazy_static! {
    static ref SELECT_WORKER_SETUP: std::sync::RwLock<Option<Box<dyn Fn(&Runtime) + Send + Sync>>> =
        std::sync::RwLock::new(None);
}

pub fn register_select_worker_setup(f: fn(&Runtime)) {
    let mut startup = SELECT_WORKER_SETUP.write().unwrap();
    assert!(startup.is_none(), "select worker setup already registered");
    *startup = Some(Box::new(f));
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;
    use datafusion::logical_plan::ToDFSchema;
    use futures_timer::Delay;
    use serde::{Deserialize, Serialize};
    use tokio::runtime::Builder;

    use crate::cluster::worker_pool::{worker_main, MessageProcessor, WorkerPool};
    use crate::config::WorkerServices;
    use crate::queryplanner::serialized_plan::SerializedLogicalPlan;
    use crate::util::respawn;
    use crate::CubeError;
    use datafusion::cube_ext;

    #[ctor::ctor]
    fn test_support_init() {
        respawn::replace_cmd_args_in_tests();
        respawn::register_handler(worker_main::<Message, Response, Processor>)
    }

    #[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
    pub enum Message {
        Delay(u64),
        Panic,
    }

    #[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
    pub enum Response {
        Foo(u64),
    }

    pub struct Processor;

    #[async_trait]
    impl MessageProcessor<Message, Response> for Processor {
        async fn process(_services: &WorkerServices, args: Message) -> Result<Response, CubeError> {
            match args {
                Message::Delay(x) => {
                    Delay::new(Duration::from_millis(x)).await;
                    Ok(Response::Foo(x))
                }
                Message::Panic => {
                    panic!("oops")
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
            cube_ext::spawn(async move { pool_to_move.wait_processing_loops().await });
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
            cube_ext::spawn(async move { pool_to_move.wait_processing_loops().await });
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
            cube_ext::spawn(async move { pool_to_move.wait_processing_loops().await });
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

    #[test]
    fn test_panic() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();

        runtime.block_on(async move {
            let pool = Arc::new(WorkerPool::<Message, Response, Processor>::new(
                4,
                Duration::from_millis(2000),
            ));
            let pool_to_move = pool.clone();
            cube_ext::spawn(async move { pool_to_move.wait_processing_loops().await });
            assert_eq!(
                pool.process(Message::Panic).await,
                Err(CubeError::panic("oops".to_string()))
            );
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
