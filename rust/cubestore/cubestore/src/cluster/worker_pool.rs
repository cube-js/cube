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
use tokio::sync::{oneshot, watch, Mutex, Notify, RwLock};
use tracing::{instrument, Instrument};
use tracing_futures::WithSubscriber;

use crate::config::{env_parse, Config, WorkerServices};
use crate::cluster::worker_services::{
    ServicesClient, ServicesServer, ServicesServerProcessor, WorkerProcessing, WorkerServicesDef,
};
use crate::util::respawn::respawn;
use crate::CubeError;
use datafusion::cube_ext;
use datafusion::cube_ext::catch_unwind::async_try_with_catch_unwind;

pub struct WorkerPool<T: WorkerProcessing + Sync + Send + 'static> {
    queue: Arc<unlimited::Queue<Message<T::Request, T::Response>>>,
    stopped_tx: watch::Sender<bool>,
    workers: Vec<Arc<WorkerProcess<T>>>,
    processor: PhantomData<T>,
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

impl<T: WorkerProcessing + Sync + Send + 'static> WorkerPool<T> {
    pub fn new(
        services_processor: Arc<<T::Services as WorkerServicesDef>::Processor>,
        num: usize,
        timeout: Duration,
        name_prefix: &str,
        envs: Vec<(String, String)>,
    ) -> Self {
        let queue = Arc::new(unlimited::Queue::new());
        let (stopped_tx, stopped_rx) = watch::channel(false);

        let mut workers = Vec::new();

        for i in 1..=num {
            let process = Arc::new(WorkerProcess::<T>::new(
                services_processor.clone(),
                format!("{}{}", name_prefix, i),
                envs.clone(),
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

    pub async fn process(&self, message: T::Request) -> Result<T::Response, CubeError> {
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

pub struct WorkerProcess<T: WorkerProcessing + Sync + Send + 'static> {
    name: String,
    envs: Vec<(String, String)>,
    queue: Arc<unlimited::Queue<Message<T::Request, T::Response>>>,
    timeout: Duration,
    processor: PhantomData<T>,
    stopped_rx: RwLock<watch::Receiver<bool>>,
    finished_notify: Arc<Notify>,
    services_processor: Arc<<T::Services as WorkerServicesDef>::Processor>,
    services_server: Mutex<Option<<T::Services as WorkerServicesDef>::Server>>,
}

impl<T: WorkerProcessing + Sync + Send + 'static> WorkerProcess<T> {
    fn new(
        services_processor: Arc<<T::Services as WorkerServicesDef>::Processor>,
        name: String,
        envs: Vec<(String, String)>,
        queue: Arc<unlimited::Queue<Message<T::Request, T::Response>>>,
        timeout: Duration,
        stopped_rx: watch::Receiver<bool>,
    ) -> Self {
        WorkerProcess {
            services_processor,
            name,
            envs,
            queue,
            timeout,
            stopped_rx: RwLock::new(stopped_rx),
            finished_notify: Arc::new(Notify::new()),
            services_server: Mutex::new(None),
            processor: PhantomData,
        }
    }

    async fn processing_loop(&self) {
        loop {
            let process = self.spawn_process().await;

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
        message: T::Request,
        args_tx: IpcSender<T::Request>,
        res_rx: IpcReceiver<Result<T::Response, CubeError>>,
    ) -> Result<
        (
            T::Response,
            IpcSender<T::Request>,
            IpcReceiver<Result<T::Response, CubeError>>,
        ),
        CubeError,
    > {
        args_tx.send(message)?;
        let (res, res_rx) = cube_ext::spawn_blocking(move || (res_rx.recv(), res_rx)).await?;
        Ok((res??, args_tx, res_rx))
    }

    async fn spawn_process(
        &self,
    ) -> Result<
        (
            IpcSender<T::Request>,
            IpcReceiver<Result<T::Response, CubeError>>,
            Child,
        ),
        CubeError,
    > {
        {
            if let Some(services_server) = self.services_server.lock().await.as_ref() {
                services_server.stop();
            }
        }

        let (args_tx, args_rx) = ipc::channel()?;
        let (res_tx, res_rx) = ipc::channel()?;

        let mut ctx = std::env::var("CUBESTORE_LOG_CONTEXT")
            .ok()
            .unwrap_or("".to_string());
        if !ctx.is_empty() {
            ctx += " ";
        }
        ctx += &self.name;

        let title = T::process_titile();
        let mut envs = vec![("CUBESTORE_LOG_CONTEXT".to_string(), ctx)];
        envs.extend(self.envs.iter().cloned());

        let (service_request_tx, service_request_rx) = ipc::channel()?;
        let (service_response_tx, service_response_rx) = ipc::channel()?;

        let handle = respawn(
            WorkerProcessArgs {
                args: args_rx,
                results: res_tx,
                processor: PhantomData::<T>::default(),
                services_sender: service_request_tx,
                services_reciever: service_response_rx,
            },
            &[title],
            &envs,
        )?;

        *self.services_server.lock().await = Some(ServicesServer::start(
            service_request_rx,
            service_response_tx,
            self.services_processor.clone(),
        ));
        Ok((args_tx, res_rx, handle))
    }
}

#[derive(Serialize, Deserialize)]
pub struct WorkerProcessArgs<T: WorkerProcessing> {
    args: IpcReceiver<T::Request>,
    results: IpcSender<Result<T::Response, CubeError>>,
    processor: PhantomData<T>,
    services_sender: IpcSender<
        <<T::Services as WorkerServicesDef>::Server as ServicesServer<
            <T::Services as WorkerServicesDef>::Processor,
        >>::IpcRequest,
    >,
    services_reciever: IpcReceiver<
        <<T::Services as WorkerServicesDef>::Server as ServicesServer<
            <T::Services as WorkerServicesDef>::Processor,
        >>::IpcResponse,
    >,
}

pub fn worker_main<T>(a: WorkerProcessArgs<T>) -> i32
where
    T: WorkerProcessing + Sync + Send + 'static,
{
    let (rx, tx, services_sender, services_reciever) =
        (a.args, a.results, a.services_sender, a.services_reciever);
    let mut tokio_builder = Builder::new_multi_thread();
    tokio_builder.enable_all();
    tokio_builder.thread_name("cubestore-worker");
    if let Ok(var) = std::env::var("CUBESTORE_EVENT_LOOP_WORKER_THREADS") {
        tokio_builder.worker_threads(var.parse().unwrap());
    }
    let stack_size = env_parse("CUBESTORE_SELECT_WORKER_STACK_SIZE", 4 * 1024 * 1024);
    tokio_builder.thread_stack_size(stack_size);
    let runtime = tokio_builder.build().unwrap();
    worker_setup(&runtime);
    runtime.block_on(async move {
        let services_client =
            <T::Services as WorkerServicesDef>::Client::connect(services_sender, services_reciever);
        let config = match T::configure(services_client).await {
            Err(e) => {
                error!(
                    "Error during {} worker configure: {}",
                    T::process_titile(),
                    e
                );
                return 1;
            }
            Ok(config) => config,
        };

        if let Err(e) = T::spawn_background_processes(config.clone()) {
            error!(
                "Error during {} worker background processes spawn: {}",
                T::process_titile(),
                e
            );
        }

        loop {
            let res = rx.recv();
            match res {
                Ok(args) => {
                    let result = match async_try_with_catch_unwind(T::process(&config, args)).await
                    {
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
    let mut setup = SELECT_WORKER_SETUP.write().unwrap();
    assert!(setup.is_none(), "select worker setup already registered");
    *setup = Some(Box::new(f));
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

    use crate::cluster::worker_pool::{worker_main, WorkerPool};
    use crate::config::Config;
    use crate::queryplanner::serialized_plan::SerializedLogicalPlan;
    use crate::util::respawn;
    use crate::CubeError;
    use datafusion::cube_ext;

    use crate::cluster::worker_services::{
        DefaultServicesServerProcessor, DefaultWorkerServicesDef, ServicesClient,
        ServicesClientImpl, ServicesServerImpl, ServicesServerProcessor, WorkerProcessing,
        WorkerServicesDef,
    };

    type TestPool = WorkerPool<Processor>;

    #[ctor::ctor]
    fn test_support_init() {
        respawn::replace_cmd_args_in_tests();
        respawn::register_handler(worker_main::<Processor>)
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
    impl WorkerProcessing for Processor {
        type Config = Config;
        type Request = Message;
        type Response = Response;
        type Services = DefaultWorkerServicesDef;

        async fn configure(
            _services_client: Arc<<DefaultWorkerServicesDef as WorkerServicesDef>::Client>,
        ) -> Result<Self::Config, CubeError> {
            let config = Config::default();
            config.configure_injector().await;
            Ok(config)
        }

        fn spawn_background_processes(_config: Self::Config) -> Result<(), CubeError> {
            Ok(())
        }
        async fn process(_config: &Config, args: Message) -> Result<Response, CubeError> {
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

        fn process_titile() -> String {
            "--sel-worker".to_string()
        }
    }

    #[test]
    fn test_basic() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();

        runtime.block_on(async move {
            let pool = Arc::new(TestPool::new(
                DefaultServicesServerProcessor::new(),
                4,
                Duration::from_millis(1000),
                "test",
                Vec::new(),
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
            let pool = Arc::new(TestPool::new(
                DefaultServicesServerProcessor::new(),
                4,
                Duration::from_millis(1000),
                "test",
                Vec::new(),
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
            let pool = Arc::new(TestPool::new(
                DefaultServicesServerProcessor::new(),
                4,
                Duration::from_millis(450),
                "test",
                Vec::new(),
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
            let pool = Arc::new(TestPool::new(
                DefaultServicesServerProcessor::new(),
                4,
                Duration::from_millis(2000),
                "test",
                Vec::new(),
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

    type TestServicePool = WorkerPool<ServProcessor>;

    #[derive(Debug)]
    pub struct TestServicesServerProcessor;

    #[async_trait]
    impl ServicesServerProcessor for TestServicesServerProcessor {
        type Request = i64;
        type Response = bool;
        async fn process(&self, request: i64) -> bool {
            request % 2 == 0
        }
    }

    impl TestServicesServerProcessor {
        pub fn new() -> Arc<Self> {
            Arc::new(Self {})
        }
    }

    pub struct TestWorkerServicesDef;

    impl WorkerServicesDef for TestWorkerServicesDef {
        type Processor = TestServicesServerProcessor;
        type Server = ServicesServerImpl<Self::Processor>;
        type Client = ServicesClientImpl<Self::Processor>;
    }

    #[derive(Clone)]
    pub struct TestConfig {
        pub services_client: Arc<<TestWorkerServicesDef as WorkerServicesDef>::Client>,
    }

    #[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
    pub struct TestServReq {
        pub v: i64,
    }

    #[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
    pub enum TestServRes {
        Even(i64),
        Odd(i64),
    }

    pub struct ServProcessor;

    #[async_trait]
    impl WorkerProcessing for ServProcessor {
        type Config = TestConfig;
        type Request = TestServReq;
        type Response = TestServRes;
        type Services = TestWorkerServicesDef;

        async fn configure(
            services_client: Arc<<TestWorkerServicesDef as WorkerServicesDef>::Client>,
        ) -> Result<Self::Config, CubeError> {
            let config = TestConfig { services_client };
            Ok(config)
        }

        fn spawn_background_processes(_config: Self::Config) -> Result<(), CubeError> {
            Ok(())
        }
        async fn process(
            config: &Self::Config,
            args: TestServReq,
        ) -> Result<TestServRes, CubeError> {
            let r = config.services_client.send(args.v.clone()).await.unwrap();

            let res = if r {
                TestServRes::Even(args.v)
            } else {
                TestServRes::Odd(args.v)
            };
            Ok(res)
        }

        fn process_titile() -> String {
            "--sel-worker".to_string()
        }
    }

    #[ctor::ctor]
    fn test_services_support_init() {
        respawn::replace_cmd_args_in_tests();
        respawn::register_handler(worker_main::<ServProcessor>)
    }

    #[test]
    fn test_services_basic() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();

        runtime.block_on(async move {
            let pool = Arc::new(TestServicePool::new(
                TestServicesServerProcessor::new(),
                1,
                Duration::from_millis(1000),
                "test",
                Vec::new(),
            ));
            let pool_to_move = pool.clone();
            cube_ext::spawn(async move { pool_to_move.wait_processing_loops().await });
            assert_eq!(
                pool.process(TestServReq { v: 10 }).await.unwrap(),
                TestServRes::Even(10)
            );
            assert_eq!(
                pool.process(TestServReq { v: 11 }).await.unwrap(),
                TestServRes::Odd(11)
            );
            pool.stop_workers().await.unwrap();
        });
    }
}
