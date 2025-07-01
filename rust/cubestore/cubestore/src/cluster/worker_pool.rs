use std::fmt::Debug;
use std::marker::PhantomData;
use std::panic;
use std::process::{Child, ExitStatus};
use std::sync::Arc;
use std::time::Duration;

use crate::util::cancellation_token_guard::CancellationGuard;
use deadqueue::unlimited;
use futures::future::join_all;
use ipc_channel::ipc;
use ipc_channel::ipc::{IpcReceiver, IpcSender};
use log::error;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::runtime::Builder;
use tokio::sync::oneshot::Sender;
use tokio::sync::{oneshot, watch, Mutex, Notify, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Instrument};
use tracing_futures::WithSubscriber;

use crate::cluster::worker_services::{
    Callable, Configurator, ServicesServer, ServicesTransport, WorkerProcessing,
};
use crate::config::env_parse;
use crate::util::respawn::respawn;
use crate::CubeError;
use datafusion::cube_ext;
use datafusion::cube_ext::catch_unwind::async_try_with_catch_unwind;

pub struct WorkerPool<C: Configurator, P: WorkerProcessing, S: ServicesTransport> {
    queue: Arc<unlimited::Queue<Message<P::Request, P::Response>>>,
    stopped_tx: watch::Sender<bool>,
    workers: Vec<Arc<WorkerProcess<C, P, S>>>,
    configurator: PhantomData<C>,
    processor: PhantomData<P>,
    services_transport: PhantomData<S>,
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

impl<
        C: Configurator,
        P: WorkerProcessing<Config = C::Config>,
        S: ServicesTransport<Request = C::ServicesRequest, Response = C::ServicesResponse>,
    > WorkerPool<C, P, S>
{
    pub fn new(
        services_processor: Arc<dyn Callable<Request = S::Request, Response = S::Response>>,
        num: usize,
        timeout: Duration,
        idle_timeout: Duration,
        name_prefix: &str,
        envs: Vec<(String, String)>,
    ) -> Self {
        let queue = Arc::new(unlimited::Queue::new());
        let (stopped_tx, stopped_rx) = watch::channel(false);

        let mut workers = Vec::new();

        for i in 1..=num {
            let process = Arc::new(WorkerProcess::<C, P, S>::new(
                services_processor.clone(),
                format!("{}{}", name_prefix, i),
                envs.clone(),
                queue.clone(),
                timeout.clone(),
                idle_timeout.clone(),
                stopped_rx.clone(),
            ));
            workers.push(process.clone());
        }

        WorkerPool {
            stopped_tx,
            queue,
            workers,
            processor: PhantomData,
            configurator: PhantomData,
            services_transport: PhantomData,
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

    pub async fn process(&self, message: P::Request) -> Result<P::Response, CubeError> {
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
    #[allow(dead_code)]
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

pub struct WorkerProcess<C: Configurator, P: WorkerProcessing, S: ServicesTransport> {
    name: String,
    envs: Vec<(String, String)>,
    queue: Arc<unlimited::Queue<Message<P::Request, P::Response>>>,
    timeout: Duration,
    idle_timeout: Duration,
    processor: PhantomData<(C, P, S)>,
    stopped_rx: RwLock<watch::Receiver<bool>>,
    finished_notify: Arc<Notify>,
    services_processor: Arc<dyn Callable<Request = S::Request, Response = S::Response>>,
    services_server: Mutex<Option<S::Server>>,
}

impl<C: Configurator, P: WorkerProcessing, S: ServicesTransport> WorkerProcess<C, P, S> {
    fn new(
        services_processor: Arc<dyn Callable<Request = S::Request, Response = S::Response>>,
        name: String,
        envs: Vec<(String, String)>,
        queue: Arc<unlimited::Queue<Message<P::Request, P::Response>>>,
        timeout: Duration,
        idle_timeout: Duration,
        stopped_rx: watch::Receiver<bool>,
    ) -> Self {
        WorkerProcess {
            services_processor,
            name,
            envs,
            queue,
            timeout,
            idle_timeout,
            stopped_rx: RwLock::new(stopped_rx),
            finished_notify: Arc::new(Notify::new()),
            services_server: Mutex::new(None),
            processor: PhantomData,
        }
    }

    async fn processing_loop(&self) {
        loop {
            let mut handle_guard: Option<ProcessHandleGuard> = None;
            let mut cancel_token: Option<CancellationToken> = None;
            let mut _cancel_token_guard: Option<CancellationGuard> = None;
            let mut args_channel = None;

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
                    _ = tokio::time::sleep(self.idle_timeout), if handle_guard.is_some() => {
                        break;
                    }
                    _ = async { if let Some(ct) =  cancel_token.as_ref() { ct.cancelled().await } }, if cancel_token.is_some()  => {
                        break;
                    }
                };

                let can_use_existing_process = !P::is_single_job_process()
                    && args_channel.is_some()
                    && handle_guard.is_some()
                    && handle_guard.as_mut().unwrap().is_alive();

                let (args_tx, res_rx) = if can_use_existing_process {
                    args_channel.unwrap()
                } else {
                    let process = self.spawn_process().await;
                    match process {
                        Ok((args_tx, res_rx, handle, c_t)) => {
                            handle_guard = Some(ProcessHandleGuard::new(handle));
                            _cancel_token_guard = Some(CancellationGuard::new(c_t.clone()));
                            cancel_token = Some(c_t);
                            (args_tx, res_rx)
                        }
                        Err(e) => {
                            error!("Can't start process: {}", e);
                            if sender
                                .send(Err(CubeError::internal(format!(
                                    "Error during spawn worker pool process: {}",
                                    e
                                ))))
                                .is_err()
                            {
                                error!("Error during worker message processing: Send Error");
                            }
                            break;
                        }
                    }
                };

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
                        args_channel = Some((a, r));
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

                if P::is_single_job_process() {
                    break;
                }
            }
        }
    }

    #[instrument(level = "trace", skip(self, message, args_tx, res_rx))]
    async fn process_message(
        &self,
        message: P::Request,
        args_tx: IpcSender<P::Request>,
        res_rx: IpcReceiver<Result<P::Response, CubeError>>,
    ) -> Result<
        (
            P::Response,
            IpcSender<P::Request>,
            IpcReceiver<Result<P::Response, CubeError>>,
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
            IpcSender<P::Request>,
            IpcReceiver<Result<P::Response, CubeError>>,
            Child,
            CancellationToken,
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

        let title = P::process_titile();
        let mut envs = vec![("CUBESTORE_LOG_CONTEXT".to_string(), ctx)];
        envs.extend(self.envs.iter().cloned());

        let (service_request_tx, service_request_rx) = ipc::channel()?;
        let (service_response_tx, service_response_rx) = ipc::channel()?;

        let handle = respawn(
            WorkerProcessArgs {
                args: args_rx,
                results: res_tx,
                processor: PhantomData::<(C, P, S)>::default(),
                services_sender: service_request_tx,
                services_reciever: service_response_rx,
                timeout: self.timeout.clone(),
            },
            &[title],
            &envs,
        )?;

        let cancel_token = CancellationToken::new();
        *self.services_server.lock().await = Some(S::start_server(
            service_request_rx,
            service_response_tx,
            self.services_processor.clone(),
            cancel_token.clone(),
        ));
        Ok((args_tx, res_rx, handle, cancel_token))
    }
}

#[derive(Serialize, Deserialize)]
pub struct WorkerProcessArgs<C: Configurator, P: WorkerProcessing, S: ServicesTransport> {
    args: IpcReceiver<P::Request>,
    results: IpcSender<Result<P::Response, CubeError>>,
    processor: PhantomData<(C, P, S)>,
    services_sender: IpcSender<S::TransportRequest>,
    services_reciever: IpcReceiver<S::TransportResponse>,
    timeout: Duration,
}

struct TeardownGuard<C: Configurator>(PhantomData<C>);

impl<C: Configurator> Drop for TeardownGuard<C> {
    fn drop(&mut self) {
        C::teardown();
    }
}

pub fn worker_main<C, P, S>(a: WorkerProcessArgs<C, P, S>) -> i32
where
    C: Configurator,
    P: WorkerProcessing<Config = C::Config>,
    S: ServicesTransport<Request = C::ServicesRequest, Response = C::ServicesResponse>,
{
    let (rx, tx, services_sender, services_reciever, timeout) = (
        a.args,
        a.results,
        a.services_sender,
        a.services_reciever,
        a.timeout,
    );
    let mut tokio_builder = Builder::new_multi_thread();
    tokio_builder.enable_all();
    tokio_builder.thread_name("cubestore-worker");
    if let Ok(var) = std::env::var("CUBESTORE_EVENT_LOOP_WORKER_THREADS") {
        tokio_builder.worker_threads(var.parse().unwrap());
    }
    let stack_size = env_parse("CUBESTORE_SELECT_WORKER_STACK_SIZE", 4 * 1024 * 1024);
    tokio_builder.thread_stack_size(stack_size);
    let runtime = tokio_builder.build().unwrap();
    C::setup(&runtime);
    let _teardown_guard = TeardownGuard::<C>(PhantomData);
    runtime.block_on(async move {
        let services_client = S::connect(services_sender, services_reciever, timeout);
        let config = match C::configure(services_client).await {
            Err(e) => {
                error!(
                    "Error during {} worker configure: {}",
                    P::process_titile(),
                    e
                );
                return 1;
            }
            Ok(config) => config,
        };

        if let Err(e) = P::spawn_background_processes(config.clone()) {
            error!(
                "Error during {} worker background processes spawn: {}",
                P::process_titile(),
                e
            );
        }

        loop {
            let res = rx.recv();
            match res {
                Ok(args) => {
                    let result = match async_try_with_catch_unwind(P::process(&config, args)).await
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_plan::ToDFSchema;
    use futures_timer::Delay;
    use serde::{Deserialize, Serialize};
    use tokio::runtime::{Builder, Runtime};

    use crate::cluster::worker_pool::{worker_main, WorkerPool};
    use crate::config::Config;
    use crate::queryplanner::serialized_plan::SerializedLogicalPlan;
    use crate::util::respawn;
    use crate::CubeError;
    use datafusion::cube_ext;

    use crate::cluster::worker_services::{
        Callable, Configurator, DefaultServicesServerProcessor, DefaultServicesTransport,
        WorkerProcessing,
    };

    type TestPool = WorkerPool<TestConfigurator, Processor, Transport>;

    #[ctor::ctor]
    fn test_support_init() {
        respawn::replace_cmd_args_in_tests();
        respawn::register_handler(worker_main::<TestConfigurator, Processor, Transport>)
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

    pub struct TestConfigurator;

    #[async_trait]
    impl Configurator for TestConfigurator {
        type Config = Config;
        type ServicesRequest = ();
        type ServicesResponse = ();
        fn setup(_runtime: &Runtime) {}
        async fn configure(
            _services_client: Arc<dyn Callable<Request = (), Response = ()>>,
        ) -> Result<Self::Config, CubeError> {
            let config = Config::default();
            config.configure_injector().await;
            Ok(config)
        }

        fn teardown() {}
    }

    pub struct Processor;

    #[async_trait]
    impl WorkerProcessing for Processor {
        type Config = Config;
        type Request = Message;
        type Response = Response;

        fn spawn_background_processes(_config: Self::Config) -> Result<(), CubeError> {
            Ok(())
        }

        fn is_single_job_process() -> bool {
            false
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

    type Transport = DefaultServicesTransport<DefaultServicesServerProcessor>;

    #[test]
    fn test_basic() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();

        runtime.block_on(async move {
            let pool = Arc::new(TestPool::new(
                DefaultServicesServerProcessor::new(),
                4,
                Duration::from_millis(1000),
                Duration::from_secs(600),
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
                Duration::from_secs(60),
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
                Duration::from_secs(60),
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
                Duration::from_secs(60),
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

    type TestServicePool = WorkerPool<ServConfigurator, ServProcessor, ServTransport>;

    #[derive(Debug)]
    pub struct TestServicesServerProcessor;

    #[async_trait]
    impl Callable for TestServicesServerProcessor {
        type Request = i64;
        type Response = bool;

        async fn call(&self, request: i64) -> Result<bool, CubeError> {
            Ok(request % 2 == 0)
        }
    }

    impl TestServicesServerProcessor {
        pub fn new() -> Arc<Self> {
            Arc::new(Self {})
        }
    }

    #[derive(Clone)]
    pub struct TestConfig {
        pub services_client: Arc<dyn Callable<Request = i64, Response = bool>>,
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

    pub struct ServConfigurator;

    #[async_trait]
    impl Configurator for ServConfigurator {
        type Config = TestConfig;
        type ServicesRequest = i64;
        type ServicesResponse = bool;

        fn setup(_runtime: &Runtime) {}

        async fn configure(
            services_client: Arc<dyn Callable<Request = i64, Response = bool>>,
        ) -> Result<Self::Config, CubeError> {
            let config = TestConfig { services_client };
            Ok(config)
        }

        fn teardown() {}
    }

    pub struct ServProcessor;

    #[async_trait]
    impl WorkerProcessing for ServProcessor {
        type Config = TestConfig;
        type Request = TestServReq;
        type Response = TestServRes;

        fn spawn_background_processes(_config: Self::Config) -> Result<(), CubeError> {
            Ok(())
        }

        fn is_single_job_process() -> bool {
            false
        }
        async fn process(
            config: &Self::Config,
            args: TestServReq,
        ) -> Result<TestServRes, CubeError> {
            let r = config.services_client.call(args.v.clone()).await.unwrap();

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

    type ServTransport = DefaultServicesTransport<TestServicesServerProcessor>;

    #[ctor::ctor]
    fn test_services_support_init() {
        respawn::replace_cmd_args_in_tests();
        respawn::register_handler(worker_main::<ServConfigurator, ServProcessor, ServTransport>)
    }

    #[test]
    fn test_services_basic() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();

        runtime.block_on(async move {
            let pool = Arc::new(TestServicePool::new(
                TestServicesServerProcessor::new(),
                1,
                Duration::from_millis(1000),
                Duration::from_secs(60),
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
