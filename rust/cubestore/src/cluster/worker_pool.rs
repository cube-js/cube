use procspawn::JoinHandle;
use deadqueue::unlimited;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tokio::sync::oneshot::Sender;
use tokio::sync::{oneshot, watch, RwLock, Notify};
use std::marker::PhantomData;
use crate::CubeError;
use ipc_channel::ipc;
use ipc_channel::ipc::{IpcSender, IpcReceiver};
use log::{error};
use std::time::Duration;
use std::panic;
use std::fmt::Debug;
use serde::{Serialize};

pub struct WorkerPool<
    T: Debug + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: Serialize + DeserializeOwned + Sync + Send + 'static,
    P: MessageProcessor<T, R> + Sync + Send + 'static
> {
    // TODO stop implementation
    workers: Vec<Arc<WorkerProcess<T, R, P>>>,
    queue: Arc<unlimited::Queue<Message<T, R>>>,
    stopped_tx: watch::Sender<bool>,
    processor: PhantomData<P>
}

pub struct Message<
    T: Debug + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: Serialize + DeserializeOwned + Sync + Send + 'static
> {
    message: T,
    sender: Sender<Result<R, CubeError>>
}

pub trait MessageProcessor<
    T: Debug + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: Serialize + DeserializeOwned + Sync + Send + 'static
> {
    fn process(args: T) -> Result<R, CubeError>;
}

impl<
    T: Debug + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: Serialize + DeserializeOwned + Sync + Send + 'static,
    P: MessageProcessor<T, R> + Sync + Send + 'static
> WorkerPool<T, R, P> {
    pub fn new(
        num: usize,
        timeout: Duration,
    ) -> WorkerPool<T, R, P> {
        let queue = Arc::new(unlimited::Queue::new());
        let (stopped_tx, stopped_rx) =  watch::channel(false);

        let mut workers = Vec::new();

        for _ in 0..num {
            let process = Arc::new(WorkerProcess::new(queue.clone(), timeout.clone(), stopped_rx.clone()));
            workers.push(process.clone());
            tokio::spawn(async move {
                process.processing_loop().await
            });
        }

        WorkerPool {
            workers: workers,
            stopped_tx,
            queue,
            processor: PhantomData
        }
    }

    pub async fn process(&self, message: T) -> Result<R, CubeError> {
        let (tx, rx) = oneshot::channel();
        self.queue.push(Message {
            message,
            sender: tx
        });
        Ok(rx.await??)
    }

    pub async fn stop_workers(&self) -> Result<(), CubeError> {
        self.stopped_tx.broadcast(true)?;
        for worker in self.workers.iter() {
            worker.finished_notify.notified().await;
        }
        Ok(())
    }
}

pub struct WorkerProcess<
    T: Debug + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: Serialize + DeserializeOwned + Sync + Send + 'static,
    P: MessageProcessor<T, R> + Sync + Send + 'static
> {
    queue: Arc<unlimited::Queue<Message<T, R>>>,
    timeout: Duration,
    processor: PhantomData<P>,
    stopped_rx: RwLock<watch::Receiver<bool>>,
    finished_notify: Arc<Notify>
}

impl<
    T: Debug + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: Serialize + DeserializeOwned + Sync + Send + 'static,
    P: MessageProcessor<T, R> + Sync + Send + 'static
> WorkerProcess<T, R, P> {
    fn new(
        queue: Arc<unlimited::Queue<Message<T, R>>>,
        timeout: Duration,
        stopped_rx: watch::Receiver<bool>
    ) -> Self {
        WorkerProcess {
            queue,
            timeout,
            stopped_rx: RwLock::new(stopped_rx),
            finished_notify: Arc::new(Notify::new()),
            processor: PhantomData
        }
    }

    async fn processing_loop(&self) {
        loop {
            let process = self.spawn_process();

            match process {
                Ok((mut args_tx, mut res_rx, mut handle)) => {
                    loop {
                        let mut stopped_rx = self.stopped_rx.write().await;
                        let Message { message, sender } = tokio::select! {
                            stopped = stopped_rx.recv() => {
                                if let Some(x) = stopped {
                                    if x {
                                        <WorkerProcess<T, R, P>>::kill(&mut handle);
                                        self.finished_notify.notify();
                                        return;
                                    }
                                }
                                continue;
                            }
                            message = self.queue.pop() => {
                                message
                            }
                        };
                        let process_message_res_timeout = tokio::time::timeout(self.timeout, self.process_message(message, args_tx, res_rx)).await;
                        let process_message_res = match process_message_res_timeout {
                            Ok(r) => r,
                            Err(e) => Err(CubeError::internal(format!("Timed out after waiting for {}", e)))
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
                                <WorkerProcess<T, R, P>>::kill(&mut handle);
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

    async fn process_message(&self, message:T, args_tx: IpcSender<T>, res_rx: IpcReceiver<Result<R, CubeError>>) -> Result<(R, IpcSender<T>, IpcReceiver<Result<R, CubeError>>), CubeError> {
        args_tx.send(message)?;
        let (res, res_rx) = tokio::task::spawn_blocking(move || {
            (res_rx.recv(), res_rx)
        }).await?;
        Ok((res??, args_tx, res_rx))
    }

    fn spawn_process(&self) -> Result<(IpcSender<T>, IpcReceiver<Result<R, CubeError>>, JoinHandle<()>), CubeError> {
        let (args_tx, args_rx) = ipc::channel()?;
        let (res_tx, res_rx) = ipc::channel()?;
        let handle = procspawn::spawn((args_rx, res_tx), |(rx, tx)| {
            loop {
                let res = rx.recv();
                match res {
                    Ok(args) => {
                        let send_res = tx.send(P::process(args));
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
    use std::thread;
    use std::time::Duration;

    use procspawn::{self};
    use crate::cluster::worker_pool::{MessageProcessor, WorkerPool};
    use crate::CubeError;
    use crate::queryplanner::serialized_plan::{SerializedLogicalPlan};
    use arrow::datatypes::{Schema, Field, DataType};
    use std::sync::Arc;
    use serde::{Serialize, Deserialize};

    procspawn::enable_test_support!();

    #[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
    pub enum Message {
        Delay(u64)
    }

    #[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
    pub enum Response {
        Foo(u64)
    }

    pub struct Processor;

    impl MessageProcessor<Message, Response> for Processor {
        fn process(args: Message) -> Result<Response, CubeError> {
            match args {
                Message::Delay(x) => {
                    thread::sleep(Duration::from_millis(x));
                    Ok(Response::Foo(x))
                }
            }
        }
    }

    #[tokio::test]
    async fn test_basic() {
        let pool = WorkerPool::<Message, Response, Processor>::new(4, Duration::from_millis(1000));
        assert_eq!(pool.process(Message::Delay(100)).await.unwrap(), Response::Foo(100));
        pool.stop_workers().await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent() {
        let pool = WorkerPool::<Message, Response, Processor>::new(4, Duration::from_millis(1000));
        let mut futures = Vec::new();
        for i in 0..10 {
            futures.push((i, pool.process(Message::Delay(i * 100))));
        }
        for (i, f) in futures {
            println!("Testing {} future", i);
            assert_eq!(f.await.unwrap(), Response::Foo(i * 100));
        }
        pool.stop_workers().await.unwrap();
    }

    #[tokio::test]
    async fn test_timeout() {
        let pool = WorkerPool::<Message, Response, Processor>::new(4, Duration::from_millis(450));
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
    }

    #[tokio::test]
    async fn serialize_plan() {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int64, false),
            Field::new("c2", DataType::Utf8, false),
        ]);
        let plan = SerializedLogicalPlan::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(schema)
        };
        let bytes = bincode::serialize(&plan).unwrap();
        bincode::deserialize::<SerializedLogicalPlan>(bytes.as_slice()).unwrap();
    }
}