//! Utilities to run multi-process tests with `procspawn`. The main goal is properly terminating the
//! spawned processes and letting them clean up gracefully.
//!
//! At the time of writing (April 20, 2021), CubeStore may keep subprocesses hanging if the control
//! process dies unexpectedly, e.g. from SIGKILL. So these helpers are mandatory to ensure we kill
//! all processes when running the tests.

use async_trait::async_trait;
use cubestore::util::respawn::respawn;
use ipc_channel::ipc::{IpcBytesReceiver, IpcBytesSender};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::time::Duration;

/// Runs the multi-process test as instructed by the passed specification. More concretely:
///     1. Starts a number of worker processes.
///     2. Waits until each signals it started.
///     3. Runs the drive() function to completion.
///     4. Signals all the worker processes to finish.
///     5. Waits until they finish.
///
/// Users must call [procspawn::init] in the right place.
pub fn run_multiproc_test<T>(test: T)
where
    T: MultiProcTest,
    T::WorkerArgs: Serialize + DeserializeOwned,
    T::WorkerProc: WorkerProc<T::WorkerArgs>,
{
    let timeout = test.timeout();

    // Start worker processes. Each process sends an empty IPC message to indicate it is ready.
    let (send_init, recv_inits) = ipc_channel::ipc::bytes_channel().unwrap();
    let worker_inputs = test.worker_arguments();
    let num_workers = worker_inputs.len();
    let mut join_workers = Vec::with_capacity(num_workers);
    for inputs in worker_inputs {
        let (send_done, recv_done) = ipc_channel::ipc::bytes_channel().unwrap();
        let args = (send_init.clone(), recv_done, inputs, timeout);
        let handle = respawn(args, &[], &[]).unwrap();
        // Ensure we signal completion to all started workers even if errors occur along the way.
        join_workers.push(scopeguard::guard(
            (send_done, handle),
            |(send_done, mut handle)| {
                ack_error(send_done.send(&[]));
                ack_error(handle.wait());
            },
        ))
    }

    Runtime::new_current_thread().inner().block_on(async move {
        // Wait until the workers are ready.
        tokio::time::timeout(test.worker_init_timeout(), async move {
            let mut recv_init = recv_inits;
            for _ in 0..num_workers as usize {
                recv_init = tokio::task::spawn_blocking(move || {
                    recv_init.recv().unwrap();
                    recv_init
                })
                .await
                .unwrap();
            }
        })
        .await
        .expect("starting the processes took too long");

        // Finally start the main node and run the tests.
        tokio::time::timeout(timeout, test.drive())
            .await
            .expect("executing the test took too long");
    });
}

pub type ProcessArgs<T> = (
    IpcBytesSender,
    IpcBytesReceiver,
    <T as MultiProcTest>::WorkerArgs,
    Duration,
);
pub fn multiproc_child_main<T>((send_init, recv_done, inputs, timeout): ProcessArgs<T>) -> i32
where
    T: MultiProcTest,
    T::WorkerProc: WorkerProc<T::WorkerArgs>,
{
    Runtime::new_current_thread().inner().block_on(async move {
        let timed_out = tokio::time::timeout(
            timeout,
            T::WorkerProc::default().run(
                inputs,
                SignalInit { send_init },
                WaitCompletion { recv_done },
            ),
        )
        .await
        .is_err();
        if timed_out {
            eprintln!("ERROR: Stopping worker after timeout");
            return -1;
        }
        return 0;
    })
}

#[async_trait]
pub trait MultiProcTest {
    type WorkerArgs: Serialize + DeserializeOwned;
    type WorkerProc;

    /// Inputs for worker_fn. Will spawn a worker for each WorkerArgs.
    fn worker_arguments(&self) -> Vec<Self::WorkerArgs>;
    /// This function will run in the current process.
    async fn drive(self);

    /// This timeout will be applied both on worker and for the drive() function.
    fn timeout(&self) -> Duration {
        Duration::from_secs(30)
    }

    fn worker_init_timeout(&self) -> Duration {
        Duration::from_secs(4)
    }
}

#[async_trait]
pub trait WorkerProc<WorkerArgs>: Default {
    async fn run(self, args: WorkerArgs, init: SignalInit, done: WaitCompletion);
}

/// Must be called on the worker to signal it is ready for requests. `drive()` will run after we
/// receive init signals from all workers.
pub struct SignalInit {
    send_init: ipc_channel::ipc::IpcBytesSender,
}

impl SignalInit {
    pub async fn signal(self) {
        tokio::task::spawn_blocking(move || self.send_init.send(&[]))
            .await
            .unwrap()
            .unwrap()
    }
}

/// Used by the worker to wait until the test finishes, e.g. to keep processing loops.
pub struct WaitCompletion {
    recv_done: ipc_channel::ipc::IpcBytesReceiver,
}

impl WaitCompletion {
    pub async fn wait_completion(self) {
        tokio::task::spawn_blocking(move || self.recv_done.recv())
            .await
            .unwrap()
            .unwrap();
    }
}

fn ack_error<R, E: Debug>(r: Result<R, E>) -> () {
    if let Err(e) = r {
        eprintln!("Error: {:?}", e);
    }
}

/// Ensures we do not wait indefinitely for blocking tasks on drop. Really important for tests.
struct Runtime {
    rt: Option<tokio::runtime::Runtime>,
}

impl Runtime {
    fn new_current_thread() -> Runtime {
        Self::wrap(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .thread_stack_size(4 * 1024 * 1024)
                .build()
                .unwrap(),
        )
    }

    fn inner(&self) -> &tokio::runtime::Runtime {
        self.rt.as_ref().unwrap()
    }

    fn wrap(rt: tokio::runtime::Runtime) -> Runtime {
        Runtime { rt: Some(rt) }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background()
        }
    }
}
