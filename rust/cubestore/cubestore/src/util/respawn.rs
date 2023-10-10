use std::collections::{HashMap, HashSet};
use std::process::{exit, Child};

use ipc_channel::ipc::{IpcOneShotServer, IpcSender};
use mysql_common::serde::de::DeserializeOwned;
use mysql_common::serde::Serialize;

use crate::sys::process::{avoid_child_zombies, die_with_parent};
use crate::CubeError;
use std::any::type_name;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;

/// Handler for [Args] must be registered prior to this call, both in main and child processes.
pub fn respawn<Args: Serialize + DeserializeOwned>(
    args: Args,
    cmd_args: &[String],
    envs: &[(String, String)],
) -> Result<Child, CubeError> {
    let handler = type_name::<Args>();
    assert!(
        HANDLERS.read().unwrap().contains_key(handler),
        "respawn handler not registered for {}",
        handler
    );

    let (server, ipc_name) = IpcOneShotServer::<IpcSender<Args>>::new()?;

    let argv0 = std::env::args_os().next().unwrap();
    let mut cmd = std::process::Command::new(argv0);
    if !USE_TEST_CMD_ARGS.load(Ordering::Acquire) {
        cmd.args(cmd_args);
    } else {
        cmd.args(&[
            "--exact",
            "-q",
            "--test-threads=1",
            "util::respawn::test_init",
        ]);
    }
    cmd.envs(envs.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    cmd.env(HANDLER_ENV, handler);
    cmd.env(PARENT_PID_ENV, std::process::id().to_string());
    cmd.env(IPC_ENV, ipc_name);
    {
        let pushdownable_envs = PUSHDOWNABLE_ENVS.read().unwrap();
        for e in pushdownable_envs.iter() {
            if let Some(v) = std::env::var(e).ok() {
                cmd.env(e, v);
            }
        }
    }

    let c = cmd.spawn()?;
    let (_, tx) = server.accept()?;
    tx.send(args)?;
    Ok(c)
}

pub fn register_handler<Args: Serialize + DeserializeOwned + 'static>(f: fn(Args) -> i32) {
    let mut handlers = HANDLERS.write().unwrap();
    // We use type_name to simplify usage, although they are not guaranteed to be unique.
    // Hope this does not happen in practice.
    let key = type_name::<Args>();
    let had_duplicate = handlers
        .insert(
            key,
            Box::new(move |ipc_sender| deserialize_and_run(ipc_sender, f)),
        )
        .is_some();
    assert!(!had_duplicate, "duplicate handler registered for {}", key);
}

pub fn register_pushdownable_envs(envs: &'static [&'static str]) {
    let mut envs_hash = PUSHDOWNABLE_ENVS.write().unwrap();
    for e in envs.iter() {
        envs_hash.insert(e);
    }
}

pub fn init() {
    avoid_child_zombies();

    let handler_name = match std::env::var(HANDLER_ENV) {
        Ok(h) => h,
        Err(_) => return, // we're the main process.
    };
    let handlers = HANDLERS.read().unwrap();
    let handler = match handlers.get(handler_name.as_str()) {
        Some(f) => f,
        None => panic!("no respawn handler for '{}'", handler_name),
    };

    let ppid = match std::env::var(PARENT_PID_ENV) {
        Ok(id) => id
            .parse::<u64>()
            .expect("could not parse parent pid in child process"),
        Err(_) => panic!("could not get parent pid in child process"),
    };
    die_with_parent(ppid);

    let ipc_server = std::env::var(IPC_ENV).expect("could not get IPC channel name");
    exit(handler(ipc_server))
}

const HANDLER_ENV: &'static str = "__CUBESTORE_RESPAWN_PROC";
const PARENT_PID_ENV: &'static str = "__CUBESTORE_RESPAWN_PPID";
const IPC_ENV: &'static str = "__CUBESTORE_IPC_NAME";

static USE_TEST_CMD_ARGS: AtomicBool = AtomicBool::new(false);
pub fn replace_cmd_args_in_tests() {
    USE_TEST_CMD_ARGS.store(true, Ordering::Release);
}
#[test]
fn test_init() {
    init();
}

lazy_static!(
static ref HANDLERS: RwLock<
    HashMap<
        /*ArgsType*/ &'static str,
        Box<dyn Fn(/*ipc_sender*/ String) -> i32 + Send + Sync>,
    >,
> = RwLock::new(HashMap::new());
);

lazy_static! {
    static ref PUSHDOWNABLE_ENVS: RwLock<HashSet<&'static str>> = RwLock::new(HashSet::new());
}

fn deserialize_and_run<Args: Serialize + DeserializeOwned>(
    ipc_sender: String,
    f: fn(Args) -> i32,
) -> i32 {
    let (args_tx, args_rx) = ipc_channel::ipc::channel().unwrap();
    IpcSender::connect(ipc_sender)
        .unwrap()
        .send(args_tx)
        .unwrap();
    let args = args_rx
        .recv()
        .expect("failed to deserialize process arguments");
    f(args)
}
