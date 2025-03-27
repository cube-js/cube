#![allow(clippy::result_large_err)]

#[cfg(feature = "python")]
extern crate findshlibs;

pub mod auth;
pub mod channel;
pub mod config;
pub mod cross;
pub mod cubesql_utils;
pub mod gateway;
pub mod logger;
pub mod node_export;
pub mod node_obj_deserializer;
pub mod node_obj_serializer;
pub mod orchestrator;
#[cfg(feature = "python")]
pub mod python;
pub mod sql4sql;
pub mod stream;
pub mod template;
pub mod transport;
pub mod utils;

use crate::config::NodeConfigurationImpl;
use cubesql::CubeError;
use neon::prelude::*;
use once_cell::sync::OnceCell;
use tokio::runtime::{Builder, Runtime};

pub fn tokio_runtime_node<'a, C: Context<'a>>(cx: &mut C) -> NeonResult<&'static Runtime> {
    match tokio_runtime() {
        Ok(r) => Ok(r),
        Err(err) => cx.throw_error(err.to_string()),
    }
}

pub fn tokio_runtime() -> Result<&'static Runtime, CubeError> {
    static RUNTIME: OnceCell<Runtime> = OnceCell::new();

    RUNTIME.get_or_try_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|err| CubeError::internal(err.to_string()))
    })
}

#[cfg(feature = "neon-entrypoint")]
#[neon::main]
fn main(cx: ModuleContext) -> NeonResult<()> {
    // We use log_rerouter to swap logger, because we init logger from js side in api-gateway
    log_reroute::init().unwrap();

    node_export::setup_local_logger(log::Level::Error);

    node_export::register_module_exports::<NodeConfigurationImpl>(cx)?;

    Ok(())
}
