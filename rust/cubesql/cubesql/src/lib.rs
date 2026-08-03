#![allow(
    // Clippy bug: https://github.com/rust-lang/rust-clippy/issues/7422
    clippy::nonstandard_macro_braces,
)]
// #![feature(trace_macros)]
#![recursion_limit = "2048"]

// trace_macros!(false);

extern crate core;

pub mod compile;
pub mod config;
pub mod error;
pub mod sql;
pub mod telemetry;
pub mod transport;
pub mod utils;

pub type RWLockSync<A> = std::sync::RwLock<A>;
pub type RWLockAsync<B> = tokio::sync::RwLock<B>;
pub type MutexAsync<A> = tokio::sync::Mutex<A>;

pub use error::{CubeError, CubeErrorCauseType};
