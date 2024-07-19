#![allow(
    // Clippy bug: https://github.com/rust-lang/rust-clippy/issues/7422
    clippy::nonstandard_macro_braces,
)]
#![feature(test)]
// #![feature(backtrace)]
#![feature(async_closure)]
#![feature(box_patterns)]
// #![feature(slice_internals)]
#![feature(vec_into_raw_parts)]
#![feature(hash_set_entry)]
// #![feature(trace_macros)]
#![recursion_limit = "2048"]
#![feature(error_generic_member_access)]

// trace_macros!(false);

#[macro_use]
extern crate lazy_static;
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
