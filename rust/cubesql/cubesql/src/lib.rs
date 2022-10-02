#![allow(
    // Clippy bug: https://github.com/rust-lang/rust-clippy/issues/7422
    clippy::nonstandard_macro_braces,
)]
#![feature(test)]
#![feature(backtrace)]
#![feature(async_closure)]
#![feature(drain_filter)]
#![feature(box_patterns)]
#![feature(slice_internals)]
#![feature(total_cmp)]
#![feature(vec_into_raw_parts)]
#![feature(hash_set_entry)]
#![feature(map_first_last)]
// #![feature(trace_macros)]
#![recursion_limit = "512"]

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

pub type RWLockSync<A> = std::sync::RwLock<A>;
pub type RWLockAsync<B> = tokio::sync::RwLock<B>;

pub use error::{CubeError, CubeErrorCauseType};
