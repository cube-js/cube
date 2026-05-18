//! Async Rust client for CubeStore's WebSocket protocol.
//!
//! The wire format is binary FlatBuffers (see `cubeshared::codegen::HttpMessage`).
//! This crate is a Rust port of the JavaScript driver in
//! `packages/cubejs-cubestore-driver/src/WebSocketConnection.ts` — request/response
//! correlation by `message_id`, 5-second heartbeats, exponential-backoff reconnect
//! with in-flight resend.

mod actor;
mod client;
pub mod codec;
mod error;
mod result;

pub use client::{Client, ClientConfig};
pub use error::TransportError;
pub use result::QueryResult;
