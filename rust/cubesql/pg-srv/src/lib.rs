//! Bindings for emulating a PostgreSQL server (protocol v3).
//! You can find overview of the protocol at
//! <https://www.postgresql.org/docs/10/protocol.html>

mod decoding;
mod encoding;

pub mod buffer;
pub mod error;
pub mod extended;
pub mod pg_type;
pub mod protocol;
pub mod values;

pub use buffer::*;
pub use decoding::*;
pub use encoding::*;
pub use error::*;
pub use extended::*;
pub use pg_type::*;
pub use values::*;
