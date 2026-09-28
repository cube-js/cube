//! Transformations that derive new symbols from existing ones.
//!
//! Symbols are immutable values; every "modified copy" — whether a
//! node-local rebuild (unrolling a rolling window, patching a measure
//! type) or a rewrite of a whole dependency tree (static-filter
//! pruning, render-form substitution) — lives here, not on the symbol
//! types themselves.
//!
//! Rebuild style: a transform that decides per field rebuilds via a
//! full struct literal, so adding a field fails to compile until the
//! transform classifies it; a transform that stamps a single field
//! uses clone-and-mutate, so new fields flow through untouched.

mod filter_symbols;
mod measures_as_state;
mod multiplied;
mod patch_measure;
mod render_modifier;
mod static_filter;
mod strip_join_prefix;
mod substitute;
mod tz_converted_at_source;
mod unroll_rolling;

pub use filter_symbols::*;
pub use measures_as_state::*;
pub use multiplied::*;
pub use patch_measure::*;
pub use render_modifier::*;
pub use static_filter::*;
pub use strip_join_prefix::*;
pub use substitute::*;
pub use tz_converted_at_source::*;
pub use unroll_rolling::*;
