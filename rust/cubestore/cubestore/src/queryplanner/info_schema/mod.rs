mod info_schema_columns;
mod info_schema_schemata;
mod info_schema_tables;
mod rocksdb_properties;
mod system_cache;
mod system_chunks;
mod system_indexes;
mod system_jobs;
mod system_partitions;
mod system_queue;
mod system_queue_results;
mod system_replay_handles;
mod system_snapshots;
mod system_tables;

use chrono::{DateTime, Utc};
pub use info_schema_columns::*;
pub use info_schema_schemata::*;
pub use info_schema_tables::*;
pub use rocksdb_properties::*;
pub use system_cache::*;
pub use system_chunks::*;
pub use system_indexes::*;
pub use system_jobs::*;
pub use system_partitions::*;
pub use system_queue::*;
pub use system_queue_results::*;
pub use system_replay_handles::*;
pub use system_snapshots::*;
pub use system_tables::*;

// This is a fairly arbitrary place to put this; maybe put it somewhere else (or pass up the error).
pub fn timestamp_nanos_or_panic(date_time: &DateTime<Utc>) -> i64 {
    date_time
        .timestamp_nanos_opt()
        .expect("value can not be represented in a timestamp with nanosecond precision.")
}
