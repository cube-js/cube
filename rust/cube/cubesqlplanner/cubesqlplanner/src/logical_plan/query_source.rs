use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

// Top-level source of a `Query`: a plain `QueryInputs`, an aggregated
// `FullKeyAggregate` (for multi-stage / multi-fact rewrites), or a
// matched `PreAggregation`.
logical_source_enum!(QuerySource, [QueryInputs, FullKeyAggregate, PreAggregation]);
