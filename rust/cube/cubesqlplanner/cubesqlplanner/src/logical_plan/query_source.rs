use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

// Top-level source of a `Query`: a plain `LogicalJoin`, an aggregated
// `FullKeyAggregate` (for multi-stage / multi-fact rewrites), or a
// matched `PreAggregation`.
logical_source_enum!(QuerySource, [LogicalJoin, FullKeyAggregate, PreAggregation]);
