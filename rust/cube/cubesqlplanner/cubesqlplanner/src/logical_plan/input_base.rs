use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

// The main input of a query: joined cubes, a merge of subqueries by their
// keys, or a matched pre-aggregation.
logical_source_enum!(InputBase, [QueryInputs, Merge, PreAggregation]);
