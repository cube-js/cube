use itertools::Itertools;

use crate::planner::sql_evaluator::MemberSymbol;

use super::*;
use std::rc::Rc;
pub fn all_symbols(schema: &Rc<LogicalSchema>, filters: &LogicalFilter) -> Vec<Rc<MemberSymbol>> {
    let mut symbols = schema.all_members().cloned().collect_vec();

    if let Some(dim_filter) = filters.all_filters() {
        symbols.extend(dim_filter.all_member_evaluators().iter().cloned());
    }
    if let Some(meas_filter) = filters.measures_filter() {
        symbols.extend(meas_filter.all_member_evaluators().iter().cloned());
    }
    symbols
}
