use crate::cube_bridge::query_context::{
    QueryContextData, QueryContextFilter, QueryContextTimeDimension,
};
use crate::planner::filter::{FilterGroupOperator, FilterItem};
use crate::planner::{MemberSymbol, QueryProperties};
use std::rc::Rc;

impl QueryContextData {
    /// The context of the node planned at `state`. Members are named `cube.member` of the
    /// data model cube, so a join path, a granularity or a view member compares equal to
    /// a `CUBE.member` reference; `measures` are the query's, since a stage computes one.
    pub fn from_node(state: &QueryProperties, query_measures: &[Rc<MemberSymbol>]) -> Self {
        let mut context = Self {
            dimensions: Vec::new(),
            time_dimensions: Vec::new(),
            measures: query_measures.iter().filter_map(member_name).collect(),
            filters: Vec::new(),
            segments: Vec::new(),
        };
        // A granularity suffix in `dimensions` compiles to a time dimension too.
        for symbol in state.dimensions().iter().chain(state.time_dimensions()) {
            let Some(dimension) = member_name(symbol) else {
                continue;
            };
            let Ok(time_dimension) = symbol.as_time_dimension() else {
                context.dimensions.push(dimension);
                continue;
            };
            context.time_dimensions.push(QueryContextTimeDimension {
                dimension,
                granularity: time_dimension.granularity().clone(),
                date_range: time_dimension.date_range_vec(),
            });
        }
        for item in state.all_filter_items().iter() {
            context.collect_filter_leaves(item);
        }
        for item in state.measures_filters() {
            context.collect_filter_leaves(item);
        }
        context
    }

    // Only the `and` spine is walked: a filter inside an `or` group does not constrain a
    // member on its own.
    fn collect_filter_leaves(&mut self, item: &FilterItem) {
        match item {
            FilterItem::Group(group) => {
                if group.operator == FilterGroupOperator::And {
                    for nested in group.items.iter() {
                        self.collect_filter_leaves(nested);
                    }
                }
            }
            FilterItem::Item(filter) => {
                if let Some(member) = member_name(&filter.member_evaluator()) {
                    self.filters.push(QueryContextFilter {
                        member,
                        operator: filter.filter_operator().to_string(),
                        values: Some(
                            filter
                                .values()
                                .iter()
                                .map(|value| value.to_param_string())
                                .collect(),
                        ),
                    });
                }
            }
            FilterItem::Segment(segment) => {
                if !segment.is_member_expression() {
                    self.segments.push(segment.id().target_path());
                }
            }
        }
    }
}

fn member_name(symbol: &Rc<MemberSymbol>) -> Option<String> {
    let symbol = symbol.clone().resolve_reference_chain();
    match symbol.as_ref() {
        MemberSymbol::MemberExpression(_) => None,
        MemberSymbol::TimeDimension(time_dimension) => member_name(time_dimension.base_symbol()),
        _ => Some(symbol.id().target_path()),
    }
}
