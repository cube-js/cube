use super::*;
use crate::planner::query_properties::OrderByItem;
use std::rc::Rc;
pub struct SimpleQuery {
    pub schema: Rc<LogicalSchema>,
    pub dimension_subqueries: Vec<Rc<DimensionSubQuery>>,
    pub filter: Rc<LogicalFilter>,
    pub offset: Option<usize>,
    pub limit: Option<usize>,
    pub ungrouped: bool,
    pub order_by: Vec<OrderByItem>,
    pub source: Rc<LogicalJoin>,
}

impl PrettyPrint for SimpleQuery {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("RegularMeasuresQuery: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        if !self.dimension_subqueries.is_empty() {
            result.println("dimension_subqueries:", &state);
            for subquery in self.dimension_subqueries.iter() {
                subquery.pretty_print(result, &details_state);
            }
        }
        result.println("filters:", &state);
        self.filter.pretty_print(result, &details_state);
        if let Some(offset) = &self.offset {
            result.println(&format!("offset:{}", offset), &state);
        }
        if let Some(limit) = &self.limit {
            result.println(&format!("limit:{}", limit), &state);
        }
        result.println(&format!("ungrouped:{}", self.ungrouped), &state);
        if !self.order_by.is_empty() {
            result.println("order_by:", &state);
            for order_by in self.order_by.iter() {
                result.println(
                    &format!(
                        "{} {}",
                        order_by.name(),
                        if order_by.desc() { "desc" } else { "asc" }
                    ),
                    &details_state,
                );
            }
        }

        result.println("source:", &state);
        self.source.pretty_print(result, &details_state);
    }
}
