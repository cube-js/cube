use super::*;
use crate::planner::query_properties::OrderByItem;
use std::rc::Rc;

pub struct FullKeyAggregateQuery {
    pub multistage_members: Vec<Rc<LogicalMultiStageMember>>,
    pub schema: Rc<LogicalSchema>,
    pub filter: Rc<LogicalFilter>,
    pub offset: Option<usize>,
    pub limit: Option<usize>,
    pub ungrouped: bool,
    pub order_by: Vec<OrderByItem>,
    pub source: Rc<FullKeyAggregate>,
}

impl PrettyPrint for FullKeyAggregateQuery {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("FullKeyAggregateQuery: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        if !self.multistage_members.is_empty() {
            result.println("multistage_members:", &state);
            for member in self.multistage_members.iter() {
                member.pretty_print(result, &details_state);
            }
        }

        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        result.println("filter:", &state);
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
