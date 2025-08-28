use super::*;
use crate::planner::query_properties::OrderByItem;

pub struct LogicalQueryModifiers {
    pub offset: Option<usize>,
    pub limit: Option<usize>,
    pub ungrouped: bool,
    pub order_by: Vec<OrderByItem>,
}

impl PrettyPrint for LogicalQueryModifiers {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        if let Some(offset) = &self.offset {
            result.println(&format!("offset:{}", offset), &state);
        }
        if let Some(limit) = &self.limit {
            result.println(&format!("limit:{}", limit), &state);
        }
        result.println(&format!("ungrouped:{}", self.ungrouped), &state);
        if !self.order_by.is_empty() {
            let details_state = state.new_level();
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
    }
}
