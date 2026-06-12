use crate::physical_plan::{Expr, MemberExpression, OrderBy};
use crate::planner::MemberSymbol;
use crate::planner::{OrderByItem, QueryProperties};
use std::rc::Rc;

/// Maps the query's `OrderByItem`s onto physical `OrderBy`
/// expressions, resolving each item against the list of selected
/// members so it can be rendered as a positional reference.
pub struct OrderPlanner {
    query_properties: Rc<QueryProperties>,
}

impl OrderPlanner {
    pub fn new(query_properties: Rc<QueryProperties>) -> Self {
        Self { query_properties }
    }

    /// Renders the request's `order_by` against the full set of
    /// selected members.
    pub fn default_order(&self) -> Vec<OrderBy> {
        Self::custom_order(
            self.query_properties.order_by(),
            &self.query_properties.all_members(false),
        )
    }

    /// Resolves an explicit `order_by` list against an explicit
    /// member list, producing positional `OrderBy` entries.
    pub fn custom_order(order_by: &[OrderByItem], members: &[Rc<MemberSymbol>]) -> Vec<OrderBy> {
        let mut result = Vec::new();
        for itm in order_by.iter() {
            for found_item in members
                .iter()
                .enumerate()
                .filter(|(_, m)| m.full_name().to_lowercase() == itm.name().to_lowercase())
            {
                result.push(OrderBy::new(
                    Expr::Member(MemberExpression::new(found_item.1.clone())),
                    found_item.0 + 1,
                    itm.desc(),
                ));
            }
        }
        result
    }
}
