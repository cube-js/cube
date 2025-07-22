use crate::plan::{Expr, MemberExpression, OrderBy};
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::{OrderByItem, QueryProperties};
use std::rc::Rc;

pub struct OrderPlanner {
    query_properties: Rc<QueryProperties>,
}

impl OrderPlanner {
    pub fn new(query_properties: Rc<QueryProperties>) -> Self {
        Self { query_properties }
    }

    pub fn default_order(&self) -> Vec<OrderBy> {
        Self::custom_order(
            self.query_properties.order_by(),
            &self.query_properties.all_members(false),
        )
    }

    pub fn custom_order(
        order_by: &Vec<OrderByItem>,
        members: &Vec<Rc<MemberSymbol>>,
    ) -> Vec<OrderBy> {
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
