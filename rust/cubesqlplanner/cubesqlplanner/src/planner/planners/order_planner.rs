use crate::plan::{Expr, MemberExpression, OrderBy};
use crate::planner::{BaseMember, OrderByItem, QueryProperties};
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
        members: &Vec<Rc<dyn BaseMember>>,
    ) -> Vec<OrderBy> {
        let mut result = Vec::new();
        for itm in order_by.iter() {
            if let Some((pos, member)) = members
                .iter()
                .enumerate()
                .find(|(_, m)| m.full_name().to_lowercase() == itm.name().to_lowercase())
            {
                result.push(OrderBy::new(
                    Expr::Member(MemberExpression::new(member.clone(), None)),
                    pos + 1,
                    itm.desc(),
                ));
            }
        }
        result
    }
}
