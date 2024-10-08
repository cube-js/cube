use crate::plan::{Expr, OrderBy};
use crate::planner::QueryProperties;
use std::rc::Rc;

pub struct OrderPlanner {
    query_properties: Rc<QueryProperties>,
}

impl OrderPlanner {
    pub fn new(query_properties: Rc<QueryProperties>) -> Self {
        Self { query_properties }
    }

    pub fn default_order(&self) -> Vec<OrderBy> {
        if let Some(granularity_dim) = self
            .query_properties
            .time_dimensions()
            .iter()
            .find(|d| d.has_granularity())
        {
            vec![OrderBy::new(Expr::Field(granularity_dim.clone()), true)]
        } else if !self.query_properties.measures().is_empty()
            && !self.query_properties.dimensions().is_empty()
        {
            vec![OrderBy::new(
                Expr::Field(self.query_properties.measures()[0].clone()),
                false,
            )]
        } else if !self.query_properties.dimensions().is_empty() {
            vec![OrderBy::new(
                Expr::Field(self.query_properties.dimensions()[0].clone()),
                true,
            )]
        } else {
            vec![]
        }
    }
}
