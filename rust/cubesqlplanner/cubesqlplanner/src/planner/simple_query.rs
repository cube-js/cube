use super::QueryRequest;
use super::VisitorContext;
use crate::plan::{Filter, Select};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct SimpleQueryPlanner {
    query_request: Rc<QueryRequest>,
}
impl SimpleQueryPlanner {
    pub fn new(query_request: Rc<QueryRequest>) -> Self {
        Self { query_request }
    }

    pub fn plan(&self) -> Result<Select, CubeError> {
        let filter = self.query_request.all_filters();
        let having = if self.query_request.measures_filters().is_empty() {
            None
        } else {
            Some(Filter {
                items: self.query_request.measures_filters().clone(),
            })
        };
        let select = Select {
            projection: self
                .query_request
                .select_all_dimensions_and_measures(self.query_request.measures())?,
            from: self.query_request.make_join_node()?,
            filter,
            group_by: self.query_request.group_by(),
            having,
            order_by: self.query_request.default_order(),
            context: VisitorContext::default(),
            is_distinct: false,
        };
        Ok(select)
    }
}
