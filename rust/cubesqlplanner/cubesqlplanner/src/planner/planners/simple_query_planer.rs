use super::{JoinPlanner, OrderPlanner};
use crate::plan::{Filter, Select};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::QueryProperties;
use crate::planner::VisitorContext;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct SimpleQueryPlanner {
    query_properties: Rc<QueryProperties>,
    join_planner: JoinPlanner,
    order_planner: OrderPlanner,
    context_factory: Rc<SqlNodesFactory>,
}
impl SimpleQueryPlanner {
    pub fn new(
        query_tools: Rc<QueryTools>,
        query_properties: Rc<QueryProperties>,
        context_factory: Rc<SqlNodesFactory>,
    ) -> Self {
        Self {
            join_planner: JoinPlanner::new(query_tools.clone()),
            order_planner: OrderPlanner::new(query_properties.clone()),
            query_properties,
            context_factory,
        }
    }

    pub fn plan(&self) -> Result<Select, CubeError> {
        let filter = self.query_properties.all_filters();
        let having = if self.query_properties.measures_filters().is_empty() {
            None
        } else {
            Some(Filter {
                items: self.query_properties.measures_filters().clone(),
            })
        };
        let select = Select {
            projection: self
                .query_properties
                .select_all_dimensions_and_measures(self.query_properties.measures())?,
            from: self.join_planner.make_join_node()?,
            filter,
            group_by: self.query_properties.group_by(),
            having,
            order_by: self.order_planner.default_order(),
            context: VisitorContext::default(self.context_factory.clone()),
            ctes: vec![],
            is_distinct: false,
            limit: self.query_properties.row_limit(),
            offset: self.query_properties.offset(),
        };
        Ok(select)
    }
}
