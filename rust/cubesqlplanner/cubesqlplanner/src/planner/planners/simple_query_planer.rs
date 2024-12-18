use super::{JoinPlanner, OrderPlanner};
use crate::plan::{Filter, Select, SelectBuilder};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::{BaseMember, QueryProperties};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct SimpleQueryPlanner {
    query_properties: Rc<QueryProperties>,
    join_planner: JoinPlanner,
    order_planner: OrderPlanner,
    context_factory: SqlNodesFactory,
}
impl SimpleQueryPlanner {
    pub fn new(
        query_tools: Rc<QueryTools>,
        query_properties: Rc<QueryProperties>,
        context_factory: SqlNodesFactory,
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
        let mut context_factory = self.context_factory.clone();
        let from = self
            .join_planner
            .make_join_node_impl(&None, self.query_properties.simple_query_join()?)?;
        let mut select_builder = SelectBuilder::new(from.clone());
        for time_dim in self.query_properties.time_dimensions() {
            if let Some(granularity) = time_dim.get_granularity() {
                context_factory.add_leaf_time_dimension(&time_dim.full_name(), &granularity);
            }
        }
        for member in self
            .query_properties
            .all_dimensions_and_measures(self.query_properties.measures())?
            .iter()
        {
            select_builder.add_projection_member(member, None);
        }
        select_builder.set_filter(filter);
        select_builder.set_group_by(self.query_properties.group_by());
        select_builder.set_order_by(self.order_planner.default_order());
        select_builder.set_having(having);
        select_builder.set_limit(self.query_properties.row_limit());
        select_builder.set_offset(self.query_properties.offset());
        let res = select_builder.build(context_factory);
        Ok(res)
    }
}
