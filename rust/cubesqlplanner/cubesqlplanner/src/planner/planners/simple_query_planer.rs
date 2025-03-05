use super::{DimensionSubqueryPlanner, JoinPlanner, OrderPlanner};
use crate::plan::{Filter, Select, SelectBuilder};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::collect_sub_query_dimensions_from_symbols;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::QueryProperties;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct SimpleQueryPlanner {
    query_tools: Rc<QueryTools>,
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
            query_tools,
        }
    }

    pub fn plan(&self) -> Result<Rc<Select>, CubeError> {
        let join = self.query_properties.simple_query_join()?;
        let subquery_dimensions = collect_sub_query_dimensions_from_symbols(
            &self.query_properties.all_member_symbols(false),
            &self.join_planner,
            &join,
            self.query_tools.clone(),
        )?;
        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;

        let filter = self.query_properties.all_filters();
        let having = if self.query_properties.measures_filters().is_empty() {
            None
        } else {
            Some(Filter {
                items: self.query_properties.measures_filters().clone(),
            })
        };
        let mut context_factory = self.context_factory.clone();
        let from =
            self.join_planner
                .make_join_node_impl(&None, join, &dimension_subquery_planner)?;
        let mut select_builder = SelectBuilder::new(from.clone());

        for member in self
            .query_properties
            .all_dimensions_and_measures(self.query_properties.measures())?
            .iter()
        {
            select_builder.add_projection_member(member, None);
        }
        let render_references = dimension_subquery_planner.dimensions_refs().clone();
        context_factory.set_render_references(render_references);
        context_factory.set_rendered_as_multiplied_measures(
            self.query_properties
                .full_key_aggregate_measures()?
                .rendered_as_multiplied_measures
                .clone(),
        );
        select_builder.set_filter(filter);
        select_builder.set_group_by(self.query_properties.group_by());
        select_builder.set_order_by(self.order_planner.default_order());
        select_builder.set_having(having);
        select_builder.set_limit(self.query_properties.row_limit());
        select_builder.set_offset(self.query_properties.offset());
        let res = Rc::new(select_builder.build(context_factory));
        Ok(res)
    }
}
