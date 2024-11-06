use super::OrderPlanner;
use crate::plan::{Cte, Filter, From, JoinBuilder, JoinCondition, Select, SelectBuilder};
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::BaseMemberHelper;
use crate::planner::QueryProperties;
use crate::planner::{BaseMeasure, VisitorContext};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub struct FullKeyAggregateQueryPlanner {
    query_properties: Rc<QueryProperties>,
    order_planner: OrderPlanner,
    context_factory: Rc<SqlNodesFactory>,
}

impl FullKeyAggregateQueryPlanner {
    pub fn new(
        query_properties: Rc<QueryProperties>,
        context_factory: Rc<SqlNodesFactory>,
    ) -> Self {
        Self {
            order_planner: OrderPlanner::new(query_properties.clone()),
            query_properties,
            context_factory,
        }
    }

    pub fn plan(self, joins: Vec<Rc<Select>>, ctes: Vec<Rc<Cte>>) -> Result<Select, CubeError> {
        if self.query_properties.is_simple_query()? {
            return Err(CubeError::internal(format!(
                "FullKeyAggregateQueryPlanner should not be used for simple query"
            )));
        }

        let measures = self.query_properties.full_key_aggregate_measures()?;

        let inner_measures = measures
            .multiplied_measures
            .iter()
            .chain(measures.multi_stage_measures.iter())
            .chain(measures.regular_measures.iter())
            .cloned()
            .collect_vec();

        let mut aggregate = self.outer_measures_join_full_key_aggregate(
            &inner_measures,
            &self.query_properties.measures(),
            joins,
        )?;
        if !ctes.is_empty() {
            aggregate.set_ctes(ctes.clone());
        }

        Ok(aggregate.build())
    }

    fn outer_measures_join_full_key_aggregate(
        &self,
        _inner_measures: &Vec<Rc<BaseMeasure>>,
        outer_measures: &Vec<Rc<BaseMeasure>>,
        joins: Vec<Rc<Select>>,
    ) -> Result<SelectBuilder, CubeError> {
        let mut join_builder = JoinBuilder::new_from_subselect(joins[0].clone(), format!("q_0"));
        let dimensions_to_select = self.query_properties.dimensions_for_select();
        for (i, join) in joins.iter().skip(1).enumerate() {
            let left_alias = format!("q_{}", i);
            let right_alias = format!("q_{}", i + 1);
            let on = JoinCondition::new_dimension_join(
                left_alias,
                right_alias,
                dimensions_to_select.clone(),
                true,
            );
            join_builder.inner_join_subselect(join.clone(), format!("q_{}", i + 1), on);
        }

        let context = VisitorContext::new(None, self.context_factory.default_node_processor());

        let having = if self.query_properties.measures_filters().is_empty() {
            None
        } else {
            Some(Filter {
                items: self.query_properties.measures_filters().clone(),
            })
        };

        let from = From::new_from_join(join_builder.build());
        let mut select_builder = SelectBuilder::new(from, context);

        for member in self
            .query_properties
            .all_dimensions_and_measures(&vec![])?
            .iter()
        {
            select_builder.add_projection_member(member, Some(format!("q_0")), None);
        }

        for member in BaseMemberHelper::iter_as_base_member(&outer_measures) {
            select_builder.add_projection_member(&member, None, None);
        }

        select_builder.set_order_by(self.order_planner.default_order());
        select_builder.set_having(having);
        select_builder.set_limit(self.query_properties.row_limit());
        select_builder.set_offset(self.query_properties.offset());
        Ok(select_builder)
    }
}
