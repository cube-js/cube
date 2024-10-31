use super::{CommonUtils, JoinPlanner, MultiStageQueryPlanner, OrderPlanner, SimpleQueryPlanner};
use crate::plan::{
    Filter, From, FromSource, Join, JoinItem, JoinSource, Select, SelectBuilder, Subquery,
};
use crate::planner::base_join_condition::DimensionJoinCondition;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::{
    collect_multiplied_measures, has_multi_stage_members,
};
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::BaseMember;
use crate::planner::QueryProperties;
use crate::planner::{BaseDimension, BaseMeasure, PrimaryJoinCondition, VisitorContext};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

pub struct FullKeyAggregateQueryPlanner {
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
    join_planner: JoinPlanner,
    order_planner: OrderPlanner,
    multi_stage_planner: MultiStageQueryPlanner,
    common_utils: CommonUtils,
    context_factory: Rc<SqlNodesFactory>,
}

impl FullKeyAggregateQueryPlanner {
    pub fn new(
        query_tools: Rc<QueryTools>,
        query_properties: Rc<QueryProperties>,
        context_factory: Rc<SqlNodesFactory>,
    ) -> Self {
        Self {
            join_planner: JoinPlanner::new(query_tools.clone()),
            order_planner: OrderPlanner::new(query_properties.clone()),
            common_utils: CommonUtils::new(query_tools.clone()),
            multi_stage_planner: MultiStageQueryPlanner::new(
                query_tools.clone(),
                query_properties.clone(),
            ),
            query_tools,
            query_properties,
            context_factory,
        }
    }

    pub fn plan(
        self,
        joins: Vec<Rc<Select>>,
        ctes: Vec<Rc<Subquery>>,
    ) -> Result<Select, CubeError> {
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
        inner_measures: &Vec<Rc<BaseMeasure>>,
        outer_measures: &Vec<Rc<BaseMeasure>>,
        joins: Vec<Rc<Select>>,
    ) -> Result<SelectBuilder, CubeError> {
        let root = JoinSource::new_from_select(joins[0].clone(), format!("q_0"));
        let mut join_items = vec![];
        let dimensions_to_select = self
            .query_properties
            .dimensions_for_select()
            .iter()
            .map(|d| d.alias_name())
            .collect_vec();
        let dimensions_to_select = Rc::new(dimensions_to_select);
        for (i, join) in joins.iter().skip(1).enumerate() {
            let left_alias = format!("q_{}", i);
            let right_alias = format!("q_{}", i + 1);
            let from = JoinSource::new_from_select(
                join.clone(),
                self.query_tools.escape_column_name(&format!("q_{}", i + 1)),
            );
            let join_item = JoinItem {
                from,
                on: DimensionJoinCondition::try_new(
                    left_alias,
                    right_alias,
                    dimensions_to_select.clone(),
                )?,
                is_inner: true,
            };
            join_items.push(join_item);
        }

        let references = inner_measures
            .iter()
            .map(|m| Ok((m.measure().clone(), m.alias_name())))
            .collect::<Result<HashMap<_, _>, CubeError>>()?;

        let context = VisitorContext::new(
            None,
            self.context_factory
                .with_render_references_default_node_processor(references),
        );

        let having = if self.query_properties.measures_filters().is_empty() {
            None
        } else {
            Some(Filter {
                items: self.query_properties.measures_filters().clone(),
            })
        };

        let from = From::new(FromSource::Join(Rc::new(Join {
            root,
            joins: join_items,
        })));
        let mut select_builder = SelectBuilder::new(from, context);
        select_builder.set_projection(
            self.query_properties
                .dimensions_references_and_measures("q_0", outer_measures)?,
        );
        select_builder.set_order_by(self.order_planner.default_order());
        select_builder.set_limit(self.query_properties.row_limit());
        select_builder.set_offset(self.query_properties.offset());
        Ok(select_builder)
    }
}
