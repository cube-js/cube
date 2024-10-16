use super::{MultiStageApplyedState, MultiStageQueryDescription};
use crate::plan::{
    Expr, From, FromSource, Join, JoinItem, JoinSource, OrderBy, QueryPlan, Select, Subquery,
};
use crate::planner::base_join_condition::DimensionJoinCondition;
use crate::planner::planners::{FullKeyAggregateQueryPlanner, OrderPlanner, SimpleQueryPlanner};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::member_childs;
use crate::planner::sql_evaluator::sql_nodes::{
    multi_stage_rank_node_processor, multi_stage_window_node_processor,
    with_render_references_default_node_processor,
};
use crate::planner::sql_evaluator::EvaluationNode;
use crate::planner::QueryProperties;
use crate::planner::{BaseDimension, BaseMeasure, BaseMember, BaseMemberHelper, VisitorContext};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;
pub struct MultiStageMemberQueryPlanner {
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
    description: Rc<MultiStageQueryDescription>,
}

impl MultiStageMemberQueryPlanner {
    pub fn new(
        query_tools: Rc<QueryTools>,
        query_properties: Rc<QueryProperties>,
        description: Rc<MultiStageQueryDescription>,
    ) -> Self {
        Self {
            query_tools,
            query_properties,
            description,
        }
    }

    pub fn plan_query(&self) -> Result<Rc<Subquery>, CubeError> {
        if self.description.is_leaf() {
            self.plan_for_leaf_cte_query()
        } else {
            self.plan_for_cte_query()
        }
    }

    fn plan_for_cte_query(&self) -> Result<Rc<Subquery>, CubeError> {
        let query_member = self.query_member_as_measure().unwrap();

        let dimensions = self.all_dimensions();
        let dimensions_aliases = BaseMemberHelper::to_alias_vec(&dimensions);

        let from = From::new_from_subquery(
            Rc::new(self.make_input_join()?),
            format!("{}_join", self.description.alias()),
        );

        let mut projection = dimensions_aliases
            .iter()
            .map(|d| Expr::Reference(None, d.clone()))
            .collect_vec();

        let group_by = if query_member.is_multi_stage_ungroupped() {
            vec![]
        } else {
            projection.clone()
        };

        let order_by = if query_member.is_multi_stage_ungroupped() {
            vec![]
        } else {
            self.query_order()
        };

        projection.push(Expr::Field(query_member.clone()));

        let references = BaseMemberHelper::to_reference_map(&self.all_input_members());

        let partition = self.member_partition(query_member.reduce_by(), query_member.group_by());

        let node_context = if query_member.measure_type() == "rank" {
            multi_stage_rank_node_processor(partition, references)
        } else if !query_member.is_calculated() && partition != dimensions_aliases {
            multi_stage_window_node_processor(partition, references)
        } else {
            with_render_references_default_node_processor(references)
        };

        let select = Select {
            projection,
            from,
            filter: None,
            group_by,
            having: None,
            order_by,
            context: VisitorContext::new(None, node_context),
            ctes: vec![],
            is_distinct: false,
        };

        Ok(Rc::new(Subquery::new_from_select(
            Rc::new(select),
            self.description.alias().clone(),
        )))
    }

    fn make_input_join(&self) -> Result<QueryPlan, CubeError> {
        let inputs = self.input_cte_aliases();
        let dimensions_aliases = BaseMemberHelper::to_alias_vec(&self.all_input_dimensions());
        let measures_aliases = BaseMemberHelper::to_alias_vec(&self.input_measures());

        let root_alias = format!("q_0");
        let root = JoinSource::new_from_reference(inputs[0].clone(), root_alias.clone());
        let mut join_items = vec![];
        let dimensions_aliases = Rc::new(dimensions_aliases.clone());
        for (i, input) in inputs.iter().skip(1).enumerate() {
            let left_alias = format!("q_{}", i);
            let right_alias = format!("q_{}", i + 1);
            let from = JoinSource::new_from_reference(
                input.clone(),
                self.query_tools.escape_column_name(&format!("q_{}", i + 1)),
            );
            let join_item = JoinItem {
                from,
                on: DimensionJoinCondition::try_new(
                    left_alias,
                    right_alias,
                    dimensions_aliases.clone(),
                )?,
                is_inner: true,
            };
            join_items.push(join_item);
        }

        let projection = dimensions_aliases
            .iter()
            .map(|d| Expr::Reference(Some(root_alias.clone()), d.clone()))
            .chain(
                measures_aliases
                    .iter()
                    .map(|m| Expr::Reference(None, m.clone())),
            )
            .collect_vec();

        let select = Select {
            projection,
            from: From::new(FromSource::Join(Rc::new(Join {
                root,
                joins: join_items,
            }))),
            filter: None,
            group_by: vec![],
            having: None,
            order_by: self.subquery_order(),
            context: VisitorContext::default(),
            ctes: vec![],
            is_distinct: false,
        };
        Ok(QueryPlan::Select(Rc::new(select)))
    }

    fn plan_for_leaf_cte_query(&self) -> Result<Rc<Subquery>, CubeError> {
        let mut cte_query_properties = QueryProperties::clone(&self.query_properties);

        let member_node = self.description.member_node();

        let measures = if let Some(measure) =
            BaseMeasure::try_new_from_precompiled(member_node.clone(), self.query_tools.clone())
        {
            vec![measure]
        } else {
            vec![]
        };

        let cte_query_properties = QueryProperties::new_from_precompiled(
            measures,
            self.description.state().dimensions().clone(),
            self.query_properties.time_dimensions().clone(),
            vec![],
            vec![],
            vec![],
            vec![],
        );

        let full_key_aggregate_query_builder = FullKeyAggregateQueryPlanner::new(
            self.query_tools.clone(),
            cte_query_properties.clone(),
        );
        let cte_select = if let Some(select) = full_key_aggregate_query_builder.plan()? {
            select
        } else {
            let simple_query_builder =
                SimpleQueryPlanner::new(self.query_tools.clone(), cte_query_properties.clone());
            simple_query_builder.plan()?
        };
        let result =
            Subquery::new_from_select(Rc::new(cte_select), self.description.alias().clone());
        Ok(Rc::new(result))
    }

    fn all_dimensions(&self) -> Vec<Rc<dyn BaseMember>> {
        BaseMemberHelper::iter_as_base_member(self.description.state().dimensions())
            .chain(BaseMemberHelper::iter_as_base_member(
                self.query_properties.time_dimensions(),
            ))
            .collect_vec()
    }

    fn input_dimensions(&self) -> Vec<Rc<BaseDimension>> {
        self.description
            .input()
            .iter()
            .flat_map(|descr| descr.state().dimensions().clone())
            .unique_by(|dim| dim.full_name())
            .collect_vec()
    }

    fn all_input_dimensions(&self) -> Vec<Rc<dyn BaseMember>> {
        BaseMemberHelper::iter_as_base_member(&self.input_dimensions())
            .chain(BaseMemberHelper::iter_as_base_member(
                self.query_properties.time_dimensions(),
            ))
            .collect_vec()
    }

    fn raw_input_measures(&self) -> Vec<Rc<BaseMeasure>> {
        self.description
            .input()
            .iter()
            .filter_map(|m| {
                BaseMeasure::try_new_from_precompiled(
                    m.member_node().clone(),
                    self.query_tools.clone(),
                )
            })
            .unique_by(|m| m.full_name())
            .collect_vec()
    }
    fn input_measures(&self) -> Vec<Rc<dyn BaseMember>> {
        BaseMemberHelper::upcast_vec_to_base_member(&self.raw_input_measures())
    }

    fn all_input_members(&self) -> Vec<Rc<dyn BaseMember>> {
        self.all_input_dimensions()
            .into_iter()
            .chain(self.input_measures().into_iter())
            .collect_vec()
    }

    fn input_cte_aliases(&self) -> Vec<String> {
        self.description
            .input()
            .iter()
            .map(|d| d.alias().clone())
            .unique()
            .collect_vec()
    }

    fn query_member_as_measure(&self) -> Option<Rc<BaseMeasure>> {
        BaseMeasure::try_new_from_precompiled(
            self.description.member_node().clone(),
            self.query_tools.clone(),
        )
    }

    fn member_partition(
        &self,
        reduce_by: &Option<Vec<String>>,
        group_by: &Option<Vec<String>>,
    ) -> Vec<String> {
        let dimensions = self.all_dimensions();
        let dimensions = if let Some(reduce_by) = reduce_by {
            dimensions
                .into_iter()
                .filter(|d| {
                    if reduce_by.contains(&d.member_evaluator().full_name()) {
                        false
                    } else {
                        true
                    }
                })
                .collect_vec()
        } else {
            dimensions
        };
        let dimensions = if let Some(group_by) = group_by {
            dimensions
                .into_iter()
                .filter(|d| {
                    if group_by.contains(&d.member_evaluator().full_name()) {
                        true
                    } else {
                        false
                    }
                })
                .collect_vec()
        } else {
            dimensions
        };
        BaseMemberHelper::to_alias_vec(&dimensions)
    }

    //FIXME unoptiomal
    fn subquery_order(&self) -> Vec<OrderBy> {
        let order_items = QueryProperties::default_order(
            &self.input_dimensions(),
            &self.query_properties.time_dimensions(),
            &self.raw_input_measures(),
        );
        OrderPlanner::custom_order(&order_items, &self.all_input_members())
    }
    //FIXME unoptiomal
    fn query_order(&self) -> Vec<OrderBy> {
        let measures = if let Some(measure) = self.query_member_as_measure() {
            vec![measure]
        } else {
            vec![]
        };

        let order_items = QueryProperties::default_order(
            &self.description.state().dimensions(),
            &self.query_properties.time_dimensions(),
            &measures,
        );
        let mut all_members = self.all_dimensions().clone();
        all_members.extend(BaseMemberHelper::iter_as_base_member(&measures));
        OrderPlanner::custom_order(&order_items, &all_members)
    }
}
