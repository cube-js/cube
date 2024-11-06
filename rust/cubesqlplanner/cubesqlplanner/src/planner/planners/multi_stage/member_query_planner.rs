use super::MultiStageQueryDescription;
use crate::plan::{
    Cte, Expr, FilterGroup, FilterItem, From, JoinBuilder, JoinCondition, MemberExpression,
    OrderBy, QueryPlan, Schema, SelectBuilder,
};
use crate::planner::planners::{
    FullKeyAggregateQueryPlanner, MultipliedMeasuresQueryPlanner, OrderPlanner, SimpleQueryPlanner,
};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::QueryProperties;
use crate::planner::{BaseDimension, BaseMeasure, BaseMember, BaseMemberHelper, VisitorContext};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
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

    pub fn plan_query(
        &self,
        cte_schemas: &HashMap<String, Rc<Schema>>,
    ) -> Result<Rc<Cte>, CubeError> {
        if self.description.is_leaf() {
            self.plan_for_leaf_cte_query()
        } else {
            self.plan_for_cte_query(cte_schemas)
        }
    }

    fn plan_for_cte_query(
        &self,
        cte_schemas: &HashMap<String, Rc<Schema>>,
    ) -> Result<Rc<Cte>, CubeError> {
        let query_member = self.query_member_as_measure()?.unwrap();

        let dimensions = self.all_dimensions();
        let dimensions_aliases = BaseMemberHelper::to_alias_vec(&dimensions);

        let from = From::new_from_subquery(
            Rc::new(self.make_input_join(cte_schemas)?),
            format!("{}_join", self.description.alias()),
        );

        let group_by = if query_member.is_multi_stage_ungroupped() {
            vec![]
        } else {
            dimensions
                .iter()
                .map(|dim| Expr::Member(MemberExpression::new(dim.clone(), None)))
                .collect_vec()
        };

        let order_by = if query_member.is_multi_stage_ungroupped() {
            vec![]
        } else {
            self.query_order()?
        };

        let partition_by =
            self.member_partition_by(query_member.reduce_by(), query_member.group_by());

        let context_factory = SqlNodesFactory::new();

        let node_context = if query_member.measure_type() == "rank" {
            context_factory.multi_stage_rank_node_processor(partition_by)
        } else if !query_member.is_calculated() && partition_by != dimensions_aliases {
            context_factory.multi_stage_window_node_processor(partition_by)
        } else {
            context_factory.default_node_processor()
        };

        let mut select_builder = SelectBuilder::new(from, VisitorContext::new(None, node_context));
        for dim in dimensions.iter() {
            select_builder.add_projection_member(&dim, None, None);
        }
        select_builder.add_projection_member(&query_member.as_base_member(), None, None);
        select_builder.set_group_by(group_by);
        select_builder.set_order_by(order_by);
        let select = select_builder.build();

        Ok(Rc::new(Cte::new_from_select(
            Rc::new(select),
            self.description.alias().clone(),
        )))
    }

    fn make_input_join(
        &self,
        cte_schemas: &HashMap<String, Rc<Schema>>,
    ) -> Result<QueryPlan, CubeError> {
        let inputs = self.input_cte_aliases();
        let dimensions = self.all_input_dimensions();

        let root_alias = format!("q_0");
        let cte_schema = cte_schemas.get(&inputs[0]).unwrap().clone();
        let mut join_builder = JoinBuilder::new_from_table_reference(
            inputs[0].clone(),
            cte_schema,
            Some(root_alias.clone()),
        );
        for (i, input) in inputs.iter().skip(1).enumerate() {
            let left_alias = format!("q_{}", i);
            let right_alias = format!("q_{}", i + 1);
            let on = JoinCondition::new_dimension_join(
                left_alias,
                right_alias,
                dimensions.clone(),
                true,
            );
            let cte_schema = cte_schemas.get(input).unwrap().clone();
            join_builder.inner_join_table_reference(
                input.clone(),
                cte_schema,
                Some(format!("q_{}", i + 1)),
                on,
            );
        }

        let from = From::new_from_join(join_builder.build());
        let mut select_builder =
            SelectBuilder::new(from, VisitorContext::default(SqlNodesFactory::new()));

        for dim in dimensions.iter() {
            select_builder.add_projection_member(dim, None, None)
        }
        for meas in self.input_measures()?.iter() {
            select_builder.add_projection_member(meas, None, None)
        }
        select_builder.set_order_by(self.subquery_order()?);

        Ok(QueryPlan::Select(Rc::new(select_builder.build())))
    }

    fn plan_for_leaf_cte_query(&self) -> Result<Rc<Cte>, CubeError> {
        let member_node = self.description.member_node();

        let measures = if let Some(measure) =
            BaseMeasure::try_new(member_node.clone(), self.query_tools.clone())?
        {
            vec![measure]
        } else {
            vec![]
        };

        let allowed_filter_members = self.description.state().allowed_filter_members().clone();

        let cte_query_properties = QueryProperties::try_new_from_precompiled(
            self.query_tools.clone(),
            measures,
            self.description.state().dimensions().clone(),
            self.query_properties.time_dimensions().clone(),
            self.extract_filters(
                &allowed_filter_members,
                self.query_properties.time_dimensions_filters(),
            ),
            self.extract_filters(
                &allowed_filter_members,
                self.query_properties.dimensions_filters(),
            ),
            self.extract_filters(
                &allowed_filter_members,
                self.query_properties.measures_filters(),
            ),
            vec![],
            None,
            None,
        )?;

        let node_factory = if self.description.state().time_shifts().is_empty() {
            SqlNodesFactory::new()
        } else {
            SqlNodesFactory::new_with_time_shifts(self.description.state().time_shifts().clone())
        };

        if cte_query_properties
            .full_key_aggregate_measures()?
            .has_multi_stage_measures()
        {
            return Err(CubeError::internal(format!(
                "Leaf multi stage query cannot contain multi stage member"
            )));
        }

        let cte_select = if cte_query_properties.is_simple_query()? {
            let planner = SimpleQueryPlanner::new(
                self.query_tools.clone(),
                cte_query_properties.clone(),
                node_factory.clone(),
            );
            planner.plan()?
        } else {
            let multiplied_measures_query_planner = MultipliedMeasuresQueryPlanner::new(
                self.query_tools.clone(),
                cte_query_properties.clone(),
                node_factory.clone(),
            );
            let full_key_aggregate_planner = FullKeyAggregateQueryPlanner::new(
                cte_query_properties.clone(),
                node_factory.clone(),
            );
            let subqueries = multiplied_measures_query_planner.plan_queries()?;
            let result = full_key_aggregate_planner.plan(subqueries, vec![])?;
            result
        };
        let result = Cte::new_from_select(Rc::new(cte_select), self.description.alias().clone());
        Ok(Rc::new(result))
    }

    fn extract_filters(
        &self,
        allowed_filter_members: &HashSet<String>,
        filters: &Vec<FilterItem>,
    ) -> Vec<FilterItem> {
        let mut result = Vec::new();
        for item in filters.iter() {
            match item {
                FilterItem::Group(group) => {
                    let new_group = FilterItem::Group(Rc::new(FilterGroup::new(
                        group.operator.clone(),
                        self.extract_filters(allowed_filter_members, &group.items),
                    )));
                    result.push(new_group);
                }
                FilterItem::Item(itm) => {
                    if allowed_filter_members.contains(&itm.member_name()) {
                        result.push(FilterItem::Item(itm.clone()));
                    }
                }
            }
        }
        result
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

    fn raw_input_measures(&self) -> Result<Vec<Rc<BaseMeasure>>, CubeError> {
        let res = self
            .description
            .input()
            .iter()
            .map(|m| BaseMeasure::try_new(m.member_node().clone(), self.query_tools.clone()))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter_map(|m| m)
            .unique_by(|m| m.full_name())
            .collect_vec();
        Ok(res)
    }
    fn input_measures(&self) -> Result<Vec<Rc<dyn BaseMember>>, CubeError> {
        Ok(BaseMemberHelper::upcast_vec_to_base_member(
            &self.raw_input_measures()?,
        ))
    }

    fn all_input_members(&self) -> Result<Vec<Rc<dyn BaseMember>>, CubeError> {
        Ok(self
            .all_input_dimensions()
            .into_iter()
            .chain(self.input_measures()?.into_iter())
            .collect_vec())
    }

    fn input_cte_aliases(&self) -> Vec<String> {
        self.description
            .input()
            .iter()
            .map(|d| d.alias().clone())
            .unique()
            .collect_vec()
    }

    fn query_member_as_measure(&self) -> Result<Option<Rc<BaseMeasure>>, CubeError> {
        BaseMeasure::try_new(
            self.description.member_node().clone(),
            self.query_tools.clone(),
        )
    }

    fn member_partition_by(
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
    fn subquery_order(&self) -> Result<Vec<OrderBy>, CubeError> {
        let order_items = QueryProperties::default_order(
            &self.input_dimensions(),
            &self.query_properties.time_dimensions(),
            &self.raw_input_measures()?,
        );
        Ok(OrderPlanner::custom_order(
            &order_items,
            &self.all_input_members()?,
        ))
    }
    //FIXME unoptiomal
    fn query_order(&self) -> Result<Vec<OrderBy>, CubeError> {
        let measures = if let Some(measure) = self.query_member_as_measure()? {
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
        Ok(OrderPlanner::custom_order(&order_items, &all_members))
    }
}
