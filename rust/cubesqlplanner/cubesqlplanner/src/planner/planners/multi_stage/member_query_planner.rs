use super::{
    MultiStageInodeMember, MultiStageInodeMemberType, MultiStageMemberType,
    MultiStageQueryDescription, RollingWindowDescription,
};
use crate::plan::{
    Cte, Expr, FilterGroup, FilterItem, From, JoinBuilder, JoinCondition, MemberExpression,
    OrderBy, QueryPlan, Schema, SelectBuilder, TimeSeries,
};
use crate::planner::planners::{
    FullKeyAggregateQueryPlanner, MultipliedMeasuresQueryPlanner, OrderPlanner, SimpleQueryPlanner,
};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::QueryProperties;
use crate::planner::{
    BaseDimension, BaseMeasure, BaseMember, BaseMemberHelper, BaseTimeDimension, VisitorContext,
};
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
        match self.description.member().member_type() {
            MultiStageMemberType::Inode(member) => match member.inode_type() {
                MultiStageInodeMemberType::RollingWindow(rolling_window_desc) => {
                    self.plan_rolling_window_query(rolling_window_desc, member, cte_schemas)
                }
                _ => self.plan_for_cte_query(member, cte_schemas),
            },
            MultiStageMemberType::Leaf(node) => match node {
                super::MultiStageLeafMemberType::Measure => self.plan_for_leaf_cte_query(),
                super::MultiStageLeafMemberType::TimeSeries(time_dimension) => {
                    self.plan_time_series_query(time_dimension.clone())
                }
            },
        }
    }

    fn plan_time_series_query(
        &self,
        time_dimension: Rc<BaseTimeDimension>,
    ) -> Result<Rc<Cte>, CubeError> {
        let granularity = time_dimension.get_granularity().unwrap(); //FIXME remove this unwrap
        let date_range = time_dimension.get_date_range().unwrap(); //FIXME remove this unwrap
        let from_date = date_range[0].clone();
        let to_date = date_range[1].clone();
        let seria = self
            .query_tools
            .base_tools()
            .generate_time_series(granularity, date_range.clone())?;
        let time_seira = TimeSeries {
            time_dimension_name: time_dimension.full_name(),
            from_date: Some(from_date),
            to_date: Some(to_date),
            seria,
        };
        let query_plan = Rc::new(QueryPlan::TimeSeries(Rc::new(time_seira)));
        Ok(Rc::new(Cte::new(query_plan, format!("time_series"))))
    }

    fn plan_rolling_window_query(
        &self,
        rolling_window_desc: &RollingWindowDescription,
        multi_stage_member: &MultiStageInodeMember,
        cte_schemas: &HashMap<String, Rc<Schema>>,
    ) -> Result<Rc<Cte>, CubeError> {
        let inputs = self.input_cte_aliases();
        let dimensions = self.all_dimensions();

        let root_alias = format!("time_series");
        let cte_schema = cte_schemas.get(&inputs[0]).unwrap().clone();

        let mut join_builder = JoinBuilder::new_from_table_reference(
            inputs[0].clone(),
            cte_schema,
            Some(root_alias.clone()),
        );

        for (i, input) in inputs.iter().skip(1).enumerate() {
            let alias = format!("rolling_{}", i + 1);
            let on = JoinCondition::new_rolling_join(
                alias.clone(),
                root_alias.clone(),
                rolling_window_desc.trailing.clone(),
                rolling_window_desc.leading.clone(),
                rolling_window_desc.offset.clone(),
                rolling_window_desc.time_dimension.clone(),
            );
            let cte_schema = cte_schemas.get(input).unwrap().clone();
            join_builder.left_join_table_reference(
                input.clone(),
                cte_schema,
                Some(format!("rolling_{}", i + 1)),
                on,
            );
        }

        let from = From::new_from_join(join_builder.build());

        let group_by = dimensions
            .iter()
            .map(|dim| Expr::Member(MemberExpression::new(dim.clone(), None)))
            .collect_vec();

        let context_factory = SqlNodesFactory::new();
        let node_context = context_factory.rolling_window_node_processor();

        let mut select_builder = SelectBuilder::new(from, VisitorContext::new(None, node_context));
        for dim in dimensions.iter() {
            if dim.full_name() == rolling_window_desc.time_dimension.full_name() {
                select_builder.add_projection_member(
                    &dim,
                    Some(root_alias.clone()),
                    Some(
                        cte_schemas
                            .get(&inputs[1])
                            .unwrap()
                            .resolve_member_alias(&dim, &Some(inputs[1].clone())),
                    ),
                );
            } else {
                select_builder.add_projection_member(&dim, None, None);
            }
        }

        let query_member = self.query_member_as_base_member()?;
        select_builder.add_projection_member(&query_member, None, None);
        select_builder.set_group_by(group_by);
        select_builder.set_order_by(self.query_order()?);
        let select = select_builder.build();

        Ok(Rc::new(Cte::new_from_select(
            Rc::new(select),
            self.description.alias().clone(),
        )))
    }

    fn plan_for_cte_query(
        &self,
        multi_stage_member: &MultiStageInodeMember,
        cte_schemas: &HashMap<String, Rc<Schema>>,
    ) -> Result<Rc<Cte>, CubeError> {
        let dimensions = self.all_dimensions();
        let dimensions_aliases = BaseMemberHelper::to_alias_vec(&dimensions);

        let from = From::new_from_subquery(
            Rc::new(self.make_input_join(multi_stage_member, cte_schemas)?),
            format!("{}_join", self.description.alias()),
        );

        let group_by = if multi_stage_member.is_ungrupped() {
            vec![]
        } else {
            dimensions
                .iter()
                .map(|dim| Expr::Member(MemberExpression::new(dim.clone(), None)))
                .collect_vec()
        };

        let order_by = if multi_stage_member.is_ungrupped() {
            vec![]
        } else {
            self.query_order()?
        };

        //FIXME here is direct use of alias, should be replaced with schema use
        let partition_by = self.member_partition_by(
            multi_stage_member.reduce_by(),
            multi_stage_member.group_by(),
        );

        let context_factory = SqlNodesFactory::new();

        let node_context = match multi_stage_member.inode_type() {
            MultiStageInodeMemberType::Rank => {
                context_factory.multi_stage_rank_node_processor(partition_by)
            }
            MultiStageInodeMemberType::Aggregate => {
                if partition_by != dimensions_aliases {
                    context_factory.multi_stage_window_node_processor(partition_by)
                } else {
                    context_factory.default_node_processor()
                }
            }
            _ => context_factory.default_node_processor(),
        };

        let mut select_builder = SelectBuilder::new(from, VisitorContext::new(None, node_context));
        for dim in dimensions.iter() {
            select_builder.add_projection_member(&dim, None, None);
        }

        let query_member = self.query_member_as_base_member()?;
        select_builder.add_projection_member(&query_member, None, None);
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
        multi_stage_member: &MultiStageInodeMember,
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

        let cte_query_properties = QueryProperties::try_new_from_precompiled(
            self.query_tools.clone(),
            measures,
            self.description.state().dimensions().clone(),
            self.description.state().time_dimensions().clone(),
            self.description.state().time_dimensions_filters().clone(),
            self.description.state().dimensions_filters().clone(),
            self.description.state().measures_filters().clone(),
            vec![],
            None,
            None,
            true,
        )?;

        let node_factory = if self.description.state().time_shifts().is_empty() {
            SqlNodesFactory::new()
        } else {
            SqlNodesFactory::new_with_time_shifts(self.description.state().time_shifts().clone())
        };

        /* if cte_query_properties
            .full_key_aggregate_measures()?
            .has_multi_stage_measures()
        {
            return Err(CubeError::internal(format!(
                "Leaf multi stage query cannot contain multi stage member"
            )));
        } */

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
                self.description.state().time_dimensions(),
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
                self.description.state().time_dimensions(),
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

    fn query_member_as_base_member(&self) -> Result<Rc<dyn BaseMember>, CubeError> {
        if let Some(measure) = BaseMeasure::try_new(
            self.description.member_node().clone(),
            self.query_tools.clone(),
        )? {
            Ok(measure)
        } else if let Some(dimension) = BaseDimension::try_new(
            self.description.member_node().clone(),
            self.query_tools.clone(),
        )? {
            Ok(dimension)
        } else {
            Err(CubeError::internal(
                "Expected measure or dimension as multi stage member".to_string(),
            ))
        }
    }

    fn member_partition_by(
        &self,
        reduce_by: &Vec<String>,
        group_by: &Option<Vec<String>>,
    ) -> Vec<String> {
        let dimensions = self.all_dimensions();
        let dimensions = if !reduce_by.is_empty() {
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
            &self.description.state().time_dimensions(),
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
            &self.description.state().time_dimensions(),
            &measures,
        );
        let mut all_members = self.all_dimensions().clone();
        all_members.extend(BaseMemberHelper::iter_as_base_member(&measures));
        Ok(OrderPlanner::custom_order(&order_items, &all_members))
    }
}
