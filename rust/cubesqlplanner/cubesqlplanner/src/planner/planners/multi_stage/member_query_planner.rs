use super::{
    MultiStageInodeMember, MultiStageInodeMemberType, MultiStageMemberType,
    MultiStageQueryDescription, RollingWindowDescription, TimeSeriesDescription,
};
use crate::logical_plan::*;
use crate::plan::{
    Cte, Expr, From, JoinBuilder, JoinCondition, MemberExpression, OrderBy, QualifiedColumnName,
    QueryPlan, Schema, SelectBuilder, TimeSeries, TimeSeriesDateRange,
};
use crate::planner::planners::{
    multi_stage::RollingWindowType, OrderPlanner, QueryPlanner, SimpleQueryPlanner,
};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::collect_sub_query_dimensions;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::ReferencesBuilder;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{BaseDimension, BaseMeasure, BaseMember, BaseMemberHelper, BaseTimeDimension};
use crate::planner::{OrderByItem, QueryProperties};

use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

pub struct MultiStageMemberQueryPlanner {
    query_tools: Rc<QueryTools>,
    _query_properties: Rc<QueryProperties>,
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
            _query_properties: query_properties,
            description,
        }
    }

    pub fn plan_logical_query(&self) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
        match self.description.member().member_type() {
            MultiStageMemberType::Inode(member) => match member.inode_type() {
                MultiStageInodeMemberType::RollingWindow(rolling_window_desc) => {
                    self.plan_logical_rolling_window_query(rolling_window_desc)
                }
                _ => self.plan_logical_for_cte_query(member),
            },
            MultiStageMemberType::Leaf(node) => match node {
                super::MultiStageLeafMemberType::Measure => self.plan_logical_for_leaf_cte_query(),
                super::MultiStageLeafMemberType::TimeSeries(time_dimension) => {
                    self.plan_logical_time_series_query(time_dimension.clone())
                }
                super::MultiStageLeafMemberType::TimeSeriesGetRange(time_dimension) => {
                    self.plan_logical_time_series_get_range_query(time_dimension.clone())
                }
            },
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
                super::MultiStageLeafMemberType::TimeSeriesGetRange(time_dimension) => {
                    self.plan_time_series_get_range_query(time_dimension.clone())
                }
            },
        }
    }

    fn plan_logical_time_series_get_range_query(
        &self,
        time_dimension: Rc<BaseTimeDimension>,
    ) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
        let cte_query_properties = QueryProperties::try_new_from_precompiled(
            self.query_tools.clone(),
            vec![],
            vec![],
            vec![time_dimension.clone()],
            vec![],
            vec![],
            vec![],
            vec![],
            vec![],
            None,
            None,
            true,
            true,
        )?;

        let simple_query_planer = SimpleQueryPlanner::new(
            self.query_tools.clone(),
            cte_query_properties,
            SqlNodesFactory::new(),
        );

        let (source, subquery_dimension_queries) =
            simple_query_planer.logical_source_and_subquery_dimensions()?;

        let result = MultiStageGetDateRange {
            time_dimension: time_dimension.member_evaluator(),
            dimension_subqueries: subquery_dimension_queries,
            source,
        };
        let member = LogicalMultiStageMember {
            name: self.description.alias().clone(),
            member_type: MultiStageMemberLogicalType::GetDateRange(result),
        };

        Ok(Rc::new(member))
    }

    fn plan_time_series_get_range_query(
        &self,
        time_dimension: Rc<BaseTimeDimension>,
    ) -> Result<Rc<Cte>, CubeError> {
        let cte_query_properties = QueryProperties::try_new_from_precompiled(
            self.query_tools.clone(),
            vec![],
            vec![],
            vec![time_dimension.clone()],
            vec![],
            vec![],
            vec![],
            vec![],
            vec![],
            None,
            None,
            true,
            true,
        )?;
        let mut context_factory = SqlNodesFactory::new();
        let simple_query_planer = SimpleQueryPlanner::new(
            self.query_tools.clone(),
            cte_query_properties,
            SqlNodesFactory::new(),
        );
        let (mut select_builder, render_references) = simple_query_planer.make_select_builder()?;
        let args = vec![time_dimension.clone().as_base_member()];
        select_builder.add_projection_function_expression(
            "MAX",
            args.clone(),
            "date_to".to_string(),
        );

        select_builder.add_projection_function_expression(
            "MIN",
            args.clone(),
            "date_from".to_string(),
        );
        context_factory.set_render_references(render_references);
        let select = Rc::new(select_builder.build(context_factory));
        let query_plan = Rc::new(QueryPlan::Select(select));
        Ok(Rc::new(Cte::new(
            query_plan,
            format!("time_series_get_range"),
        )))
    }

    fn plan_logical_time_series_query(
        &self,
        time_series_description: Rc<TimeSeriesDescription>,
    ) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
        let time_dimension = time_series_description.time_dimension.clone();
        let result = MultiStageTimeSeries {
            time_dimension: time_dimension.member_evaluator().clone(),
            date_range: time_dimension.get_date_range().clone(),
            get_date_range_multistage_ref: time_series_description.date_range_cte.clone(),
        };
        Ok(Rc::new(LogicalMultiStageMember {
            name: self.description.alias().clone(),
            member_type: MultiStageMemberLogicalType::TimeSeries(result),
        }))
    }
    fn plan_time_series_query(
        &self,
        time_series_description: Rc<TimeSeriesDescription>,
    ) -> Result<Rc<Cte>, CubeError> {
        let time_dimension = time_series_description.time_dimension.clone();
        let granularity_obj = if let Some(granularity_obj) = time_dimension.get_granularity_obj() {
            granularity_obj.clone()
        } else {
            return Err(CubeError::user(
                "Time dimension granularity is required for rolling window".to_string(),
            ));
        };

        let templates = PlanSqlTemplates::new(self.query_tools.templates_render());

        let ts_date_range = if templates.supports_generated_time_series() {
            if let Some(date_range) = time_dimension.get_range_for_time_series()? {
                TimeSeriesDateRange::Filter(date_range.0.clone(), date_range.1.clone())
            } else {
                if let Some(date_range_cte) = &time_series_description.date_range_cte {
                    TimeSeriesDateRange::Generated(date_range_cte.clone())
                } else {
                    return Err(CubeError::internal(
                        "Date range cte is required for time series without date range".to_string(),
                    ));
                }
            }
        } else {
            if let Some(date_range) = time_dimension.get_date_range() {
                TimeSeriesDateRange::Filter(date_range[0].clone(), date_range[1].clone())
            } else {
                return Err(CubeError::internal(
                    "Date range is required for time series without date range".to_string(),
                ));
            }
        };

        let time_seira = TimeSeries::new(
            self.query_tools.clone(),
            time_dimension.full_name(),
            ts_date_range,
            granularity_obj,
        );
        let query_plan = Rc::new(QueryPlan::TimeSeries(Rc::new(time_seira)));
        Ok(Rc::new(Cte::new(query_plan, format!("time_series"))))
    }

    fn plan_logical_rolling_window_query(
        &self,
        rolling_window_desc: &RollingWindowDescription,
    ) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
        let inputs = self.input_cte_aliases();
        assert!(inputs.len() == 2);
        let rolling_window = match &rolling_window_desc.rolling_window {
            RollingWindowType::Regular(regular_rolling_window) => {
                MultiStageRollingWindowType::Regular(MultiStageRegularRollingWindow {
                    trailing: regular_rolling_window.trailing.clone(),
                    leading: regular_rolling_window.leading.clone(),
                    offset: regular_rolling_window.offset.clone(),
                })
            }
            RollingWindowType::ToDate(to_date_rolling_window) => {
                MultiStageRollingWindowType::ToDate(MultiStageToDateRollingWindow {
                    granularity: to_date_rolling_window.granularity.clone(),
                })
            }
            RollingWindowType::RunningTotal => MultiStageRollingWindowType::RunningTotal,
        };

        let logical_schema = Rc::new(LogicalSchema {
            time_dimensions: self.description.state().time_dimensions_symbols(),
            dimensions: self.description.state().dimensions_symbols(),
            measures: vec![self.description.member().evaluation_node().clone()],
            multiplied_measures: HashSet::new(),
        });
        let result = MultiStageRollingWindow {
            schema: logical_schema,
            is_ungrouped: self.description.member().is_ungrupped(),
            rolling_window,
            order_by: self.query_order_by()?,
            time_series_input: inputs[0].clone(),
            measure_input: inputs[1].clone(),
            rolling_time_dimension: rolling_window_desc.time_dimension.member_evaluator(),
            time_dimension_in_measure_input: rolling_window_desc
                .base_time_dimension
                .member_evaluator(), //time dimension in measure input can have different granularity
        };
        Ok(Rc::new(LogicalMultiStageMember {
            name: self.description.alias().clone(),
            member_type: MultiStageMemberLogicalType::RollingWindow(result),
        }))
    }
    fn plan_rolling_window_query(
        &self,
        rolling_window_desc: &RollingWindowDescription,
        _multi_stage_member: &MultiStageInodeMember,
        cte_schemas: &HashMap<String, Rc<Schema>>,
    ) -> Result<Rc<Cte>, CubeError> {
        let inputs = self.input_cte_aliases();
        assert!(inputs.len() == 2);
        let all_dimensions = self.all_dimensions();

        let root_alias = format!("time_series");
        let cte_schema = cte_schemas.get(&inputs[0]).unwrap().clone();

        let mut join_builder = JoinBuilder::new_from_table_reference(
            inputs[0].clone(),
            cte_schema,
            Some(root_alias.clone()),
        );

        let input = &inputs[1];
        let alias = format!("rolling_source");
        let rolling_base_cte_schema = cte_schemas.get(input).unwrap().clone();
        let time_dimension_alias = rolling_base_cte_schema
            .resolve_member_alias(&rolling_window_desc.time_dimension.clone().as_base_member());
        let on = match &rolling_window_desc.rolling_window {
            RollingWindowType::Regular(regular_rolling_window) => {
                JoinCondition::new_regular_rolling_join(
                    root_alias.clone(),
                    regular_rolling_window.trailing.clone(),
                    regular_rolling_window.leading.clone(),
                    regular_rolling_window.offset.clone(),
                    Expr::Reference(QualifiedColumnName::new(
                        Some(alias.clone()),
                        time_dimension_alias,
                    )),
                )
            }
            RollingWindowType::ToDate(to_date_rolling_window) => {
                JoinCondition::new_to_date_rolling_join(
                    root_alias.clone(),
                    to_date_rolling_window.granularity.clone(),
                    Expr::Reference(QualifiedColumnName::new(
                        Some(alias.clone()),
                        time_dimension_alias,
                    )),
                    self.query_tools.clone(),
                )
            }
            RollingWindowType::RunningTotal => JoinCondition::new_rolling_total_join(
                root_alias.clone(),
                Expr::Reference(QualifiedColumnName::new(
                    Some(alias.clone()),
                    time_dimension_alias,
                )),
            ),
        };
        join_builder.left_join_table_reference(
            input.clone(),
            rolling_base_cte_schema.clone(),
            Some(alias.clone()),
            on,
        );

        let from = From::new_from_join(join_builder.build());

        let group_by = if self.description.member().is_ungrupped() {
            vec![]
        } else {
            all_dimensions
                .iter()
                .map(|dim| Expr::Member(MemberExpression::new(dim.clone())))
                .collect_vec()
        };

        let mut context_factory = SqlNodesFactory::new();
        context_factory.set_rolling_window(true);

        if self.description.member().is_ungrupped() {
            context_factory.set_ungrouped(true);
        }

        let references_builder = ReferencesBuilder::new(from.clone());
        let mut render_references = HashMap::new();
        let mut select_builder = SelectBuilder::new(from.clone());

        //We insert render reference for main time dimension (with the some granularity as in time series to avoid unnessesary date_tranc)
        render_references.insert(
            rolling_window_desc.time_dimension.full_name(),
            QualifiedColumnName::new(Some(root_alias.clone()), format!("date_from")),
        );

        //We also insert render reference for the base dimension of time dimension (i.e. without `_granularity` prefix to let other time dimensions make date_tranc)
        render_references.insert(
            rolling_window_desc
                .time_dimension
                .base_dimension()
                .full_name(),
            QualifiedColumnName::new(Some(root_alias.clone()), format!("date_from")),
        );

        for dim in self.description.state().time_dimensions().iter() {
            let alias =
                references_builder.resolve_alias_for_member(&dim.full_name(), &Some(alias.clone()));
            context_factory.add_dimensions_with_ignored_timezone(dim.full_name());
            select_builder.add_projection_member(&dim.clone().as_base_member(), alias);
        }
        for dim in self.description.state().dimensions().iter() {
            if dim.full_name() == rolling_window_desc.time_dimension.full_name() {
                render_references.insert(
                    dim.full_name(),
                    QualifiedColumnName::new(Some(root_alias.clone()), format!("date_from")),
                );
            } else {
                references_builder.resolve_references_for_member(
                    dim.member_evaluator(),
                    &Some(alias.clone()),
                    &mut render_references,
                )?;
            }
            let alias =
                references_builder.resolve_alias_for_member(&dim.full_name(), &Some(alias.clone()));
            select_builder.add_projection_member(&dim.clone().as_base_member(), alias);
        }

        let query_member = self.query_member_as_base_member()?;
        let query_member_base_name = rolling_base_cte_schema.resolve_member_alias(&query_member);

        context_factory.add_ungrouped_measure_reference(
            query_member.full_name(),
            QualifiedColumnName::new(Some(alias), query_member_base_name),
        );
        context_factory.set_render_references(render_references);

        select_builder.add_projection_member(&query_member, None);
        select_builder.set_group_by(group_by);
        select_builder.set_order_by(self.query_order()?);
        let select = select_builder.build(context_factory);

        Ok(Rc::new(Cte::new_from_select(
            Rc::new(select),
            self.description.alias().clone(),
        )))
    }

    fn plan_logical_for_cte_query(
        &self,
        multi_stage_member: &MultiStageInodeMember,
    ) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
        let input_dimensions = self.all_input_dimensions();

        let partition_by = self.member_partition_by_logical(
            &multi_stage_member.reduce_by_symbols(),
            &multi_stage_member.group_by_symbols(),
        );

        let window_function_to_use = match multi_stage_member.inode_type() {
            MultiStageInodeMemberType::Rank => MultiStageCalculationWindowFunction::Rank,
            MultiStageInodeMemberType::Aggregate => {
                if partition_by.len() != self.all_dimensions().len() {
                    MultiStageCalculationWindowFunction::Window
                } else {
                    MultiStageCalculationWindowFunction::None
                }
            }
            _ => MultiStageCalculationWindowFunction::None,
        };

        let logical_schema = LogicalSchema {
            time_dimensions: self.description.state().time_dimensions_symbols(),
            dimensions: self.description.state().dimensions_symbols(),
            measures: vec![self.description.member().evaluation_node().clone()],
            multiplied_measures: HashSet::new(),
        };

        let calculation_type = match multi_stage_member.inode_type() {
            MultiStageInodeMemberType::Rank => MultiStageCalculationType::Rank,
            MultiStageInodeMemberType::Aggregate => MultiStageCalculationType::Aggregate,
            MultiStageInodeMemberType::Calculate => MultiStageCalculationType::Calculate,
            _ => {
                return Err(CubeError::internal(format!(
                    "Wrong inode type for measure calculation"
                )))
            }
        };

        let input_sources = self
            .input_cte_aliases()
            .into_iter()
            .map(|alias| {
                FullKeyAggregateSource::MultiStageSubqueryRef(Rc::new(MultiStageSubqueryRef {
                    name: alias,
                }))
            })
            .collect_vec();

        let result = MultiStageMeasureCalculation {
            schema: Rc::new(logical_schema),
            is_ungrouped: self.description.member().is_ungrupped(),
            calculation_type,
            partition_by,
            window_function_to_use,
            order_by: self.query_order_by()?,
            source: Rc::new(FullKeyAggregate {
                join_dimensions: input_dimensions
                    .iter()
                    .map(|d| d.member_evaluator().clone())
                    .collect(),
                use_full_join_and_coalesce: true,
                sources: input_sources,
            }),
        };

        let result = LogicalMultiStageMember {
            name: self.description.alias().clone(),
            member_type: MultiStageMemberLogicalType::MeasureCalculation(result),
        };
        Ok(Rc::new(result))
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

        let group_by = if self.description.member().is_ungrupped() {
            vec![]
        } else {
            dimensions
                .iter()
                .map(|dim| Expr::Member(MemberExpression::new(dim.clone())))
                .collect_vec()
        };

        let order_by = if self.description.member().is_ungrupped() {
            vec![]
        } else {
            self.query_order()?
        };

        //FIXME here is direct use of alias, should be replaced with schema use
        let partition_by = self.member_partition_by(
            &multi_stage_member.reduce_by(),
            &multi_stage_member.group_by(),
        );

        let mut context_factory = SqlNodesFactory::new();

        match multi_stage_member.inode_type() {
            MultiStageInodeMemberType::Rank => context_factory.set_multi_stage_rank(partition_by),
            MultiStageInodeMemberType::Aggregate => {
                if partition_by != dimensions_aliases {
                    context_factory.set_multi_stage_window(partition_by)
                }
            }
            _ => {}
        };

        let references_builder = ReferencesBuilder::new(from.clone());
        let mut render_references = HashMap::new();
        let mut select_builder = SelectBuilder::new(from.clone());
        for dim in dimensions.iter() {
            references_builder.resolve_references_for_member(
                dim.member_evaluator(),
                &None,
                &mut render_references,
            )?;
            let alias = references_builder.resolve_alias_for_member(&dim.full_name(), &None);
            select_builder.add_projection_member(&dim, alias);
        }

        let query_member = self.query_member_as_base_member()?;
        references_builder.resolve_references_for_member(
            query_member.member_evaluator(),
            &None,
            &mut render_references,
        )?;
        let alias = references_builder.resolve_alias_for_member(&query_member.full_name(), &None);
        select_builder.add_projection_member(&query_member, alias);
        select_builder.set_group_by(group_by);
        select_builder.set_order_by(order_by);
        context_factory.set_render_references(render_references);
        if self.description.member().is_ungrupped() {
            context_factory.set_ungrouped(true);
        }
        let select = select_builder.build(context_factory);

        Ok(Rc::new(Cte::new_from_select(
            Rc::new(select),
            self.description.alias().clone(),
        )))
    }

    fn make_input_join(
        &self,
        _multi_stage_member: &MultiStageInodeMember,
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
        for (i, input) in inputs.iter().enumerate().skip(1) {
            let right_alias = format!("q_{}", i);
            let left_schema = cte_schemas.get(&inputs[i - 1]).unwrap().clone();
            let cte_schema = cte_schemas.get(input).unwrap().clone();
            let conditions = dimensions
                .iter()
                .map(|dim| {
                    (0..i)
                        .map(|left_alias| {
                            let left_alias = format!("q_{}", left_alias);
                            let alias_in_left_query = left_schema.resolve_member_alias(dim);
                            let left_ref = Expr::Reference(QualifiedColumnName::new(
                                Some(left_alias.clone()),
                                alias_in_left_query,
                            ));
                            let alias_in_right_query = cte_schema.resolve_member_alias(dim);
                            let right_ref = Expr::Reference(QualifiedColumnName::new(
                                Some(right_alias.clone()),
                                alias_in_right_query,
                            ));
                            (left_ref, right_ref)
                        })
                        .collect()
                })
                .collect_vec();
            let on = JoinCondition::new_dimension_join(conditions, true);
            join_builder.inner_join_table_reference(
                input.clone(),
                cte_schema,
                Some(format!("q_{}", i)),
                on,
            );
        }

        let from = From::new_from_join(join_builder.build());
        let references_builder = ReferencesBuilder::new(from.clone());
        let mut render_references = HashMap::new();
        let mut select_builder = SelectBuilder::new(from.clone());

        let root_source = Some(root_alias);
        for dim in dimensions.iter() {
            references_builder.resolve_references_for_member(
                dim.member_evaluator(),
                &root_source,
                &mut render_references,
            )?;
            let alias = references_builder.resolve_alias_for_member(&dim.full_name(), &root_source);
            select_builder.add_projection_member(dim, alias)
        }
        for meas in self.input_measures()?.iter() {
            references_builder.resolve_references_for_member(
                meas.member_evaluator(),
                &None,
                &mut render_references,
            )?;
            let alias = references_builder.resolve_alias_for_member(&meas.full_name(), &None);
            select_builder.add_projection_member(meas, alias)
        }
        select_builder.set_order_by(self.subquery_order()?);

        let mut node_factory = SqlNodesFactory::new();
        node_factory.set_render_references(render_references);
        Ok(QueryPlan::Select(Rc::new(
            select_builder.build(node_factory),
        )))
    }

    fn plan_logical_for_leaf_cte_query(&self) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
        let member_node = self.description.member_node();
        let measures =
            if let Some(measure) = //TODO rewrite it!!
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
            self.description.state().segments().clone(),
            vec![],
            None,
            None,
            true,
            self.description.member().is_ungrupped(),
        )?;

        let mut node_factory = SqlNodesFactory::new();
        let query_planner = QueryPlanner::new_with_context_factory(
            cte_query_properties.clone(),
            self.query_tools.clone(),
            node_factory,
        );
        let query = query_planner.plan_logical()?;
        let leaf_measure_plan = MultiStageLeafMeasure {
            measure: member_node.clone(),
            query,
            render_measure_as_state: self.description.member().has_aggregates_on_top(),
            time_shifts: self.description.state().time_shifts().clone(),
            render_measure_for_ungrouped: self.description.member().is_ungrupped(),
        };
        let result = LogicalMultiStageMember {
            name: self.description.alias().clone(),
            member_type: MultiStageMemberLogicalType::LeafMeasure(leaf_measure_plan),
        };
        Ok(Rc::new(result))
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
            self.description.state().segments().clone(),
            vec![],
            None,
            None,
            true,
            self.description.member().is_ungrupped(),
        )?;

        let mut node_factory = SqlNodesFactory::new();
        node_factory.set_time_shifts(self.description.state().time_shifts().clone());
        if self.description.member().has_aggregates_on_top() {
            node_factory.set_count_approx_as_state(true);
        }
        node_factory.set_ungrouped_measure(self.description.member().is_ungrupped());

        if cte_query_properties
            .full_key_aggregate_measures()?
            .has_multi_stage_measures()
        {
            return Err(CubeError::internal(format!(
                "Leaf multi stage query cannot contain multi stage member"
            )));
        }

        let query_planner = QueryPlanner::new_with_context_factory(
            cte_query_properties.clone(),
            self.query_tools.clone(),
            node_factory,
        );
        let cte_select = query_planner.plan()?;
        let result = Cte::new_from_select(cte_select, self.description.alias().clone());
        Ok(Rc::new(result))
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
        let res = match self.description.member_node().as_ref() {
            MemberSymbol::Dimension(_) | MemberSymbol::TimeDimension(_) => {
                BaseDimension::try_new_required(
                    self.description.member_node().clone(),
                    self.query_tools.clone(),
                )?
                .as_base_member()
            }
            MemberSymbol::Measure(_) | MemberSymbol::MemberExpression(_) => {
                // We always treat the member expression as a measure here.
                BaseMeasure::try_new_required(
                    self.description.member_node().clone(),
                    self.query_tools.clone(),
                )?
                .as_base_member()
            }
            MemberSymbol::CubeName(_) | MemberSymbol::CubeTable(_) => {
                return Err(CubeError::internal(
                    "Expected measure or dimension as multi stage member".to_string(),
                ));
            }
        };
        Ok(res)
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

    fn member_partition_by_logical(
        &self,
        reduce_by: &Vec<Rc<MemberSymbol>>,
        group_by: &Option<Vec<Rc<MemberSymbol>>>,
    ) -> Vec<Rc<MemberSymbol>> {
        let dimensions = self
            .all_dimensions()
            .into_iter()
            .map(|d| d.member_evaluator().clone())
            .collect_vec();
        let dimensions = if !reduce_by.is_empty() {
            dimensions
                .into_iter()
                .filter(|d| {
                    if reduce_by.iter().any(|m| d.full_name() == m.full_name()) {
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
                    if group_by.iter().any(|m| d.full_name() == m.full_name()) {
                        true
                    } else {
                        false
                    }
                })
                .collect_vec()
        } else {
            dimensions
        };
        dimensions
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

    fn query_order_by(&self) -> Result<Vec<OrderByItem>, CubeError> {
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
        Ok(order_items)
    }
}
