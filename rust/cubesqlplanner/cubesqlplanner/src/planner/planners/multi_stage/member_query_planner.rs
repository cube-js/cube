use super::{
    MultiStageInodeMember, MultiStageInodeMemberType, MultiStageMemberType,
    MultiStageQueryDescription, RollingWindowDescription, TimeSeriesDescription,
};
use crate::logical_plan::*;
use crate::planner::planners::{multi_stage::RollingWindowType, QueryPlanner, SimpleQueryPlanner};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::GranularityHelper;
use crate::planner::{OrderByItem, QueryProperties};

use cubenativeutils::CubeError;
use itertools::Itertools;
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

    pub fn plan_logical_query(&self) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
        match self.description.member().member_type() {
            MultiStageMemberType::Inode(member) => match member.inode_type() {
                MultiStageInodeMemberType::RollingWindow(rolling_window_desc) => {
                    self.plan_rolling_window_query(rolling_window_desc)
                }
                _ => self.plan_for_cte_query(member),
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

    fn plan_time_series_get_range_query(
        &self,
        time_dimension: Rc<MemberSymbol>,
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
            false,
            false,
            Rc::new(vec![]),
        )?;

        let simple_query_planer =
            SimpleQueryPlanner::new(self.query_tools.clone(), cte_query_properties);

        let source = simple_query_planer.source_and_subquery_dimensions()?;

        let result = MultiStageGetDateRange {
            time_dimension: time_dimension.clone(),
            source,
        };
        let member = LogicalMultiStageMember {
            name: self.description.alias().clone(),
            member_type: MultiStageMemberLogicalType::GetDateRange(Rc::new(result)),
        };

        Ok(Rc::new(member))
    }

    fn plan_time_series_query(
        &self,
        time_series_description: Rc<TimeSeriesDescription>,
    ) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
        let time_dimension = time_series_description.time_dimension.clone();
        let result = MultiStageTimeSeries {
            time_dimension: time_dimension.clone(),
            date_range: time_dimension.as_time_dimension()?.date_range_vec(),
            get_date_range_multistage_ref: time_series_description.date_range_cte.clone(),
        };
        Ok(Rc::new(LogicalMultiStageMember {
            name: self.description.alias().clone(),
            member_type: MultiStageMemberLogicalType::TimeSeries(Rc::new(result)),
        }))
    }

    fn plan_rolling_window_query(
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
                let time_dimension = &rolling_window_desc.time_dimension;
                let query_granularity = to_date_rolling_window.granularity.clone();

                let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
                let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();

                let Some(granularity_obj) = GranularityHelper::make_granularity_obj(
                    self.query_tools.cube_evaluator().clone(),
                    &mut evaluator_compiler,
                    self.query_tools.timezone().clone(),
                    &time_dimension.cube_name(),
                    &time_dimension.name(),
                    Some(query_granularity.clone()),
                )?
                else {
                    return Err(CubeError::internal(format!(
                        "Rolling window granularity '{}' is not found in time dimension '{}'",
                        query_granularity,
                        time_dimension.name()
                    )));
                };

                MultiStageRollingWindowType::ToDate(MultiStageToDateRollingWindow {
                    granularity_obj: Rc::new(granularity_obj),
                })
            }
            RollingWindowType::RunningTotal => MultiStageRollingWindowType::RunningTotal,
        };

        let schema = LogicalSchema::default()
            .set_dimensions(self.query_properties.dimensions().clone())
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
            .set_measures(vec![self.description.member().evaluation_node().clone()])
            .into_rc();

        let result = MultiStageRollingWindow {
            schema,
            is_ungrouped: self.description.member().is_ungrupped(),
            rolling_window,
            order_by: self.query_order_by()?,
            time_series_input: MultiStageSubqueryRef {
                name: inputs[0].0.clone(),
                symbols: inputs[0].1.clone(),
            },
            measure_input: MultiStageSubqueryRef {
                name: inputs[1].0.clone(),
                symbols: inputs[1].1.clone(),
            },
            rolling_time_dimension: rolling_window_desc.time_dimension.clone(),
            time_dimension_in_measure_input: rolling_window_desc.base_time_dimension.clone(),
        };
        Ok(Rc::new(LogicalMultiStageMember {
            name: self.description.alias().clone(),
            member_type: MultiStageMemberLogicalType::RollingWindow(Rc::new(result)),
        }))
    }

    fn plan_for_cte_query(
        &self,
        multi_stage_member: &MultiStageInodeMember,
    ) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
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

        let schema = LogicalSchema::default()
            .set_dimensions(self.description.state().dimensions_symbols())
            .set_time_dimensions(self.description.state().time_dimensions_symbols())
            .set_measures(vec![self.description.member().evaluation_node().clone()])
            .into_rc();

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
            .map(|(name, symbols)| {
                Rc::new(MultiStageSubqueryRef {
                    name: name.clone(),
                    symbols: symbols.clone(),
                })
            })
            .collect_vec();

        let full_key_aggregate_schema = self.input_schema();
        let result = MultiStageMeasureCalculation {
            schema,
            is_ungrouped: self.description.member().is_ungrupped(),
            calculation_type,
            partition_by,
            window_function_to_use,
            order_by: self.query_order_by()?,

            source: Rc::new(FullKeyAggregate {
                schema: full_key_aggregate_schema,
                use_full_join_and_coalesce: true,
                multiplied_measures_resolver: None,
                multi_stage_subquery_refs: input_sources,
            }),
        };

        let result = LogicalMultiStageMember {
            name: self.description.alias().clone(),
            member_type: MultiStageMemberLogicalType::MeasureCalculation(Rc::new(result)),
        };
        Ok(Rc::new(result))
    }

    fn plan_for_leaf_cte_query(&self) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
        let member_node = self.description.member_node();
        let measures = if member_node.as_measure().is_ok() {
            vec![member_node.clone()]
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
            false,
            false,
            self.query_properties.query_join_hints().clone(),
        )?;

        let query_planner =
            QueryPlanner::new(cte_query_properties.clone(), self.query_tools.clone());
        let query = query_planner.plan()?;
        let leaf_measure_plan = MultiStageLeafMeasure {
            measure: member_node.clone(),
            query,
            render_measure_as_state: self.description.member().has_aggregates_on_top(),
            time_shifts: self.description.state().time_shifts().clone(),
            render_measure_for_ungrouped: self.description.member().is_ungrupped(),
        };
        let result = LogicalMultiStageMember {
            name: self.description.alias().clone(),
            member_type: MultiStageMemberLogicalType::LeafMeasure(Rc::new(leaf_measure_plan)),
        };
        Ok(Rc::new(result))
    }

    fn all_dimensions(&self) -> Vec<Rc<MemberSymbol>> {
        self.description
            .state()
            .dimensions()
            .iter()
            .cloned()
            .chain(self.description.state().time_dimensions().iter().cloned())
            .collect_vec()
    }

    fn input_schema(&self) -> Rc<LogicalSchema> {
        let dimensions = self
            .description
            .input()
            .iter()
            .flat_map(|descr| descr.state().dimensions_symbols().clone())
            .unique_by(|dim| dim.full_name())
            .collect_vec();
        let time_dimensions = self
            .description
            .input()
            .iter()
            .flat_map(|descr| descr.state().time_dimensions_symbols().clone())
            .unique_by(|dim| dim.full_name())
            .collect_vec();

        LogicalSchema::default()
            .set_dimensions(dimensions)
            .set_time_dimensions(time_dimensions)
            .into_rc()
    }

    fn input_cte_aliases(&self) -> Vec<(String, Vec<Rc<MemberSymbol>>)> {
        self.description
            .input()
            .iter()
            .map(|d| (d.alias().clone(), vec![d.member_node().clone()]))
            .unique_by(|(a, _)| a.clone())
            .collect_vec()
    }

    fn member_partition_by_logical(
        &self,
        reduce_by: &Vec<Rc<MemberSymbol>>,
        group_by: &Option<Vec<Rc<MemberSymbol>>>,
    ) -> Vec<Rc<MemberSymbol>> {
        let dimensions = self.all_dimensions();
        let dimensions = if !reduce_by.is_empty() {
            dimensions
                .into_iter()
                .filter(|d| !reduce_by.iter().any(|m| d.has_member_in_reference_chain(m)))
                .collect_vec()
        } else {
            dimensions
        };
        let dimensions = if let Some(group_by) = group_by {
            dimensions
                .into_iter()
                .filter(|d| group_by.iter().any(|m| d.has_member_in_reference_chain(m)))
                .collect_vec()
        } else {
            dimensions
        };
        dimensions
    }

    fn query_order_by(&self) -> Result<Vec<OrderByItem>, CubeError> {
        let member_node = self.description.member_node();
        let measures = if member_node.as_measure().is_ok() {
            vec![member_node.clone()]
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
