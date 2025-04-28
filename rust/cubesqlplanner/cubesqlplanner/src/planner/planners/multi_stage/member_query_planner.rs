use super::{
    MultiStageInodeMember, MultiStageInodeMemberType, MultiStageMemberType,
    MultiStageQueryDescription, RollingWindowDescription, TimeSeriesDescription,
};
use crate::logical_plan::*;
use crate::planner::planners::{multi_stage::RollingWindowType, QueryPlanner, SimpleQueryPlanner};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::{BaseDimension, BaseMeasure, BaseMember, BaseMemberHelper, BaseTimeDimension};
use crate::planner::{OrderByItem, QueryProperties};

use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashSet;
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

        let simple_query_planer =
            SimpleQueryPlanner::new(self.query_tools.clone(), cte_query_properties);

        let (source, subquery_dimension_queries) =
            simple_query_planer.source_and_subquery_dimensions()?;

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

    fn plan_time_series_query(
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

    fn plan_for_cte_query(
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

    fn plan_for_leaf_cte_query(&self) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
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
            member_type: MultiStageMemberLogicalType::LeafMeasure(leaf_measure_plan),
        };
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
