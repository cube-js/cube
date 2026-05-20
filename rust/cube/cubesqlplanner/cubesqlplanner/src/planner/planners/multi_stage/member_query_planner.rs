use super::cte_state::CteState;
use super::{
    MultiStageInodeMember, MultiStageInodeMemberType, MultiStageMemberType,
    MultiStageQueryDescription, RollingWindowDescription, TimeSeriesDescription,
};
use crate::logical_plan::*;
use crate::planner::planners::{multi_stage::RollingWindowType, QueryPlanner, SimpleQueryPlanner};
use crate::planner::query_tools::QueryTools;
use crate::planner::GranularityHelper;
use crate::planner::{AggregationType, MeasureSymbol, MemberSymbol};
use crate::planner::{OrderByItem, QueryProperties};

use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;
use std::vec;

/// Renders one `MultiStageQueryDescription` into a
/// `LogicalMultiStageMember`. The shape of the output is dispatched
/// from the description's `MultiStageMemberType`: rolling-window /
/// dimension / measure inode, or a leaf (base measure /
/// time-series / time-series-get-range).
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

    /// Builds the `LogicalMultiStageMember` for this description,
    /// dispatching on `MultiStageMemberType` to the appropriate
    /// `plan_*` builder. The `cte_state` is threaded into builders that
    /// may publish additional CTEs (DSQ bodies, leaf bodies).
    pub fn plan_logical_query(
        &self,
        cte_state: &mut CteState,
    ) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
        match self.description.member().member_type() {
            MultiStageMemberType::Inode(member) => match member.inode_type() {
                MultiStageInodeMemberType::RollingWindow(rolling_window_desc) => {
                    self.plan_rolling_window_query(rolling_window_desc)
                }
                MultiStageInodeMemberType::Dimension => self.plan_for_cte_dimension_query(member),
                _ => self.plan_for_cte_query(member),
            },
            MultiStageMemberType::Leaf(node) => match node {
                super::MultiStageLeafMemberType::Measure => self.plan_for_leaf_cte_query(cte_state),
                super::MultiStageLeafMemberType::TimeSeries(time_dimension) => {
                    self.plan_time_series_query(time_dimension.clone())
                }
                super::MultiStageLeafMemberType::TimeSeriesGetRange(time_dimension) => {
                    self.plan_time_series_get_range_query(time_dimension.clone(), cte_state)
                }
            },
        }
    }

    /// Builds the leaf `GetDateRange` CTE used when a rolling-window
    /// time dimension has no explicit date range — runs a
    /// `SimpleQueryPlanner` to compute the actual bounds at query
    /// time.
    fn plan_time_series_get_range_query(
        &self,
        time_dimension: Rc<MemberSymbol>,
        cte_state: &mut CteState,
    ) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
        let cte_query_properties = QueryProperties::builder()
            .query_tools(self.query_tools.clone())
            .time_dimensions(vec![time_dimension.clone()])
            .ignore_cumulative(true)
            .ungrouped(true)
            .disable_external_pre_aggregations(
                self.query_properties.disable_external_pre_aggregations(),
            )
            .build()?;

        let simple_query_planer =
            SimpleQueryPlanner::new(self.query_tools.clone(), cte_query_properties);

        // Bodies of DSQ CTEs encountered during source build flow into
        // the outer `cte_state` instead of being held inside this body.
        let (source, multi_stage_dimensions) =
            simple_query_planer.source_and_subquery_dimensions(cte_state)?;

        let cube_symbol = self
            .query_tools
            .evaluator_compiler()
            .borrow_mut()
            .add_cube_table_evaluator(time_dimension.cube_name().clone(), vec![])?;
        let max_date = MemberSymbol::new_measure(MeasureSymbol::new_synthetic_aggregation(
            cube_symbol.clone(),
            "max_date",
            AggregationType::Max,
            time_dimension.clone(),
        ));
        let min_date = MemberSymbol::new_measure(MeasureSymbol::new_synthetic_aggregation(
            cube_symbol,
            "min_date",
            AggregationType::Min,
            time_dimension.clone(),
        ));

        let schema = LogicalSchema::default()
            .set_measures(vec![max_date, min_date])
            .into_rc();
        let query = Query::builder()
            .schema(schema)
            .filter(Rc::new(LogicalFilter::default()))
            .modifers(Rc::new(LogicalQueryModifiers::default()))
            .source(source.into())
            .multi_stage_dimensions(multi_stage_dimensions)
            .build();

        let member = LogicalMultiStageMember {
            name: self.description.alias().clone(),
            body: MultiStageMemberBody::Query(Rc::new(query)),
        };

        Ok(Rc::new(member))
    }

    /// Builds the leaf `TimeSeries` CTE — the date axis a rolling
    /// window walks over. References a sibling `GetDateRange` CTE
    /// for its bounds when the time dimension has no explicit
    /// `date_range`.
    fn plan_time_series_query(
        &self,
        time_series_description: Rc<TimeSeriesDescription>,
    ) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
        let time_dimension = time_series_description.time_dimension.clone();
        let result = MultiStageTimeSeries::builder()
            .time_dimension(time_dimension.clone())
            .date_range(time_dimension.as_time_dimension()?.date_range_vec())
            .get_date_range_multistage_ref(time_series_description.date_range_cte.clone())
            .build();
        Ok(Rc::new(LogicalMultiStageMember {
            name: self.description.alias().clone(),
            body: MultiStageMemberBody::TimeSeries(Rc::new(result)),
        }))
    }

    /// Builds the rolling-window CTE that combines a time-series
    /// input with a measure input, dispatching on
    /// `RollingWindowDescription` into the regular / to-date /
    /// running-total variant.
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
            time_series_input: MultiStageSubqueryRef::builder()
                .name(inputs[0].0.clone())
                .symbols(inputs[0].1.clone())
                .schema(inputs[0].2.clone())
                .build(),
            measure_input: MultiStageSubqueryRef::builder()
                .name(inputs[1].0.clone())
                .symbols(inputs[1].1.clone())
                .schema(inputs[1].2.clone())
                .build(),
            rolling_time_dimension: rolling_window_desc.time_dimension.clone(),
            time_dimension_in_measure_input: rolling_window_desc.base_time_dimension.clone(),
        };
        Ok(Rc::new(LogicalMultiStageMember {
            name: self.description.alias().clone(),
            body: MultiStageMemberBody::RollingWindow(Rc::new(result)),
        }))
    }

    /// Builds a measure-calculation CTE (Rank / Aggregate /
    /// Calculate). Picks the partition-by from the inode's
    /// `reduce_by` / `group_by` settings, chooses a window-function
    /// flavour (Rank, Window, or None) when the partition is
    /// narrower than the full dimension set, and wires the input
    /// CTEs into a `FullKeyAggregate` source.
    fn plan_for_cte_query(
        &self,
        multi_stage_member: &MultiStageInodeMember,
    ) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
        let partition_by = self.member_partition_by_logical(
            &multi_stage_member.reduce_by_symbols(),
            &multi_stage_member.group_by_symbols(),
        );

        let stage_kind = match multi_stage_member.inode_type() {
            MultiStageInodeMemberType::Rank => StageKind::Rank { partition_by },
            MultiStageInodeMemberType::Aggregate => {
                if partition_by.len() != self.all_dimensions().len() {
                    StageKind::Window { partition_by }
                } else {
                    StageKind::Aggregation
                }
            }
            MultiStageInodeMemberType::Calculate => StageKind::Aggregation,
            _ => {
                return Err(CubeError::internal(format!(
                    "Wrong inode type for measure calculation"
                )))
            }
        };

        let measures = if self.description.member().evaluation_node().is_measure() {
            vec![self.description.member().evaluation_node().clone()]
        } else {
            vec![]
        };
        let schema = LogicalSchema::default()
            .set_dimensions(self.description.state().dimensions().clone())
            .set_time_dimensions(self.description.state().time_dimensions().clone())
            .set_measures(measures)
            .into_rc();

        let input_sources = self
            .input_cte_aliases()
            .into_iter()
            .map(|(name, symbols, schema)| {
                Rc::new(
                    MultiStageSubqueryRef::builder()
                        .name(name.clone())
                        .symbols(symbols.clone())
                        .schema(schema)
                        .build(),
                )
            })
            .collect_vec();

        let full_key_aggregate_schema = self.input_schema();
        let source = Rc::new(
            FullKeyAggregate::builder()
                .schema(full_key_aggregate_schema)
                .data_inputs(input_sources)
                .build(),
        );
        let modifiers = LogicalQueryModifiers {
            ungrouped: self.description.member().is_ungrupped(),
            order_by: self.query_order_by()?,
            ..Default::default()
        };
        let query = Query::builder()
            .schema(schema)
            .filter(Rc::new(LogicalFilter::default()))
            .modifers(Rc::new(modifiers))
            .source(source.into())
            .kind(QueryKind::Stage(stage_kind))
            .build();

        let result = LogicalMultiStageMember {
            name: self.description.alias().clone(),
            body: MultiStageMemberBody::Query(Rc::new(query)),
        };
        Ok(Rc::new(result))
    }

    /// Builds a dimension-calculation CTE for a multi-stage
    /// dimension. Includes the dimension itself plus every
    /// non-multi-stage dimension reachable from the subtree, so the
    /// resulting CTE can be joined back into the host query on
    /// those dimensions.
    fn plan_for_cte_dimension_query(
        &self,
        _multi_stage_member: &MultiStageInodeMember,
    ) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
        let mut dimensions = self.description.state().dimensions().clone();
        let mut time_dimensions = self.description.state().time_dimensions().clone();
        let mut measures = vec![];
        let cte_member = self.description.member().evaluation_node();
        match cte_member.as_ref() {
            MemberSymbol::Dimension(_) => {
                if !dimensions.iter().any(|d| {
                    d.clone().resolve_reference_chain()
                        == cte_member.clone().resolve_reference_chain()
                }) {
                    dimensions.push(cte_member.clone())
                }
            }
            MemberSymbol::TimeDimension(_) => {
                if !time_dimensions.iter().any(|d| {
                    d.clone().resolve_reference_chain()
                        == cte_member.clone().resolve_reference_chain()
                }) {
                    time_dimensions.push(cte_member.clone())
                }
            }
            MemberSymbol::Measure(_) => measures.push(cte_member.clone()),
            _ => {}
        }
        // We add all non–multi-stage dimensions from the underlying states because
        // they’re needed to join a multi-stage dimension into the measure query
        let (all_dependend_dimensions, all_dependend_time_dimensions) =
            self.description.collect_all_non_multi_stage_dimension()?;
        dimensions.extend(all_dependend_dimensions.iter().cloned());
        time_dimensions.extend(all_dependend_time_dimensions.iter().cloned());
        dimensions = dimensions
            .into_iter()
            .unique_by(|d| d.full_name())
            .collect_vec();
        time_dimensions = time_dimensions
            .into_iter()
            .unique_by(|d| d.full_name())
            .collect_vec();

        let schema = LogicalSchema::default()
            .set_dimensions(dimensions)
            .set_time_dimensions(time_dimensions)
            .set_measures(measures)
            .into_rc();

        let input_sources = self
            .input_cte_aliases()
            .into_iter()
            .map(|(name, symbols, schema)| {
                Rc::new(
                    MultiStageSubqueryRef::builder()
                        .name(name.clone())
                        .symbols(symbols.clone())
                        .schema(schema)
                        .build(),
                )
            })
            .collect_vec();

        let full_key_aggregate_schema = self.input_schema();
        let source = Rc::new(
            FullKeyAggregate::builder()
                .schema(full_key_aggregate_schema)
                .data_inputs(input_sources)
                .build(),
        );
        let modifiers = LogicalQueryModifiers {
            order_by: self.query_order_by()?,
            ..Default::default()
        };
        let query = Query::builder()
            .schema(schema)
            .filter(Rc::new(LogicalFilter::default()))
            .modifers(Rc::new(modifiers))
            .source(source.into())
            .kind(QueryKind::Stage(StageKind::DimensionCalc {
                multi_stage_dimension: cte_member.clone(),
            }))
            .build();

        let result = LogicalMultiStageMember {
            name: self.description.alias().clone(),
            body: MultiStageMemberBody::Query(Rc::new(query)),
        };
        Ok(Rc::new(result))
    }

    /// Builds the leaf CTE body for a base measure — runs a fresh
    /// `QueryPlanner` on the description's state with
    /// `allow_multi_stage = false`. The resulting `LogicalPlan` carries
    /// whatever sub-CTEs the leaf needed bundled inside, so pre-agg
    /// sees the body as one rewrite unit. Respects the
    /// `without-member-leaf` shape for cases like `Rank` where the leaf
    /// selects only the dimension grid.
    fn plan_for_leaf_cte_query(
        &self,
        cte_state: &mut CteState,
    ) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
        let member_node = self.description.member_node();
        let mut dimensions = self.description.state().dimensions().clone();
        let mut time_dimensions = self.description.state().time_dimensions().clone();
        let mut measures = vec![];
        if !self.description.member().is_without_member_leaf() {
            match member_node.as_ref() {
                MemberSymbol::Dimension(_) => {
                    if !dimensions.iter().any(|d| {
                        d.clone().resolve_reference_chain()
                            == member_node.clone().resolve_reference_chain()
                    }) {
                        dimensions.push(member_node.clone())
                    }
                }
                MemberSymbol::TimeDimension(_) => {
                    if !time_dimensions.iter().any(|d| {
                        d.clone().resolve_reference_chain()
                            == member_node.clone().resolve_reference_chain()
                    }) {
                        time_dimensions.push(member_node.clone())
                    }
                }
                MemberSymbol::Measure(_) => measures.push(member_node.clone()),
                _ => {}
            }
        }

        let cte_query_properties = QueryProperties::builder()
            .query_tools(self.query_tools.clone())
            .measures(measures)
            .dimensions(dimensions)
            .time_dimensions(time_dimensions)
            .time_dimensions_filters(self.description.state().time_dimensions_filters().clone())
            .dimensions_filters(self.description.state().dimensions_filters().clone())
            .measures_filters(self.description.state().measures_filters().clone())
            .segments(self.description.state().segments().clone())
            .time_shifts(self.description.state().time_shifts().clone())
            .ignore_cumulative(true)
            .ungrouped(self.description.member().is_ungrupped())
            .query_join_hints(self.query_properties.query_join_hints().clone())
            .allow_multi_stage(false)
            .disable_external_pre_aggregations(
                self.query_properties.disable_external_pre_aggregations(),
            )
            .build()?;

        let query_planner =
            QueryPlanner::new(cte_query_properties.clone(), self.query_tools.clone());
        // Inner CTEs (multiplied-measure keys/measure/agg-multiplied
        // bodies for a cross-cube leaf) flow into the outer `cte_state`;
        // pre-agg walks the resulting pool as a single graph from FK refs.
        let leaf_root = query_planner.plan_into(cte_state)?;
        // Render flags are leaf-CTE-only — they describe how this body is
        // rendered, not what it computes. Apply on top of whatever modifiers
        // the planner produced for the inner query.
        let modifiers = LogicalQueryModifiers {
            render_measure_as_state: self.description.member().has_aggregates_on_top(),
            render_measure_for_ungrouped: self.description.member().is_ungrupped(),
            ..(**leaf_root.modifers()).clone()
        };
        let leaf_root = leaf_root.with_modifers(Rc::new(modifiers));
        let result = LogicalMultiStageMember {
            name: self.description.alias().clone(),
            body: MultiStageMemberBody::Query(leaf_root),
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
            .flat_map(|descr| descr.state().dimensions().iter().cloned())
            .unique_by(|dim| dim.full_name())
            .collect_vec();
        let time_dimensions = self
            .description
            .input()
            .iter()
            .flat_map(|descr| descr.state().time_dimensions().iter().cloned())
            .unique_by(|dim| dim.full_name())
            .collect_vec();

        LogicalSchema::default()
            .set_dimensions(dimensions)
            .set_time_dimensions(time_dimensions)
            .into_rc()
    }

    fn input_cte_aliases(&self) -> Vec<(String, Vec<Rc<MemberSymbol>>, Rc<LogicalSchema>)> {
        self.description
            .input()
            .iter()
            .map(|d| {
                let schema = LogicalSchema::default()
                    .set_time_dimensions(d.state().time_dimensions().clone())
                    .set_dimensions(d.state().dimensions().clone())
                    .set_measures(vec![d.member_node().clone()])
                    .into_rc();
                (d.alias().clone(), vec![d.member_node().clone()], schema)
            })
            .unique_by(|(a, _, _)| a.clone())
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
