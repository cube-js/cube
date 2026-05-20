use super::cte_state::CteState;
use super::{
    MultiStageInodeMember, MultiStageInodeMemberType, MultiStageMember, RollingWindowDescription,
    TimeSeriesDescription,
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

/// Leaf CTE that computes the date bounds of a rolling-window time
/// dimension when no explicit `date_range` is set — runs a
/// `SimpleQueryPlanner` over a synthetic min/max measure pair.
pub(crate) fn build_time_series_get_range_query(
    query_tools: &Rc<QueryTools>,
    query_properties: &Rc<QueryProperties>,
    alias: String,
    time_dimension: Rc<MemberSymbol>,
    cte_state: &mut CteState,
) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
    let cte_query_properties = QueryProperties::builder()
        .query_tools(query_tools.clone())
        .time_dimensions(vec![time_dimension.clone()])
        .ignore_cumulative(true)
        .ungrouped(true)
        .disable_external_pre_aggregations(query_properties.disable_external_pre_aggregations())
        .build()?;

    let simple_query_planer = SimpleQueryPlanner::new(query_tools.clone(), cte_query_properties);

    // Bodies of DSQ CTEs encountered during source build flow into
    // the outer `cte_state` instead of being held inside this body.
    let (source, multi_stage_dimensions) =
        simple_query_planer.source_and_subquery_dimensions(cte_state)?;

    let cube_symbol = query_tools
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
        .source(source.into())
        .multi_stage_dimensions(multi_stage_dimensions)
        .build();

    Ok(Rc::new(LogicalMultiStageMember {
        name: alias,
        body: MultiStageMemberBody::Query(Rc::new(query)),
    }))
}

/// Leaf `TimeSeries` CTE — the date axis a rolling window walks
/// over. References a sibling `GetDateRange` CTE alias when the time
/// dimension has no explicit `date_range`.
pub(crate) fn build_time_series_query(
    alias: String,
    time_series_description: Rc<TimeSeriesDescription>,
) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
    let time_dimension = time_series_description.time_dimension.clone();
    let result = MultiStageTimeSeries::builder()
        .time_dimension(time_dimension.clone())
        .date_range(time_dimension.as_time_dimension()?.date_range_vec())
        .get_date_range_multistage_ref(time_series_description.date_range_cte.clone())
        .build();
    Ok(Rc::new(LogicalMultiStageMember {
        name: alias,
        body: MultiStageMemberBody::TimeSeries(Rc::new(result)),
    }))
}

/// Rolling-window CTE that combines a time-series input with a
/// measure input, dispatching on `RollingWindowDescription` into the
/// regular / to-date / running-total variant.
pub(crate) fn build_rolling_window_query(
    query_tools: &Rc<QueryTools>,
    query_properties: &Rc<QueryProperties>,
    alias: String,
    state: &Rc<QueryProperties>,
    multi_stage_member: &Rc<MultiStageMember>,
    rolling_window_desc: &RollingWindowDescription,
    children: &[Rc<MultiStageSubqueryRef>],
) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
    assert!(children.len() == 2);
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

            let evaluator_compiler_cell = query_tools.evaluator_compiler().clone();
            let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();

            let Some(granularity_obj) = GranularityHelper::make_granularity_obj(
                query_tools.cube_evaluator().clone(),
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
        .set_dimensions(query_properties.dimensions().clone())
        .set_time_dimensions(query_properties.time_dimensions().clone())
        .set_measures(vec![multi_stage_member.evaluation_node().clone()])
        .into_rc();

    let result = MultiStageRollingWindow {
        schema,
        is_ungrouped: multi_stage_member.is_ungrupped(),
        rolling_window,
        order_by: default_order_for_member(state, multi_stage_member.evaluation_node()),
        time_series_input: (*children[0]).clone(),
        measure_input: (*children[1]).clone(),
        rolling_time_dimension: rolling_window_desc.time_dimension.clone(),
        time_dimension_in_measure_input: rolling_window_desc.base_time_dimension.clone(),
    };
    Ok(Rc::new(LogicalMultiStageMember {
        name: alias,
        body: MultiStageMemberBody::RollingWindow(Rc::new(result)),
    }))
}

/// Measure-calculation CTE (Rank / Aggregate / Calculate). Picks the
/// partition-by from the inode's `reduce_by` / `group_by`, chooses a
/// window-function flavour when the partition is narrower than the
/// full dimension set, and wires `children` into a `FullKeyAggregate`
/// source.
pub(crate) fn build_for_cte_query(
    alias: String,
    state: &Rc<QueryProperties>,
    multi_stage_member: &Rc<MultiStageMember>,
    multi_stage_inode_member: &MultiStageInodeMember,
    children: &[Rc<MultiStageSubqueryRef>],
) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
    let partition_by = member_partition_by_logical(
        state,
        &multi_stage_inode_member.reduce_by_symbols(),
        &multi_stage_inode_member.group_by_symbols(),
    );

    let stage_kind = match multi_stage_inode_member.inode_type() {
        MultiStageInodeMemberType::Rank => StageKind::Rank { partition_by },
        MultiStageInodeMemberType::Aggregate => {
            if partition_by.len() != all_dimensions(state).len() {
                StageKind::Window { partition_by }
            } else {
                StageKind::Aggregation
            }
        }
        MultiStageInodeMemberType::Calculate => StageKind::Aggregation,
        _ => {
            return Err(CubeError::internal(
                "Wrong inode type for measure calculation".to_string(),
            ))
        }
    };

    let measures = if multi_stage_member.evaluation_node().is_measure() {
        vec![multi_stage_member.evaluation_node().clone()]
    } else {
        vec![]
    };
    let schema = LogicalSchema::default()
        .set_dimensions(state.dimensions().clone())
        .set_time_dimensions(state.time_dimensions().clone())
        .set_measures(measures)
        .into_rc();

    let full_key_aggregate_schema = input_schema_from_refs(children);
    let source = Rc::new(
        FullKeyAggregate::builder()
            .schema(full_key_aggregate_schema)
            .data_inputs(children.to_vec())
            .build(),
    );
    let modifiers = LogicalQueryModifiers {
        ungrouped: multi_stage_member.is_ungrupped(),
        order_by: default_order_for_member(state, multi_stage_member.evaluation_node()),
        ..Default::default()
    };
    let query = Query::builder()
        .schema(schema)
        .modifers(Rc::new(modifiers))
        .source(source.into())
        .kind(QueryKind::Stage(stage_kind))
        .build();

    Ok(Rc::new(LogicalMultiStageMember {
        name: alias,
        body: MultiStageMemberBody::Query(Rc::new(query)),
    }))
}

/// Dimension-calculation CTE for a multi-stage dimension. Includes
/// the dimension itself plus every non-multi-stage dimension carried
/// up through the child CTEs' schemas (this is the bottom-up
/// equivalent of the former `collect_all_non_multi_stage_dimension`
/// walk: extension dims added by deeper `add_group_by` directives are
/// already projected into child schemas, so a union over them yields
/// the same set).
pub(crate) fn build_for_cte_dimension_query(
    alias: String,
    state: &Rc<QueryProperties>,
    multi_stage_member: &Rc<MultiStageMember>,
    children: &[Rc<MultiStageSubqueryRef>],
) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
    let mut dimensions = state.dimensions().clone();
    let mut time_dimensions = state.time_dimensions().clone();
    let mut measures = vec![];
    let cte_member = multi_stage_member.evaluation_node();
    match cte_member.as_ref() {
        MemberSymbol::Dimension(_) => {
            if !dimensions.iter().any(|d| {
                d.clone().resolve_reference_chain() == cte_member.clone().resolve_reference_chain()
            }) {
                dimensions.push(cte_member.clone())
            }
        }
        MemberSymbol::TimeDimension(_) => {
            if !time_dimensions.iter().any(|d| {
                d.clone().resolve_reference_chain() == cte_member.clone().resolve_reference_chain()
            }) {
                time_dimensions.push(cte_member.clone())
            }
        }
        MemberSymbol::Measure(_) => measures.push(cte_member.clone()),
        _ => {}
    }
    // Carry up every dimension already projected by a child CTE — that
    // covers `add_group_by`-extended grain from deeper leaves which must
    // remain visible to outer consumers so they can join on these keys.
    for child in children.iter() {
        for dim in child.schema().dimensions.iter() {
            dimensions.push(dim.clone());
        }
        for dim in child.schema().time_dimensions.iter() {
            time_dimensions.push(dim.clone());
        }
    }
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

    let full_key_aggregate_schema = input_schema_from_refs(children);
    let source = Rc::new(
        FullKeyAggregate::builder()
            .schema(full_key_aggregate_schema)
            .data_inputs(children.to_vec())
            .build(),
    );
    let modifiers = LogicalQueryModifiers {
        order_by: default_order_for_member(state, multi_stage_member.evaluation_node()),
        ..Default::default()
    };
    let query = Query::builder()
        .schema(schema)
        .modifers(Rc::new(modifiers))
        .source(source.into())
        .kind(QueryKind::Stage(StageKind::DimensionCalc {
            multi_stage_dimension: cte_member.clone(),
        }))
        .build();

    Ok(Rc::new(LogicalMultiStageMember {
        name: alias,
        body: MultiStageMemberBody::Query(Rc::new(query)),
    }))
}

/// Leaf CTE body for a base measure — runs a fresh `QueryPlanner`
/// with `allow_multi_stage = false`. The resulting `LogicalPlan`
/// carries whatever sub-CTEs the leaf needed bundled inside, so
/// pre-agg sees the body as one rewrite unit. Respects the
/// `without-member-leaf` shape for cases like `Rank` where the leaf
/// selects only the dimension grid.
pub(crate) fn build_for_leaf_cte_query(
    query_tools: &Rc<QueryTools>,
    query_properties: &Rc<QueryProperties>,
    alias: String,
    state: &Rc<QueryProperties>,
    multi_stage_member: &Rc<MultiStageMember>,
    cte_state: &mut CteState,
) -> Result<Rc<LogicalMultiStageMember>, CubeError> {
    let member_node = multi_stage_member.evaluation_node();
    let mut dimensions = state.dimensions().clone();
    let mut time_dimensions = state.time_dimensions().clone();
    let mut measures = vec![];
    if !multi_stage_member.is_without_member_leaf() {
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
        .query_tools(query_tools.clone())
        .measures(measures)
        .dimensions(dimensions)
        .time_dimensions(time_dimensions)
        .time_dimensions_filters(state.time_dimensions_filters().clone())
        .dimensions_filters(state.dimensions_filters().clone())
        .measures_filters(state.measures_filters().clone())
        .segments(state.segments().clone())
        .time_shifts(state.time_shifts().clone())
        .ignore_cumulative(true)
        .ungrouped(multi_stage_member.is_ungrupped())
        .query_join_hints(query_properties.query_join_hints().clone())
        .allow_multi_stage(false)
        .disable_external_pre_aggregations(query_properties.disable_external_pre_aggregations())
        .build()?;

    let query_planner = QueryPlanner::new(cte_query_properties.clone(), query_tools.clone());
    // Inner CTEs (multiplied-measure keys/measure/agg-multiplied
    // bodies for a cross-cube leaf) flow into the outer `cte_state`;
    // pre-agg walks the resulting pool as a single graph from FK refs.
    let leaf_root = query_planner.plan_into(cte_state)?;
    // Render flags are leaf-CTE-only — they describe how this body is
    // rendered, not what it computes. Apply on top of whatever modifiers
    // the planner produced for the inner query.
    let modifiers = LogicalQueryModifiers {
        render_measure_as_state: multi_stage_member.has_aggregates_on_top(),
        render_measure_for_ungrouped: multi_stage_member.is_ungrupped(),
        ..(**leaf_root.modifers()).clone()
    };
    let leaf_root = leaf_root.with_modifers(Rc::new(modifiers));
    Ok(Rc::new(LogicalMultiStageMember {
        name: alias,
        body: MultiStageMemberBody::Query(leaf_root),
    }))
}

/// Builds a `MultiStageSubqueryRef` for a freshly registered CTE.
/// The schema mirrors the state-derived shape used historically by
/// `input_cte_aliases` — `state.dimensions + state.time_dimensions +
/// [member]` — which keeps the ref interchangeable with what former
/// description-tree consumers expected.
pub(crate) fn ref_for_member(
    alias: String,
    member: &Rc<MemberSymbol>,
    state: &Rc<QueryProperties>,
) -> Rc<MultiStageSubqueryRef> {
    let schema = LogicalSchema::default()
        .set_time_dimensions(state.time_dimensions().clone())
        .set_dimensions(state.dimensions().clone())
        .set_measures(vec![member.clone()])
        .into_rc();
    Rc::new(
        MultiStageSubqueryRef::builder()
            .name(alias)
            .symbols(vec![member.clone()])
            .schema(schema)
            .build(),
    )
}

fn all_dimensions(state: &Rc<QueryProperties>) -> Vec<Rc<MemberSymbol>> {
    state
        .dimensions()
        .iter()
        .cloned()
        .chain(state.time_dimensions().iter().cloned())
        .collect_vec()
}

fn input_schema_from_refs(children: &[Rc<MultiStageSubqueryRef>]) -> Rc<LogicalSchema> {
    let dimensions = children
        .iter()
        .flat_map(|c| c.schema().dimensions.iter().cloned())
        .unique_by(|d| d.full_name())
        .collect_vec();
    let time_dimensions = children
        .iter()
        .flat_map(|c| c.schema().time_dimensions.iter().cloned())
        .unique_by(|d| d.full_name())
        .collect_vec();

    LogicalSchema::default()
        .set_dimensions(dimensions)
        .set_time_dimensions(time_dimensions)
        .into_rc()
}

fn member_partition_by_logical(
    state: &Rc<QueryProperties>,
    reduce_by: &Vec<Rc<MemberSymbol>>,
    group_by: &Option<Vec<Rc<MemberSymbol>>>,
) -> Vec<Rc<MemberSymbol>> {
    let dimensions = all_dimensions(state);
    let dimensions = if !reduce_by.is_empty() {
        dimensions
            .into_iter()
            .filter(|d| !reduce_by.iter().any(|m| d.has_member_in_reference_chain(m)))
            .collect_vec()
    } else {
        dimensions
    };
    if let Some(group_by) = group_by {
        dimensions
            .into_iter()
            .filter(|d| group_by.iter().any(|m| d.has_member_in_reference_chain(m)))
            .collect_vec()
    } else {
        dimensions
    }
}

fn default_order_for_member(
    state: &Rc<QueryProperties>,
    member_node: &Rc<MemberSymbol>,
) -> Vec<OrderByItem> {
    let measures = if member_node.as_measure().is_ok() {
        vec![member_node.clone()]
    } else {
        vec![]
    };
    QueryProperties::default_order(state.dimensions(), state.time_dimensions(), &measures)
}
