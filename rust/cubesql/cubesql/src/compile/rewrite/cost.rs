use crate::{
    compile::{
        rewrite::{
            rules::utils::granularity_str_to_int_order, CubeScanUngrouped, CubeScanWrapped,
            DimensionName, LogicalPlanLanguage, MemberErrorPriority, ScalarUDFExprFun,
            TimeDimensionGranularity, WrappedSelectUngroupedScan,
        },
        MetaContext,
    },
    transport::V1CubeMetaDimensionExt,
};
use egg::{CostFunction, Id, Language};
use std::sync::Arc;

pub struct BestCubePlan {
    meta_context: Arc<MetaContext>,
}

impl BestCubePlan {
    pub fn new(meta_context: Arc<MetaContext>) -> Self {
        Self { meta_context }
    }
}

/// This cost struct maintains following structural relationships:
/// - `replacers` > other nodes - having replacers in structure means not finished processing
/// - `table_scans` > other nodes - having table scan means not detected cube scan
/// - `empty_wrappers` > `non_detected_cube_scans` - we don't want empty wrapper to hide non detected cube scan errors
/// - `non_detected_cube_scans` > other nodes - minimize cube scans without members
/// - `filters` > `filter_members` - optimize for push down of filters
/// - `filter_members` > `cube_members` - optimize for `inDateRange` filter push down to time dimension
/// - `member_errors` > `cube_members` - extra cube members may be required (e.g. CASE)
/// - `member_errors` > `wrapper_nodes` - use SQL push down where possible if cube scan can't be detected
/// - `non_pushed_down_window` > `wrapper_nodes` - prefer to always push down window functions
/// - match errors by priority - optimize for more specific errors
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct CubePlanCost {
    replacers: i64,
    table_scans: i64,
    empty_wrappers: i64,
    non_detected_cube_scans: i64,
    unwrapped_subqueries: usize,
    member_errors: i64,
    // TODO if pre-aggregation can be used for window functions, then it'd be suboptimal
    non_pushed_down_window: i64,
    non_pushed_down_grouping_sets: i64,
    ungrouped_aggregates: usize,
    wrapper_nodes: i64,
    wrapped_select_ungrouped_scan: usize,
    ast_size_outside_wrapper: usize,
    filters: i64,
    structure_points: i64,
    filter_members: i64,
    cube_members: i64,
    errors: i64,
    time_dimensions_used_as_dimensions: i64,
    max_time_dimensions_granularity: i64,
    cube_scan_nodes: i64,
    ast_size_without_alias: usize,
    ast_size: usize,
    ast_size_inside_wrapper: usize,
    ungrouped_nodes: usize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CubePlanState {
    Wrapped,
    Unwrapped(usize),
    Wrapper,
}

impl CubePlanState {
    pub fn add_child(&self, other: &Self) -> Self {
        match (self, other) {
            (CubePlanState::Wrapper, _) => CubePlanState::Wrapper,
            (_, CubePlanState::Wrapped) => CubePlanState::Wrapped,
            (CubePlanState::Wrapped, _) => CubePlanState::Wrapped,
            (CubePlanState::Unwrapped(a), _) => CubePlanState::Unwrapped(*a),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CubePlanCostAndState {
    pub cost: CubePlanCost,
    pub state: CubePlanState,
}

impl PartialOrd for CubePlanCostAndState {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cost.cmp(&other.cost))
    }
}

impl Ord for CubePlanCostAndState {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.cost.cmp(&other.cost)
    }
}

impl CubePlanCostAndState {
    pub fn add_child(&self, other: &Self) -> Self {
        let state = self.state.add_child(&other.state);
        Self {
            cost: self.cost.add_child(&other.cost),
            state,
        }
    }

    pub fn finalize(&self, enode: &LogicalPlanLanguage) -> Self {
        Self {
            cost: self.cost.finalize(&self.state, enode),
            state: self.state.clone(),
        }
    }
}

impl CubePlanCost {
    pub fn add_child(&self, other: &Self) -> Self {
        Self {
            replacers: self.replacers + other.replacers,
            table_scans: self.table_scans + other.table_scans,
            filters: self.filters + other.filters,
            non_detected_cube_scans: (if other.cube_members == 0 {
                self.non_detected_cube_scans
            } else {
                0
            }) + other.non_detected_cube_scans,
            filter_members: self.filter_members + other.filter_members,
            non_pushed_down_window: self.non_pushed_down_window + other.non_pushed_down_window,
            non_pushed_down_grouping_sets: self.non_pushed_down_grouping_sets
                + other.non_pushed_down_grouping_sets,
            member_errors: self.member_errors + other.member_errors,
            cube_members: self.cube_members + other.cube_members,
            errors: self.errors + other.errors,
            structure_points: self.structure_points + other.structure_points,
            empty_wrappers: self.empty_wrappers + other.empty_wrappers,
            ast_size_outside_wrapper: self.ast_size_outside_wrapper
                + other.ast_size_outside_wrapper,
            ungrouped_aggregates: self.ungrouped_aggregates + other.ungrouped_aggregates,
            wrapper_nodes: self.wrapper_nodes + other.wrapper_nodes,
            wrapped_select_ungrouped_scan: self.wrapped_select_ungrouped_scan
                + other.wrapped_select_ungrouped_scan,
            cube_scan_nodes: self.cube_scan_nodes + other.cube_scan_nodes,
            time_dimensions_used_as_dimensions: self.time_dimensions_used_as_dimensions
                + other.time_dimensions_used_as_dimensions,
            max_time_dimensions_granularity: self
                .max_time_dimensions_granularity
                .max(other.max_time_dimensions_granularity),
            ast_size_without_alias: self.ast_size_without_alias + other.ast_size_without_alias,
            ast_size: self.ast_size + other.ast_size,
            ast_size_inside_wrapper: self.ast_size_inside_wrapper + other.ast_size_inside_wrapper,
            ungrouped_nodes: self.ungrouped_nodes + other.ungrouped_nodes,
            unwrapped_subqueries: self.unwrapped_subqueries + other.unwrapped_subqueries,
        }
    }

    pub fn finalize(&self, state: &CubePlanState, enode: &LogicalPlanLanguage) -> Self {
        Self {
            replacers: self.replacers,
            table_scans: self.table_scans,
            filters: self.filters,
            non_detected_cube_scans: match state {
                CubePlanState::Wrapped => 0,
                CubePlanState::Unwrapped(_) => self.non_detected_cube_scans,
                CubePlanState::Wrapper => 0,
            },
            filter_members: self.filter_members,
            member_errors: self.member_errors,
            non_pushed_down_window: self.non_pushed_down_window,
            non_pushed_down_grouping_sets: match state {
                CubePlanState::Wrapped => 0,
                CubePlanState::Unwrapped(_) => self.non_pushed_down_grouping_sets,
                CubePlanState::Wrapper => 0,
            },
            cube_members: self.cube_members,
            errors: self.errors,
            structure_points: self.structure_points,
            ast_size_outside_wrapper: match state {
                CubePlanState::Wrapped => 0,
                CubePlanState::Unwrapped(size) => *size,
                CubePlanState::Wrapper => 0,
            } + self.ast_size_outside_wrapper,
            empty_wrappers: match state {
                CubePlanState::Wrapped => 0,
                CubePlanState::Unwrapped(_) => 0,
                CubePlanState::Wrapper => {
                    if self.ast_size_inside_wrapper == 0 {
                        1
                    } else {
                        0
                    }
                }
            } + self.empty_wrappers,
            time_dimensions_used_as_dimensions: self.time_dimensions_used_as_dimensions,
            max_time_dimensions_granularity: self.max_time_dimensions_granularity,
            ungrouped_aggregates: match state {
                CubePlanState::Wrapped => 0,
                CubePlanState::Unwrapped(_) => {
                    if let LogicalPlanLanguage::Aggregate(_) = enode {
                        if self.ungrouped_nodes > 0 {
                            1
                        } else {
                            0
                        }
                    } else {
                        0
                    }
                }
                CubePlanState::Wrapper => 0,
            } + self.ungrouped_aggregates,
            unwrapped_subqueries: self.unwrapped_subqueries,
            wrapper_nodes: self.wrapper_nodes,
            wrapped_select_ungrouped_scan: self.wrapped_select_ungrouped_scan,
            cube_scan_nodes: self.cube_scan_nodes,
            ast_size_without_alias: self.ast_size_without_alias,
            ast_size: self.ast_size,
            ast_size_inside_wrapper: self.ast_size_inside_wrapper,
            ungrouped_nodes: self.ungrouped_nodes,
        }
    }
}

impl CostFunction<LogicalPlanLanguage> for BestCubePlan {
    type Cost = CubePlanCostAndState;
    fn cost<C>(&mut self, enode: &LogicalPlanLanguage, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        let table_scans = match enode {
            LogicalPlanLanguage::TableScan(_) => 1,
            _ => 0,
        };

        let non_detected_cube_scans = match enode {
            LogicalPlanLanguage::CubeScan(_) => 1,
            _ => 0,
        };

        let cube_scan_nodes = match enode {
            LogicalPlanLanguage::CubeScan(_) => 1,
            _ => 0,
        };

        let ast_size_outside_wrapper = match enode {
            LogicalPlanLanguage::Aggregate(_) => 1,
            LogicalPlanLanguage::Projection(_) => 1,
            LogicalPlanLanguage::Limit(_) => 1,
            LogicalPlanLanguage::Sort(_) => 1,
            LogicalPlanLanguage::Filter(_) => 1,
            LogicalPlanLanguage::Join(_) => 1,
            LogicalPlanLanguage::CrossJoin(_) => 1,
            LogicalPlanLanguage::Union(_) => 1,
            LogicalPlanLanguage::Window(_) => 1,
            LogicalPlanLanguage::Subquery(_) => 1,
            _ => 0,
        };

        let non_pushed_down_window = match enode {
            LogicalPlanLanguage::Window(_) => 1,
            _ => 0,
        };

        let non_pushed_down_grouping_sets = match enode {
            LogicalPlanLanguage::GroupingSetExpr(_) => 1,
            _ => 0,
        };

        let ast_size_inside_wrapper = match enode {
            LogicalPlanLanguage::WrappedSelect(_) => 1,
            _ => 0,
        };

        let wrapper_nodes = match enode {
            LogicalPlanLanguage::CubeScanWrapper(_) => 1,
            _ => 0,
        };

        let filter_members = match enode {
            LogicalPlanLanguage::FilterMember(_) => 1,
            _ => 0,
        };

        let filters = match enode {
            LogicalPlanLanguage::Filter(_) => 1,
            _ => 0,
        };

        let member_errors = match enode {
            LogicalPlanLanguage::MemberError(_) => 1,
            _ => 0,
        };

        let cube_members = match enode {
            LogicalPlanLanguage::Measure(_) => 1,
            LogicalPlanLanguage::Dimension(_) => 1,
            LogicalPlanLanguage::ChangeUser(_) => 1,
            LogicalPlanLanguage::VirtualField(_) => 1,
            LogicalPlanLanguage::LiteralMember(_) => 1,
            LogicalPlanLanguage::TimeDimensionGranularity(TimeDimensionGranularity(Some(_))) => 1,
            // MemberError must be present here as well in order to preserve error priority
            LogicalPlanLanguage::MemberError(_) => 1,
            _ => 0,
        };

        let this_replacers = match enode {
            LogicalPlanLanguage::OrderReplacer(_) => 1,
            LogicalPlanLanguage::MemberReplacer(_) => 1,
            LogicalPlanLanguage::FilterReplacer(_) => 1,
            LogicalPlanLanguage::TimeDimensionDateRangeReplacer(_) => 1,
            LogicalPlanLanguage::InnerAggregateSplitReplacer(_) => 1,
            LogicalPlanLanguage::OuterProjectionSplitReplacer(_) => 1,
            LogicalPlanLanguage::OuterAggregateSplitReplacer(_) => 1,
            LogicalPlanLanguage::GroupExprSplitReplacer(_) => 1,
            LogicalPlanLanguage::GroupAggregateSplitReplacer(_) => 1,
            LogicalPlanLanguage::MemberPushdownReplacer(_) => 1,
            LogicalPlanLanguage::EventNotification(_) => 1,
            LogicalPlanLanguage::MergedMembersReplacer(_) => 1,
            LogicalPlanLanguage::CaseExprReplacer(_) => 1,
            LogicalPlanLanguage::WrapperPushdownReplacer(_) => 1,
            LogicalPlanLanguage::WrapperPullupReplacer(_) => 1,
            LogicalPlanLanguage::FlattenPushdownReplacer(_) => 1,
            LogicalPlanLanguage::AggregateSplitPushDownReplacer(_) => 1,
            LogicalPlanLanguage::AggregateSplitPullUpReplacer(_) => 1,
            LogicalPlanLanguage::ProjectionSplitPushDownReplacer(_) => 1,
            LogicalPlanLanguage::ProjectionSplitPullUpReplacer(_) => 1,
            LogicalPlanLanguage::QueryParam(_) => 1,
            // Not really replacers but those should be deemed as mandatory rewrites and as soon as
            // there's always rewrite rule it's fine to have replacer cost.
            // Needs to be added as alias rewrite always more expensive than original function.
            LogicalPlanLanguage::ScalarUDFExprFun(ScalarUDFExprFun(fun))
                if fun.as_str() == "current_timestamp" =>
            {
                1
            }
            LogicalPlanLanguage::ScalarUDFExprFun(ScalarUDFExprFun(fun))
                if fun.as_str() == "localtimestamp" =>
            {
                1
            }
            _ => 0,
        };

        let time_dimensions_used_as_dimensions = match enode {
            LogicalPlanLanguage::DimensionName(DimensionName(name)) => {
                if let Some(dimension) = self.meta_context.find_dimension_with_name(name.clone()) {
                    if dimension.is_time() {
                        1
                    } else {
                        0
                    }
                } else {
                    0
                }
            }
            _ => 0,
        };

        let max_time_dimensions_granularity = match enode {
            LogicalPlanLanguage::TimeDimensionGranularity(TimeDimensionGranularity(Some(
                granularity,
            ))) => (8 - granularity_str_to_int_order(granularity, Some(false)).unwrap_or(0)) as i64,
            _ => 0,
        };

        let this_errors = match enode {
            LogicalPlanLanguage::MemberErrorPriority(MemberErrorPriority(priority)) => {
                (100 - priority) as i64
            }
            _ => 0,
        };

        let structure_points = match enode {
            // TODO needed to get rid of FilterOpFilters on upper level
            LogicalPlanLanguage::FilterOpFilters(_) => 1,
            LogicalPlanLanguage::Join(_) => 1,
            LogicalPlanLanguage::CrossJoin(_) => 1,
            _ => 0,
        };

        let ast_size_without_alias = match enode {
            LogicalPlanLanguage::AliasExpr(_) => 0,
            LogicalPlanLanguage::AliasExprAlias(_) => 0,
            _ => 1,
        };

        let ungrouped_nodes = match enode {
            LogicalPlanLanguage::CubeScanUngrouped(CubeScanUngrouped(true)) => 1,
            _ => 0,
        };

        let wrapped_select_ungrouped_scan = match enode {
            LogicalPlanLanguage::WrappedSelectUngroupedScan(WrappedSelectUngroupedScan(true)) => 1,
            _ => 0,
        };

        let unwrapped_subqueries = match enode {
            LogicalPlanLanguage::Subquery(_) => 1,
            _ => 0,
        };

        let initial_cost = CubePlanCostAndState {
            cost: CubePlanCost {
                replacers: this_replacers,
                table_scans,
                filters,
                filter_members,
                non_detected_cube_scans,
                member_errors,
                non_pushed_down_window,
                non_pushed_down_grouping_sets,
                cube_members,
                errors: this_errors,
                time_dimensions_used_as_dimensions,
                max_time_dimensions_granularity,
                structure_points,
                ungrouped_aggregates: 0,
                wrapper_nodes,
                wrapped_select_ungrouped_scan,
                empty_wrappers: 0,
                ast_size_outside_wrapper: 0,
                ast_size_inside_wrapper,
                cube_scan_nodes,
                ast_size_without_alias,
                ast_size: 1,
                ungrouped_nodes,
                unwrapped_subqueries,
            },
            state: match enode {
                LogicalPlanLanguage::CubeScanWrapped(CubeScanWrapped(true)) => {
                    CubePlanState::Wrapped
                }
                LogicalPlanLanguage::CubeScanWrapper(_) => CubePlanState::Wrapper,
                _ => CubePlanState::Unwrapped(ast_size_outside_wrapper),
            },
        };
        let res = enode
            .children()
            .iter()
            .fold(initial_cost.clone(), |cost, id| {
                let child = costs(*id);
                cost.add_child(&child)
            })
            .finalize(enode);
        res
    }
}
