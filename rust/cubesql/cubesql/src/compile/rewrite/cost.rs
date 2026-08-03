use std::{
    collections::HashMap, fmt::Debug, hash::Hash, marker::PhantomData, mem::take, sync::Arc,
};

use crate::{
    compile::rewrite::{
        rules::utils::granularity_str_to_int_order, CubeScanLimit, CubeScanUngrouped,
        CubeScanWrapped, DimensionName, LogicalPlanLanguage, MemberErrorPriority, ScalarUDFExprFun,
        TimeDimensionGranularity, WrappedSelectLimit, WrappedSelectPushToCube,
        WrappedSelectUngroupedScan,
    },
    transport::{MetaContext, V1CubeMetaDimensionExt},
};
use egg::{Analysis, EClass, EGraph, Id, Language, RecExpr};
use indexmap::IndexSet;

#[derive(Debug)]
pub struct BestCubePlan {
    meta_context: Arc<MetaContext>,
    penalize_post_processing: bool,
}

impl BestCubePlan {
    pub fn new(meta_context: Arc<MetaContext>, penalize_post_processing: bool) -> Self {
        Self {
            meta_context,
            penalize_post_processing,
        }
    }

    pub fn initial_cost(&self, enode: &LogicalPlanLanguage) -> CubePlanCost {
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

        let joins = match enode {
            LogicalPlanLanguage::Join(_) => 1,
            LogicalPlanLanguage::CrossJoin(_) => 1,
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

        let zero_members_wrapper = match enode {
            LogicalPlanLanguage::WrappedSelect(_) => 1,
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
            LogicalPlanLanguage::FilterSimplifyPushDownReplacer(_) => 1,
            LogicalPlanLanguage::FilterSimplifyPullUpReplacer(_) => 1,
            LogicalPlanLanguage::TimeDimensionDateRangeReplacer(_) => 1,
            LogicalPlanLanguage::InnerAggregateSplitReplacer(_) => 1,
            LogicalPlanLanguage::OuterProjectionSplitReplacer(_) => 1,
            LogicalPlanLanguage::OuterAggregateSplitReplacer(_) => 1,
            LogicalPlanLanguage::GroupExprSplitReplacer(_) => 1,
            LogicalPlanLanguage::GroupAggregateSplitReplacer(_) => 1,
            LogicalPlanLanguage::MemberPushdownReplacer(_) => 1,
            LogicalPlanLanguage::EventNotification(_) => 1,
            LogicalPlanLanguage::CaseExprReplacer(_) => 1,
            LogicalPlanLanguage::WrapperPushdownReplacer(_) => 1,
            LogicalPlanLanguage::WrapperPullupReplacer(_) => 1,
            LogicalPlanLanguage::FlattenPushdownReplacer(_) => 1,
            LogicalPlanLanguage::AggregateSplitPushDownReplacer(_) => 1,
            LogicalPlanLanguage::AggregateSplitPullUpReplacer(_) => 1,
            LogicalPlanLanguage::ProjectionSplitPushDownReplacer(_) => 1,
            LogicalPlanLanguage::ProjectionSplitPullUpReplacer(_) => 1,
            LogicalPlanLanguage::QueryParam(_) => 1,
            LogicalPlanLanguage::JoinCheckStage(_) => 1,
            LogicalPlanLanguage::JoinCheckPushDown(_) => 1,
            LogicalPlanLanguage::JoinCheckPullUp(_) => 1,
            LogicalPlanLanguage::MultiFactJoinWrapper(_) => 1,
            LogicalPlanLanguage::SortProjectionPushdownReplacer(_) => 1,
            LogicalPlanLanguage::SortProjectionPullupReplacer(_) => 1,
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
                if let Some(dimension) = self.meta_context.find_dimension_with_name(name) {
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

        let wrapped_select_non_push_to_cube = match enode {
            LogicalPlanLanguage::WrappedSelectPushToCube(WrappedSelectPushToCube(false)) => 1,
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

        CubePlanCost {
            replacers: this_replacers,
            // Will be filled in finalize
            penalized_ast_size_outside_wrapper: 0,
            table_scans,
            filters,
            filter_members,
            non_detected_cube_scans,
            member_errors,
            non_pushed_down_window,
            non_pushed_down_grouping_sets,
            // Will be filled in finalize
            non_pushed_down_limit_sort: 0,
            zero_members_wrapper,
            cube_members,
            errors: this_errors,
            time_dimensions_used_as_dimensions,
            max_time_dimensions_granularity,
            structure_points,
            ungrouped_aggregates: 0,
            wrapper_nodes,
            joins,
            wrapped_select_non_push_to_cube,
            wrapped_select_ungrouped_scan,
            empty_wrappers: 0,
            ast_size_outside_wrapper: 0,
            ast_size_inside_wrapper,
            cube_scan_nodes,
            ast_size_without_alias,
            ast_size: 1,
            ungrouped_nodes,
            unwrapped_subqueries,
            // Will be filled in finalize
            truncated_post_processing_scans: 0,
        }
    }
}

#[derive(Clone, Copy)]
pub struct CubePlanCostOptions {
    penalize_post_processing: bool,
}

/// This cost struct maintains following structural relationships:
/// - `replacers` > other nodes - having replacers in structure means not finished processing
/// - `penalized_ast_size_outside_wrapper` > other nodes - this is used to force "no post processing" mode, only CubeScan and CubeScanWrapped are expected as result
/// - `table_scans` > other nodes - having table scan means not detected cube scan
/// - `empty_wrappers` > `non_detected_cube_scans` - we don't want empty wrapper to hide non detected cube scan errors
/// - `non_detected_cube_scans` > other nodes - minimize cube scans without members
/// - `filters` > `filter_members` - optimize for push down of filters
/// - `zero_members_wrapper` > `filter_members` - prefer CubeScan(filters) to WrappedSelect(CubeScan(*), filters)
/// - `filter_members` > `cube_members` - optimize for `inDateRange` filter push down to time dimension
/// - `member_errors` > `cube_members` - extra cube members may be required (e.g. CASE)
/// - `member_errors` > `wrapper_nodes` - use SQL push down where possible if cube scan can't be detected
/// - `non_pushed_down_window` > `wrapper_nodes` - prefer to always push down window functions
/// - `non_pushed_down_limit_sort` > `wrapper_nodes` - prefer to always push down limit-sort expressions
/// - `wrapped_select_non_push_to_cube` > `wrapped_select_ungrouped_scan` - otherwise cost would prefer any aggregation, even non-push-to-Cube
/// - `truncated_post_processing_scans` > `wrapper_nodes` - post-processing truncated data is
///   wrong, not just slow, so it outranks the tiers that trade post-processing against wrappers.
///   It has to stay *below* `table_scans` and `non_detected_cube_scans` though: a plan that never
///   detected a cube scan has nothing to truncate and would otherwise win on this tier.
/// - match errors by priority - optimize for more specific errors
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct CubePlanCost {
    replacers: i64,
    penalized_ast_size_outside_wrapper: usize,
    table_scans: i64,
    empty_wrappers: i64,
    non_detected_cube_scans: i64,
    unwrapped_subqueries: usize,
    member_errors: i64,
    ungrouped_aggregates: usize,
    // TODO if pre-aggregation can be used for window functions, then it'd be suboptimal
    non_pushed_down_window: i64,
    non_pushed_down_grouping_sets: i64,
    non_pushed_down_limit_sort: i64,
    joins: usize,
    /// Pushed-down subtrees whose result is truncated to `CUBEJS_DB_QUERY_LIMIT` rows and then
    /// post-processed in memory - the plan shape that silently returns wrong results. Counted
    /// only when the caller asked for it, see [`CubePlanTopDownState::max_intermediate_rows`].
    truncated_post_processing_scans: usize,
    wrapper_nodes: i64,
    ast_size_outside_wrapper: usize,
    wrapped_select_non_push_to_cube: usize,
    wrapped_select_ungrouped_scan: usize,
    filters: i64,
    structure_points: i64,
    // This is separate from both non_detected_cube_scans and cube_members
    // Because it's ok to use all members inside wrapper (so non_detected_cube_scans would be zero)
    // And we want to select representation with less members
    // But only when members are present!
    zero_members_wrapper: i64,
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

impl CubePlanCost {
    pub fn truncated_post_processing_scans(&self) -> usize {
        self.truncated_post_processing_scans
    }
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum CubePlanState {
    Wrapped,
    Unwrapped(usize),
    Wrapper,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum SortState {
    None,
    Current,
    DirectChild,
}

impl CubePlanCost {
    pub fn add_child(&self, other: &Self) -> Self {
        Self {
            replacers: self.replacers + other.replacers,
            // Will be filled in finalize
            penalized_ast_size_outside_wrapper: 0,
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
            non_pushed_down_limit_sort: self.non_pushed_down_limit_sort
                + other.non_pushed_down_limit_sort,
            member_errors: self.member_errors + other.member_errors,
            zero_members_wrapper: (if other.cube_members == 0 {
                self.zero_members_wrapper
            } else {
                0
            }) + other.zero_members_wrapper,
            cube_members: self.cube_members + other.cube_members,
            errors: self.errors + other.errors,
            structure_points: self.structure_points + other.structure_points,
            joins: self.joins + other.joins,
            empty_wrappers: self.empty_wrappers + other.empty_wrappers,
            ast_size_outside_wrapper: self.ast_size_outside_wrapper
                + other.ast_size_outside_wrapper,
            ungrouped_aggregates: self.ungrouped_aggregates + other.ungrouped_aggregates,
            wrapper_nodes: self.wrapper_nodes + other.wrapper_nodes,
            wrapped_select_non_push_to_cube: self.wrapped_select_non_push_to_cube
                + other.wrapped_select_non_push_to_cube,
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
            truncated_post_processing_scans: self.truncated_post_processing_scans
                + other.truncated_post_processing_scans,
        }
    }

    pub fn finalize(
        &self,
        top_down_state: &CubePlanTopDownState,
        enode: &LogicalPlanLanguage,
        options: CubePlanCostOptions,
    ) -> Self {
        let state = &top_down_state.wrapped;
        let sort_state = &top_down_state.limit;
        let ast_size_outside_wrapper = match state {
            CubePlanState::Wrapped => 0,
            CubePlanState::Unwrapped(size) => *size,
            CubePlanState::Wrapper => 0,
        } + self.ast_size_outside_wrapper;
        let penalized_ast_size_outside_wrapper = if options.penalize_post_processing {
            ast_size_outside_wrapper
        } else {
            0
        };

        Self {
            replacers: self.replacers,
            penalized_ast_size_outside_wrapper,
            // Only ever set on the root of a pushed-down subtree, so this counts boundaries
            // rather than every node above one.
            truncated_post_processing_scans: self.truncated_post_processing_scans
                + top_down_state.truncated_post_processing as usize,
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
            non_pushed_down_limit_sort: match sort_state {
                SortState::Current => self.non_pushed_down_limit_sort + 1,
                _ => self.non_pushed_down_limit_sort,
            },
            // Don't track state here: we want representation that have fewer wrappers with zero members _in total_
            zero_members_wrapper: self.zero_members_wrapper,
            cube_members: self.cube_members,
            errors: self.errors,
            structure_points: self.structure_points,
            joins: self.joins,
            ast_size_outside_wrapper,
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
            wrapped_select_non_push_to_cube: self.wrapped_select_non_push_to_cube,
            wrapped_select_ungrouped_scan: self.wrapped_select_ungrouped_scan,
            cube_scan_nodes: self.cube_scan_nodes,
            ast_size_without_alias: self.ast_size_without_alias,
            ast_size: self.ast_size,
            ast_size_inside_wrapper: self.ast_size_inside_wrapper,
            ungrouped_nodes: self.ungrouped_nodes,
        }
    }
}

pub trait TopDownCost: Clone + Debug + PartialOrd {
    fn add(&self, other: &Self) -> Self;
}

pub trait TopDownState<L>: Clone + Debug + Eq + Hash
where
    L: Language,
{
    /// Transforms the current state based on node's contents.
    fn transform<A>(&self, node: &L, egraph: &EGraph<L, A>) -> Self
    where
        A: Analysis<L>;
}

/// Simple implementation of TopDownState for lack of state.
impl<L> TopDownState<L> for ()
where
    L: Language,
{
    fn transform<A>(&self, _: &L, _: &EGraph<L, A>) -> Self
    where
        A: Analysis<L>,
    {
        ()
    }
}

pub trait TopDownCostFunction<L, S, C>: Debug
where
    L: Language,
    S: TopDownState<L>,
    C: TopDownCost,
{
    /// Returns the cost for the current node.
    fn cost(&self, node: &L) -> C;

    // Finalize the cost based on node and state.
    fn finalize(&self, cost: C, node: &L, state: &S) -> C;
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct IdWithState<L, S>
where
    L: Language,
    S: TopDownState<L>,
{
    id: Id,
    state: Arc<S>,
    phantom: PhantomData<L>,
}

impl<L, S> IdWithState<L, S>
where
    L: Language,
    S: TopDownState<L>,
{
    pub fn new(id: Id, state: Arc<S>) -> Self {
        Self {
            id,
            state,
            phantom: PhantomData,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TopDownExtractor<'a, L, A, C, S, CF>
where
    L: Language,
    A: Analysis<L>,
    C: TopDownCost,
    S: TopDownState<L>,
    CF: TopDownCostFunction<L, S, C>,
{
    egraph: &'a EGraph<L, A>,
    // Caches results. `None` for nodes in progress to prevent recursion
    extract_map: HashMap<IdWithState<L, S>, Option<(usize, C)>>,
    has_deep_recursion: bool,
    // Cache for second pass to calculate recursive nodes cost.
    extract_map_recursive_cache: Option<HashMap<IdWithState<L, S>, Option<(usize, C)>>>,
    cost_fn: Arc<CF>,
    root_state: Arc<S>,
}

impl<'a, L, A, C, S, CF> TopDownExtractor<'a, L, A, C, S, CF>
where
    L: Language,
    A: Analysis<L>,
    C: TopDownCost,
    S: TopDownState<L>,
    CF: TopDownCostFunction<L, S, C>,
{
    pub fn new(egraph: &'a EGraph<L, A>, cost_fn: CF, root_state: S) -> Self {
        Self {
            egraph,
            extract_map: HashMap::new(),
            has_deep_recursion: false,
            extract_map_recursive_cache: None,
            cost_fn: Arc::new(cost_fn),
            root_state: Arc::new(root_state),
        }
    }

    /// Returns cost and path for best plan for provided root eclass.
    ///
    /// If all nodes happen to be recursive, returns `None`.
    ///
    /// If there were any nodes with deep recursion, the cost is calculated in two passes;
    /// the second pass fetches the cost from the extract map obtained on the first pass
    /// for recursive nodes only.
    pub fn find_best(&mut self, root: Id) -> Option<(C, RecExpr<L>)> {
        let mut cost = self.extract(root, Arc::clone(&self.root_state))?;
        if self.has_deep_recursion {
            self.extract_map_recursive_cache = Some(take(&mut self.extract_map));
            cost = self.extract(root, Arc::clone(&self.root_state))?;
        }

        let root_id_with_state = IdWithState::new(root, Arc::clone(&self.root_state));
        let root_node = self.choose_node(&root_id_with_state)?;
        let recexpr =
            self.build_recexpr(&root_node, root_id_with_state.state, |id_with_state| {
                self.choose_node(id_with_state)
            })?;
        Some((cost, recexpr))
    }

    /// Recursively extracts the cost of each node in the eclass
    /// and returns cost of the node with least cost based on the passed state,
    /// caching the cost together with node index inside eclass in `extract_map`.
    /// If `extract_map_recursive_cache` is available, fetches the costs
    /// of deep recursion nodes from there.
    ///
    /// Yields `None` if eclass is already in progress
    /// or all its nodes happen to be recursive.
    fn extract(&mut self, eclass: Id, state: Arc<S>) -> Option<C> {
        let id_with_state = IdWithState::new(eclass, state);
        if let Some(cached_index_and_cost) = self.extract_map.get(&id_with_state) {
            // If the cost has been computed, return it
            if let Some((_, cached_cost)) = cached_index_and_cost {
                // TODO: avoid cloning here?
                return Some(cached_cost.clone());
            }

            // If the cost is recursive, fetch from recursive cache if available
            if let Some(extract_map_recursive_cache) = &self.extract_map_recursive_cache {
                if let Some(Some((_, cached_cost))) =
                    extract_map_recursive_cache.get(&id_with_state)
                {
                    // TODO: avoid cloning here?
                    return Some(cached_cost.clone());
                }
            }

            // Otherwise, mark this extractor as having deep recursion
            self.has_deep_recursion = true;
            return None;
        }

        // Mark this eclass as in progress
        self.extract_map.insert(id_with_state.clone(), None);

        // Compute the cost of each node, take the minimum
        let mut min_index = None;
        let mut min_cost = None;
        'nodes: for (index, node) in self.egraph[eclass].nodes.iter().enumerate() {
            // Compute the cost of this node
            let this_node_cost = self.cost_fn.cost(node);

            // Get state for this node and its children
            let new_state = Arc::new(id_with_state.state.transform(node, self.egraph));

            // Recursively get children cost
            let mut total_node_cost = this_node_cost;
            for child in node.children() {
                // If a child is recursive to self, skip this node, as it will never compute
                // the cost
                if child == &eclass {
                    continue 'nodes;
                }

                let Some(child_cost) = self.extract(*child, Arc::clone(&new_state)) else {
                    // This path is inevitably recursive, try the next node
                    continue 'nodes;
                };
                total_node_cost = total_node_cost.add(&child_cost);
            }
            total_node_cost = self.cost_fn.finalize(total_node_cost, node, &new_state);

            // Now that we've finalized the cost, check if it's lower than the minimum
            if let Some(min_cost) = &min_cost {
                if &total_node_cost > min_cost {
                    continue;
                }
            }

            min_index = Some(index);
            min_cost = Some(total_node_cost);
        }

        let (Some(min_index), Some(min_cost)) = (min_index, min_cost) else {
            // All nodes were recursive
            self.extract_map.remove(&id_with_state);
            return None;
        };

        self.extract_map
            .insert(id_with_state, Some((min_index, min_cost.clone())));
        Some(min_cost)
    }

    /// A custom version of [`egg::Language::build_recexpr`], accepting state
    /// in addition to [`egg::Id`].
    fn build_recexpr<F>(&self, node: &L, start_state: Arc<S>, get_node: F) -> Option<RecExpr<L>>
    where
        F: Fn(&IdWithState<L, S>) -> Option<L>,
    {
        let state = Arc::new(start_state.transform(node, self.egraph));
        let mut set = IndexSet::<L>::default();
        let mut ids = HashMap::<IdWithState<L, S>, Id>::default();
        let mut todo = node
            .children()
            .iter()
            .map(|id| IdWithState::new(*id, Arc::clone(&state)))
            .collect::<Vec<_>>();

        while let Some(id_with_state) = todo.last().cloned() {
            if ids.contains_key(&id_with_state) {
                todo.pop();
                continue;
            }

            let node = get_node(&id_with_state)?;
            let node_state = Arc::new(id_with_state.state.transform(&node, self.egraph));

            // Check to see if we can do this node yet
            let mut ids_has_all_children = true;
            for child in node.children() {
                let child_id_with_state = IdWithState::new(*child, Arc::clone(&node_state));
                if !ids.contains_key(&child_id_with_state) {
                    ids_has_all_children = false;
                    todo.push(child_id_with_state);
                }
            }

            // All children are processed, so we can lookup this node safely
            if ids_has_all_children {
                let node = node.map_children(|id| {
                    let id_with_state = IdWithState::new(id, Arc::clone(&node_state));
                    ids[&id_with_state]
                });
                let (new_id, _) = set.insert_full(node);
                ids.insert(id_with_state, Id::from(new_id));
                todo.pop();
            }
        }

        // Finally, add the root node and create the expression
        let mut nodes = set.into_iter().collect::<Vec<_>>();
        nodes.push(node.clone().map_children(|id| {
            let id_with_state = IdWithState::new(id, Arc::clone(&state));
            ids[&id_with_state]
        }));
        Some(RecExpr::from(nodes))
    }

    fn choose_node(&self, id_with_state: &IdWithState<L, S>) -> Option<L> {
        let index = *self
            .extract_map
            .get(&id_with_state)?
            .as_ref()
            .map(|(index, _)| index)?;
        Some(self.egraph[id_with_state.id].nodes[index].clone())
    }
}

impl TopDownCost for CubePlanCost {
    fn add(&self, other: &Self) -> Self {
        self.add_child(other)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CubePlanTopDownState {
    wrapped: CubePlanState,
    limit: SortState,
    /// Whether any ancestor of the current node post-processes in memory. Reset upon entering a
    /// pushed-down subtree, where there is no in-memory work left to do.
    post_processing_above: bool,
    /// Set when the current node is the root of a pushed-down subtree - a `CubeScanWrapper`, or a
    /// `CubeScan` that is not wrapped - that feeds post-processing above it and is not bounded by
    /// a limit fitting into `max_intermediate_rows`. Such a subtree is silently truncated to that
    /// many rows at execution time (see `CubeScanExecutionPlan::execute` and
    /// `CubeScanWrapperNode::set_max_limit_for_node`), so the post-processing above it runs on
    /// partial data and can return a wrong answer with no error.
    truncated_post_processing: bool,
    /// Row count a pushed-down subtree is truncated to, i.e. `CUBEJS_DB_QUERY_LIMIT`.
    /// `None`, the default, turns off `truncated_post_processing` entirely.
    max_intermediate_rows: Option<usize>,
}

impl CubePlanTopDownState {
    pub fn new() -> Self {
        Self {
            wrapped: CubePlanState::Unwrapped(0),
            limit: SortState::None,
            post_processing_above: false,
            truncated_post_processing: false,
            max_intermediate_rows: None,
        }
    }

    /// Starts tracking pushed-down subtrees that get truncated to `max_intermediate_rows` before
    /// being post-processed, which makes [`CubePlanCost::truncated_post_processing_scans`] count
    /// them. `None` leaves the tracking off, for when nothing asked for the check or streaming
    /// is on and there is no truncation to detect.
    pub fn with_max_intermediate_rows(&mut self, max_intermediate_rows: Option<usize>) {
        self.max_intermediate_rows = max_intermediate_rows;
    }

    pub fn is_wrapped<A>(
        &self,
        node: &LogicalPlanLanguage,
        egraph: &EGraph<LogicalPlanLanguage, A>,
    ) -> bool
    where
        A: Analysis<LogicalPlanLanguage>,
    {
        let LogicalPlanLanguage::CubeScan(cube_scan) = node else {
            return false;
        };
        let wrapped_index = 8;
        let wrapped_id = cube_scan[wrapped_index];
        for node in &egraph[wrapped_id].nodes {
            if !matches!(
                node,
                LogicalPlanLanguage::CubeScanWrapped(CubeScanWrapped(true))
            ) {
                return false;
            }
        }
        return true;
    }

    /// Whether the pushed-down subtree rooted at `node` returns at most `max_rows` rows.
    ///
    /// An eclass can hold several alternatives, and extraction has not picked one yet, so this
    /// only reports `true` when every alternative is bounded. Anything it can't prove bounded -
    /// including a wrapper input that isn't a plain `WrappedSelect` - counts as unbounded, which
    /// biases towards pushing the query down rather than towards post-processing truncated data.
    fn is_bounded<A>(
        node: &LogicalPlanLanguage,
        max_rows: usize,
        egraph: &EGraph<LogicalPlanLanguage, A>,
    ) -> bool
    where
        A: Analysis<LogicalPlanLanguage>,
    {
        match node {
            LogicalPlanLanguage::CubeScan(cube_scan) => {
                let limit_id = cube_scan[4];
                Self::all_nodes(&egraph[limit_id], |node| {
                    matches!(
                        node,
                        LogicalPlanLanguage::CubeScanLimit(CubeScanLimit(Some(limit)))
                            if *limit <= max_rows
                    )
                })
            }
            // Whatever tops the wrapped plan decides how many rows come back, which is what
            // `CubeScanWrapperNode::set_max_limit_for_node` clamps. Usually that's a
            // `WrappedSelect`, whose limit lands in the generated SQL while the `CubeScan`
            // beneath it stays limitless - but a wrapper straight over a `CubeScan` is just as
            // real, and there the scan's own limit is the one that counts.
            LogicalPlanLanguage::CubeScanWrapper(cube_scan_wrapper) => {
                let input_id = cube_scan_wrapper[0];
                Self::all_nodes(&egraph[input_id], |node| match node {
                    LogicalPlanLanguage::WrappedSelect(wrapped_select) => {
                        let limit_id = wrapped_select[10];
                        Self::all_nodes(&egraph[limit_id], |node| {
                            matches!(
                                node,
                                LogicalPlanLanguage::WrappedSelectLimit(WrappedSelectLimit(Some(
                                    limit
                                ))) if *limit <= max_rows
                            )
                        })
                    }
                    LogicalPlanLanguage::CubeScan(_) => Self::is_bounded(node, max_rows, egraph),
                    _ => false,
                })
            }
            _ => true,
        }
    }

    fn all_nodes<D>(
        eclass: &EClass<LogicalPlanLanguage, D>,
        predicate: impl Fn(&LogicalPlanLanguage) -> bool,
    ) -> bool {
        !eclass.nodes.is_empty() && eclass.nodes.iter().all(predicate)
    }

    /// Whether `node` is an operator that does its work in memory, and therefore post-processes
    /// whatever is under it when it sits outside a wrapper.
    ///
    /// Deliberately not the node list behind `ast_size_outside_wrapper`. That one exists to
    /// weight a cost, where missing a node only makes a plan look slightly cheaper than it is;
    /// this one decides whether a query is rejected, where missing a node means accepting a
    /// query the flag promised to reject and handing back a wrong answer. `Distinct` is the
    /// case in point: it is absent there, and `Distinct` over a wrapper is exactly the shape
    /// that would slip through.
    ///
    /// Keep in sync with the `LogicalPlan` variants handled in
    /// [`crate::compile::rewrite::converter`] - anything that survives to execution and is not
    /// pushed down belongs here.
    fn is_post_processing(node: &LogicalPlanLanguage) -> bool {
        matches!(
            node,
            LogicalPlanLanguage::Aggregate(_)
                | LogicalPlanLanguage::CrossJoin(_)
                | LogicalPlanLanguage::Distinct(_)
                | LogicalPlanLanguage::Filter(_)
                | LogicalPlanLanguage::Join(_)
                | LogicalPlanLanguage::Limit(_)
                | LogicalPlanLanguage::Projection(_)
                | LogicalPlanLanguage::Repartition(_)
                | LogicalPlanLanguage::Sort(_)
                | LogicalPlanLanguage::Subquery(_)
                | LogicalPlanLanguage::TableUDFs(_)
                | LogicalPlanLanguage::Union(_)
                | LogicalPlanLanguage::Window(_)
        )
    }
}

impl TopDownState<LogicalPlanLanguage> for CubePlanTopDownState {
    fn transform<A>(
        &self,
        node: &LogicalPlanLanguage,
        egraph: &EGraph<LogicalPlanLanguage, A>,
    ) -> Self
    where
        A: Analysis<LogicalPlanLanguage>,
    {
        let wrapped = match node {
            LogicalPlanLanguage::CubeScanWrapper(_) => CubePlanState::Wrapper,
            _ if self.wrapped == CubePlanState::Wrapped => CubePlanState::Wrapped,
            LogicalPlanLanguage::CubeScan(_) if self.is_wrapped(node, egraph) => {
                CubePlanState::Wrapped
            }
            _ => {
                let ast_size_outside_wrapper = match node {
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
                CubePlanState::Unwrapped(ast_size_outside_wrapper)
            }
        };

        let limit = match node {
            LogicalPlanLanguage::Limit(_) => SortState::DirectChild,
            LogicalPlanLanguage::Sort(_) if self.limit == SortState::DirectChild => {
                SortState::Current
            }
            _ => SortState::None,
        };

        // Only in-memory nodes count, so this stops accumulating once inside a pushed-down
        // subtree.
        //
        // Kept pinned to `false` when the tracking is off: this is part of the extractor's cache
        // key, and a value that varies by position in the plan would split every eclass into
        // several entries for no gain on the default path.
        let post_processing_above = self.max_intermediate_rows.is_some()
            && match wrapped {
                CubePlanState::Wrapped | CubePlanState::Wrapper => false,
                CubePlanState::Unwrapped(_) => {
                    self.post_processing_above || Self::is_post_processing(node)
                }
            };

        // Guarded on `self.post_processing_above` rather than the value just computed: what
        // matters is what sits *above* this node, and the root of a pushed-down subtree never
        // post-processes anything itself.
        let truncated_post_processing = match self.max_intermediate_rows {
            Some(max_rows) if self.post_processing_above => match node {
                LogicalPlanLanguage::CubeScanWrapper(_) => {
                    !Self::is_bounded(node, max_rows, egraph)
                }
                // A wrapped `CubeScan` is not the root of the pushed-down subtree, the
                // `CubeScanWrapper` above it is; counting both would report every pushdown plan
                // as truncated.
                LogicalPlanLanguage::CubeScan(_) if !self.is_wrapped(node, egraph) => {
                    !Self::is_bounded(node, max_rows, egraph)
                }
                _ => false,
            },
            _ => false,
        };

        Self {
            wrapped,
            limit,
            post_processing_above,
            truncated_post_processing,
            max_intermediate_rows: self.max_intermediate_rows,
        }
    }
}

impl TopDownCostFunction<LogicalPlanLanguage, CubePlanTopDownState, CubePlanCost> for BestCubePlan {
    fn cost(&self, node: &LogicalPlanLanguage) -> CubePlanCost {
        self.initial_cost(node)
    }

    fn finalize(
        &self,
        cost: CubePlanCost,
        node: &LogicalPlanLanguage,
        state: &CubePlanTopDownState,
    ) -> CubePlanCost {
        CubePlanCost::finalize(
            &cost,
            state,
            node,
            CubePlanCostOptions {
                penalize_post_processing: self.penalize_post_processing,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_ROWS: usize = 50_000;

    /// A `CubeScan` with `limit`. Every child slot points at the limit eclass, which is fine
    /// because [`CubePlanTopDownState::is_bounded`] only ever reads the limit.
    fn add_cube_scan(egraph: &mut EGraph<LogicalPlanLanguage, ()>, limit: Option<usize>) -> Id {
        let limit_id = egraph.add(LogicalPlanLanguage::CubeScanLimit(CubeScanLimit(limit)));
        egraph.add(LogicalPlanLanguage::CubeScan([limit_id; 11]))
    }

    fn add_wrapped_select(
        egraph: &mut EGraph<LogicalPlanLanguage, ()>,
        limit: Option<usize>,
    ) -> Id {
        let limit_id = egraph.add(LogicalPlanLanguage::WrappedSelectLimit(WrappedSelectLimit(
            limit,
        )));
        let mut children = [limit_id; 17];
        children[10] = limit_id;
        egraph.add(LogicalPlanLanguage::WrappedSelect(children))
    }

    fn add_wrapper(egraph: &mut EGraph<LogicalPlanLanguage, ()>, input: Id) -> Id {
        egraph.add(LogicalPlanLanguage::CubeScanWrapper([input; 2]))
    }

    fn is_bounded(egraph: &EGraph<LogicalPlanLanguage, ()>, id: Id) -> bool {
        let node = egraph[id].nodes[0].clone();
        CubePlanTopDownState::is_bounded(&node, MAX_ROWS, egraph)
    }

    #[test]
    fn test_is_bounded_cube_scan() {
        let mut egraph = EGraph::<LogicalPlanLanguage, ()>::default();

        let bounded = add_cube_scan(&mut egraph, Some(100));
        let unbounded = add_cube_scan(&mut egraph, None);
        let over_limit = add_cube_scan(&mut egraph, Some(MAX_ROWS + 1));

        assert!(is_bounded(&egraph, bounded));
        assert!(!is_bounded(&egraph, unbounded));
        // Clamped down to `MAX_ROWS` at execution time, so the rows above it are lost.
        assert!(!is_bounded(&egraph, over_limit));
    }

    /// A wrapper can sit straight on a `CubeScan` instead of on a `WrappedSelect` -
    /// `CubeScanWrapperNode::set_max_limit_for_node` handles both - and then it is the scan's own
    /// limit that bounds the result. Reporting those as unbounded rejects queries that are
    /// perfectly safe, purely because of which representation extraction landed on.
    #[test]
    fn test_is_bounded_wrapper_over_cube_scan() {
        let mut egraph = EGraph::<LogicalPlanLanguage, ()>::default();

        let bounded = add_cube_scan(&mut egraph, Some(100));
        let bounded = add_wrapper(&mut egraph, bounded);
        let unbounded = add_cube_scan(&mut egraph, None);
        let unbounded = add_wrapper(&mut egraph, unbounded);

        assert!(is_bounded(&egraph, bounded));
        assert!(!is_bounded(&egraph, unbounded));
    }

    #[test]
    fn test_is_bounded_wrapper_over_wrapped_select() {
        let mut egraph = EGraph::<LogicalPlanLanguage, ()>::default();

        let bounded = add_wrapped_select(&mut egraph, Some(100));
        let bounded = add_wrapper(&mut egraph, bounded);
        let unbounded = add_wrapped_select(&mut egraph, None);
        let unbounded = add_wrapper(&mut egraph, unbounded);

        assert!(is_bounded(&egraph, bounded));
        assert!(!is_bounded(&egraph, unbounded));
    }

    /// Extraction has not picked an alternative yet, so a wrapper input that could still turn
    /// out to be something unrecognised stays unbounded.
    #[test]
    fn test_is_bounded_wrapper_over_unknown_input_is_conservative() {
        let mut egraph = EGraph::<LogicalPlanLanguage, ()>::default();

        let scan = add_cube_scan(&mut egraph, Some(100));
        let wrapper = add_wrapper(&mut egraph, scan);
        let other = egraph.add(LogicalPlanLanguage::EmptyRelation([scan; 3]));
        egraph.union(scan, other);
        egraph.rebuild();

        assert!(!is_bounded(&egraph, wrapper));
    }
}
