use std::{collections::HashMap, fmt::Debug, hash::Hash, marker::PhantomData, sync::Arc};

use crate::{
    compile::rewrite::{
        rules::utils::granularity_str_to_int_order, CubeScanUngrouped, CubeScanWrapped,
        DimensionName, LogicalPlanLanguage, MemberErrorPriority, ScalarUDFExprFun,
        TimeDimensionGranularity, WrappedSelectPushToCube, WrappedSelectUngroupedScan,
    },
    transport::{MetaContext, V1CubeMetaDimensionExt},
};
use egg::{Analysis, EGraph, Id, Language, RecExpr};
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

        let non_pushed_down_limit_sort = match enode {
            LogicalPlanLanguage::Sort(_) => 1,
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
            LogicalPlanLanguage::JoinCheckStage(_) => 1,
            LogicalPlanLanguage::JoinCheckPushDown(_) => 1,
            LogicalPlanLanguage::JoinCheckPullUp(_) => 1,
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
            non_pushed_down_limit_sort,
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
        }
    }

    pub fn finalize(
        &self,
        state: &CubePlanState,
        sort_state: &SortState,
        enode: &LogicalPlanLanguage,
        options: CubePlanCostOptions,
    ) -> Self {
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
                SortState::DirectChild => self.non_pushed_down_limit_sort,
                SortState::Current => self.non_pushed_down_limit_sort,
                _ => 0,
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
            cost_fn: Arc::new(cost_fn),
            root_state: Arc::new(root_state),
        }
    }

    /// Returns cost and path for best plan for provided root eclass.
    ///
    /// If all nodes happen to be recursive, returns `None`.
    pub fn find_best(&mut self, root: Id) -> Option<(C, RecExpr<L>)> {
        let cost = self.extract(root, Arc::clone(&self.root_state))?;
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
    ///
    /// Yields `None` if eclass is already in progress
    /// or all its nodes happen to be recursive.
    fn extract(&mut self, eclass: Id, state: Arc<S>) -> Option<C> {
        let id_with_state = IdWithState::new(eclass, state);
        if let Some(cached_index_and_cost) = self.extract_map.get(&id_with_state) {
            // TODO: avoid cloning here?
            return cached_index_and_cost.as_ref().map(|(_, cost)| cost.clone());
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
}

impl CubePlanTopDownState {
    pub fn new() -> Self {
        Self {
            wrapped: CubePlanState::Unwrapped(0),
            limit: SortState::None,
        }
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

        Self { wrapped, limit }
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
            &state.wrapped,
            &state.limit,
            node,
            CubePlanCostOptions {
                penalize_post_processing: self.penalize_post_processing,
            },
        )
    }
}
