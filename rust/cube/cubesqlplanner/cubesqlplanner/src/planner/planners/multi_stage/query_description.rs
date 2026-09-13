use super::{MultiStageLeafMemberType, MultiStageMember, MultiStageMemberType};
use crate::logical_plan::LogicalSchema;
use crate::planner::collectors::has_multi_stage_members;
use crate::planner::{MemberSymbol, QueryProperties};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::cell::RefCell;
use std::rc::Rc;

/// One CTE in the multi-stage tree as the planner sees it: the
/// `member` rendered, the `state` (`QueryProperties` snapshot for
/// this CTE's scope), the input CTEs it depends on, and the alias
/// it will be referenced by.
pub struct MultiStageQueryDescription {
    member: Rc<MultiStageMember>,
    /// Measures this CTE renders alongside `member`. Rolling windows whose
    /// base scans would be identical share one, and each window then reads its
    /// own measure column off it.
    ///
    /// Load-bearing, like the time series' granularity list: every description
    /// is built before any is planned, so a window joining an existing base
    /// scan is always visible to it.
    co_measures: RefCell<Vec<Rc<MemberSymbol>>>,
    state: Rc<QueryProperties>,
    input: Vec<Rc<MultiStageQueryDescription>>,
    /// Dim-grid sources for the JOIN-based assembly. Empty for the
    /// window-based path. Populated by `make_queries_descriptions` when
    /// the grain reshape actually shrinks the partition grain vs the
    /// leaf grain — in that case `input` is rebuilt at partition grain
    /// and the original full-grain inputs move here as keys.
    keys_input: Vec<Rc<MultiStageQueryDescription>>,
    alias: String,
}

impl MultiStageQueryDescription {
    pub fn new(
        member: Rc<MultiStageMember>,
        state: Rc<QueryProperties>,
        input: Vec<Rc<MultiStageQueryDescription>>,
        keys_input: Vec<Rc<MultiStageQueryDescription>>,
        alias: String,
    ) -> Rc<Self> {
        Rc::new(Self {
            member,
            co_measures: RefCell::new(vec![]),
            state,
            input,
            keys_input,
            alias,
        })
    }

    pub fn schema(&self) -> Rc<LogicalSchema> {
        LogicalSchema::default()
            .set_time_dimensions(self.state.time_dimensions().clone())
            .set_dimensions(self.state.dimensions().clone())
            .set_measures(self.measures())
            .into_rc()
    }

    /// Every measure this CTE renders: its own member first, then the measures
    /// riding along on it.
    pub fn measures(&self) -> Vec<Rc<MemberSymbol>> {
        let mut measures = vec![self.member_node().clone()];
        measures.extend(self.co_measures());
        measures
    }

    /// The measures riding along on this CTE, without its own member.
    pub fn co_measures(&self) -> Vec<Rc<MemberSymbol>> {
        self.co_measures.borrow().clone()
    }

    /// Registers `measure` to be rendered by this CTE as well. A measure
    /// already rendered here is left alone.
    pub fn add_co_measure(&self, measure: Rc<MemberSymbol>) {
        let name = measure.full_name();
        if self.member_name() == name {
            return;
        }
        let mut co_measures = self.co_measures.borrow_mut();
        if co_measures.iter().any(|m| m.full_name() == name) {
            return;
        }
        co_measures.push(measure);
    }

    pub fn member_node(&self) -> &Rc<MemberSymbol> {
        &self.member.evaluation_node()
    }

    pub fn is_multi_stage_dimension(&self) -> bool {
        self.member.member_type().is_multi_stage_dimension()
    }

    pub fn member(&self) -> &Rc<MultiStageMember> {
        &self.member
    }

    pub fn state(&self) -> &Rc<QueryProperties> {
        &self.state
    }

    pub fn member_name(&self) -> String {
        self.member_node().full_name()
    }

    pub fn alias(&self) -> &String {
        &self.alias
    }

    pub fn input(&self) -> &Vec<Rc<MultiStageQueryDescription>> {
        &self.input
    }

    pub fn keys_input(&self) -> &Vec<Rc<MultiStageQueryDescription>> {
        &self.keys_input
    }

    pub fn is_leaf(&self) -> bool {
        self.input.is_empty()
    }

    /// Walks the description subtree and returns
    /// `(dimensions, time_dimensions)` whose chain-resolved members
    /// have no multi-stage members of their own. Duplicates are
    /// removed by full name.
    pub fn collect_all_non_multi_stage_dimension(
        &self,
    ) -> Result<(Vec<Rc<MemberSymbol>>, Vec<Rc<MemberSymbol>>), CubeError> {
        let mut dimensions = vec![];
        let mut time_dimensions = vec![];
        self.collect_all_non_multi_stage_dimension_impl(&mut dimensions, &mut time_dimensions);
        let dimensions = dimensions
            .into_iter()
            .unique_by(|d| d.full_name())
            .filter_map(|d| match has_multi_stage_members(&d, true) {
                Ok(true) => None,
                Ok(false) => Some(Ok(d)),
                Err(e) => Some(Err(e)),
            })
            .collect::<Result<Vec<_>, _>>()?;

        let time_dimensions = time_dimensions
            .into_iter()
            .unique_by(|d| d.full_name())
            .filter_map(|d| match has_multi_stage_members(&d, true) {
                Ok(true) => None,
                Ok(false) => Some(Ok(d)),
                Err(e) => Some(Err(e)),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok((dimensions, time_dimensions))
    }

    fn collect_all_non_multi_stage_dimension_impl(
        &self,
        dimensions: &mut Vec<Rc<MemberSymbol>>,
        time_dimensions: &mut Vec<Rc<MemberSymbol>>,
    ) {
        dimensions.extend(self.state.dimensions().iter().cloned());
        time_dimensions.extend(self.state.time_dimensions().iter().cloned());
        for child in self.input.iter() {
            child.collect_all_non_multi_stage_dimension_impl(dimensions, time_dimensions);
        }
    }

    /// True if this description is the base scan of a rolling window built for
    /// an equivalent state. Such a scan is byte-identical whatever measure is
    /// aggregated over it, so another window's measure can ride along instead
    /// of scanning the fact table again.
    pub fn is_match_rolling_window_base(
        &self,
        state: &Rc<QueryProperties>,
        is_ungrouped: bool,
    ) -> bool {
        matches!(
            self.member.member_type(),
            MultiStageMemberType::Leaf(MultiStageLeafMemberType::Measure)
        ) && self.member.has_aggregates_on_top()
            && !self.member.is_without_member_leaf()
            && self.member.is_ungrupped() == is_ungrouped
            && state.eq_as_state(&self.state)
    }

    /// True if this description renders `member_node` under an
    /// equivalent state — used to deduplicate CTEs when the same
    /// member is reached through different paths in the dependency
    /// graph.
    pub fn is_match_member_and_state(
        &self,
        member_node: &Rc<MemberSymbol>,
        state: &Rc<QueryProperties>,
    ) -> bool {
        member_node.full_name() == self.member_name() && state.eq_as_state(&self.state)
    }
}
