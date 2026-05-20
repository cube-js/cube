use crate::logical_plan::{LogicalMultiStageMember, MultiStageSubqueryRef};
use crate::planner::{MemberSymbol, QueryProperties};
use std::collections::HashMap;
use std::rc::Rc;

/// Role each CTE plays in the logical plan. Matching together with
/// `members + state` defines an equivalence class for dedup: two CTEs
/// with the same role / member-set / state describe the same body and
/// should map to the same `cte_N`.
///
/// Coverage today:
/// - `Keys` — KS-CTE of the multiplied flow (primary keys + outer dims).
/// - `MultiStageDimension` — multi-stage / sub-query dimension CTEs
///   (Stage::DimensionCalc inode and DSQ leaf — DSQ is a degenerate
///   single-level multi-stage dim).
/// - `MultiStageMeasure` — multi-stage measure CTEs (Stage::Rank /
///   Aggregate / Calculate, both inode and leaf).
/// - `FactMeasure` — regular-measure leaf CTE per multi-fact group.
/// - `MultipliedMeasureSubquery` — MS-CTE (raw projection of dedup-by-pk
///   measures) and the outer `Query{AggregateMultiplied}` CTE that
///   stitches it to KS-CTE. They share the role today; the outer's
///   member-set differs from the inner's so dedup wouldn't conflate
///   them in practice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CteRole {
    Keys,
    MultiStageDimension,
    MultiStageMeasure,
    FactMeasure,
    MultipliedMeasureSubquery,
}

impl CteRole {
    /// Prefix used for generated CTE names: `keys_0`, `keys_1`,
    /// `multi_stage_dim_0`, etc.
    pub fn name_prefix(self) -> &'static str {
        match self {
            CteRole::Keys => "keys",
            CteRole::MultiStageDimension => "multi_stage_dim",
            CteRole::MultiStageMeasure => "multi_stage_measure",
            CteRole::FactMeasure => "fact_measure",
            CteRole::MultipliedMeasureSubquery => "multiplied_measure",
        }
    }
}

/// One CTE registered in `CteState`: its role, the members it
/// projects (drives matching), the `QueryProperties` state under
/// which it was planned (drives matching), and the rendered body.
pub struct CteEntry {
    pub role: CteRole,
    pub members: Vec<Rc<MemberSymbol>>,
    pub state: Rc<QueryProperties>,
    pub body: Rc<LogicalMultiStageMember>,
}

/// Accumulator the multi-stage / multiplied planners write CTEs
/// into during planning: a monotonic counter for generated CTE
/// names (`cte_0`, `cte_1`, ...), the list of `CteEntry`s, and the
/// subquery refs that the outer `FullKeyAggregate` will join over.
pub struct CteState {
    counters: HashMap<CteRole, usize>,
    entries: Vec<CteEntry>,
    subquery_refs: Vec<Rc<MultiStageSubqueryRef>>,
}

impl CteState {
    pub fn new() -> Self {
        Self {
            counters: HashMap::new(),
            entries: Vec::new(),
            subquery_refs: Vec::new(),
        }
    }

    /// Generates the next unique CTE name for `role`: `{prefix}_0`,
    /// `{prefix}_1`, ...
    pub fn next_cte_name(&mut self, role: CteRole) -> String {
        let counter = self.counters.entry(role).or_insert(0);
        let name = format!("{}_{}", role.name_prefix(), counter);
        *counter += 1;
        name
    }

    /// Registers `body` under `role + members + state`. Returns the
    /// final CTE name to use in refs and SQL — this is `body.name` for
    /// fresh entries, and the existing entry's name when a same-named
    /// body is already registered (so callers can't accidentally pin
    /// a name that won't appear in the WITH clause).
    pub fn add_member(
        &mut self,
        role: CteRole,
        members: Vec<Rc<MemberSymbol>>,
        state: Rc<QueryProperties>,
        body: Rc<LogicalMultiStageMember>,
    ) -> String {
        if let Some(existing) = self.entries.iter().find(|e| e.body.name == body.name) {
            // Same-named bodies (e.g. the same DSQ referenced from
            // multiple consumers) are deduplicated here so each CTE is
            // rendered once.
            return existing.body.name.clone();
        }
        let name = body.name.clone();
        self.entries.push(CteEntry {
            role,
            members,
            state,
            body,
        });
        name
    }

    pub fn add_subquery_ref(&mut self, subquery_ref: Rc<MultiStageSubqueryRef>) {
        self.subquery_refs.push(subquery_ref);
    }

    /// Consumes the state, returning the accumulated CTE bodies
    /// and subquery refs. Role/member/state metadata is planning-only
    /// and dropped here.
    pub fn into_results(
        self,
    ) -> (
        Vec<Rc<LogicalMultiStageMember>>,
        Vec<Rc<MultiStageSubqueryRef>>,
    ) {
        let members = self.entries.into_iter().map(|e| e.body).collect();
        (members, self.subquery_refs)
    }

    /// Current number of subquery refs — used by `plan_into` to mark
    /// a per-scope baseline before driving sub-planners.
    pub fn subquery_refs_len(&self) -> usize {
        self.subquery_refs.len()
    }

    /// Drain refs accumulated since `baseline` — caller uses them as the
    /// FK data inputs of its root Query. Members stay in this `CteState`
    /// and are read off `into_results` to populate the `LogicalPlan`
    /// CTE pool.
    pub fn drain_subquery_refs_from(&mut self, baseline: usize) -> Vec<Rc<MultiStageSubqueryRef>> {
        self.subquery_refs.split_off(baseline)
    }

    /// Look up an existing CTE entry by `(role, members, state)`.
    /// Members are matched as an unordered set of reference-chain-
    /// resolved names; state via `QueryProperties::eq_as_state`.
    /// Returned for future dedup wiring — no callers yet.
    pub fn find_matching(
        &self,
        role: CteRole,
        members: &[Rc<MemberSymbol>],
        state: &QueryProperties,
    ) -> Option<&CteEntry> {
        self.entries.iter().find(|e| {
            e.role == role && members_equivalent(&e.members, members) && e.state.eq_as_state(state)
        })
    }
}

fn members_equivalent(a: &[Rc<MemberSymbol>], b: &[Rc<MemberSymbol>]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut a_names: Vec<String> = a
        .iter()
        .map(|m| m.clone().resolve_reference_chain().full_name())
        .collect();
    let mut b_names: Vec<String> = b
        .iter()
        .map(|m| m.clone().resolve_reference_chain().full_name())
        .collect();
    a_names.sort();
    b_names.sort();
    a_names == b_names
}
