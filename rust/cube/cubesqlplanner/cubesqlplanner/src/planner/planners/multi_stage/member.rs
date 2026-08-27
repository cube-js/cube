use crate::planner::{MeasureTimeShifts, MemberSymbol, MultiStageGrain};
use std::rc::Rc;

/// Description of the time-series CTE driving a rolling-window
/// computation: the time dimension and, optionally, the alias of a
/// sibling CTE that resolves its date range at query time.
#[derive(Clone)]
pub struct TimeSeriesDescription {
    pub time_dimension: Rc<MemberSymbol>,
    pub date_range_cte: Option<String>,
}

/// Kind of leaf CTE in a multi-stage chain: a base measure query,
/// a time-series axis, or a query that just resolves the date range
/// of a time dimension.
#[derive(Clone)]
pub enum MultiStageLeafMemberType {
    Measure,
    TimeSeries(Rc<TimeSeriesDescription>),
    TimeSeriesGetRange(Rc<MemberSymbol>),
}

/// Bounds of a regular rolling window: trailing / leading interval
/// strings and a time-series offset.
#[derive(Clone)]
pub struct RegularRollingWindow {
    pub trailing: Option<String>,
    pub leading: Option<String>,
    pub offset: String,
}

/// To-date rolling window — accumulates since the start of
/// `granularity` (month-to-date, year-to-date, …).
#[derive(Clone)]
pub struct ToDateRollingWindow {
    pub granularity: String,
}

/// Flavour of rolling-window computation: regular trailing/leading
/// window or a to-date window.
#[derive(Clone)]
pub enum RollingWindowType {
    Regular(RegularRollingWindow),
    ToDate(ToDateRollingWindow),
}

/// Planner-side description of a rolling window: the time
/// dimension used for windowing (and its lower-granularity base
/// version produced in the leaf CTE) plus the chosen window type.
#[derive(Clone)]
pub struct RollingWindowDescription {
    pub time_dimension: Rc<MemberSymbol>,
    pub base_time_dimension: Rc<MemberSymbol>,
    pub rolling_window: RollingWindowType,
}

impl RollingWindowDescription {
    pub fn new_regular(
        time_dimension: Rc<MemberSymbol>,
        base_time_dimension: Rc<MemberSymbol>,
        trailing: Option<String>,
        leading: Option<String>,
        offset: String,
    ) -> Self {
        let regular_window = RegularRollingWindow {
            trailing,
            leading,
            offset,
        };
        Self {
            time_dimension,
            base_time_dimension,
            rolling_window: RollingWindowType::Regular(regular_window),
        }
    }

    pub fn new_to_date(
        time_dimension: Rc<MemberSymbol>,
        base_time_dimension: Rc<MemberSymbol>,
        granularity: String,
    ) -> Self {
        Self {
            time_dimension,
            base_time_dimension,
            rolling_window: RollingWindowType::ToDate(ToDateRollingWindow { granularity }),
        }
    }
}

/// Semantic shape of a non-leaf multi-stage CTE: a rank window,
/// an aggregate (possibly window-rendered), a non-aggregating
/// calculation, a dimension calculation, or a rolling window.
#[derive(Clone)]
pub enum MultiStageInodeMemberType {
    Rank,
    Aggregate,
    Calculate,
    Dimension,
    RollingWindow(RollingWindowDescription),
}

/// Non-leaf node in a multi-stage tree. Bundles the semantic
/// `inode_type` (Rank / Aggregate / Calculate / Dimension /
/// RollingWindow) with the partition-shaping `grain` carried over from
/// the measure's data-model directives and an optional `time_shift`.
#[derive(Clone)]
pub struct MultiStageInodeMember {
    inode_type: MultiStageInodeMemberType,
    grain: MultiStageGrain,
    time_shift: Option<MeasureTimeShifts>,
    /// Optimisation flag: this Aggregate inode is a safe candidate for
    /// the `window`-based render — single measure dep, additive identity
    /// rollup, no leaf-extending modifiers. When `true`, assembly skips
    /// the JOIN-model and `member_query_planner` emits a window function.
    /// Default `false`.
    use_window_path: bool,
}

impl MultiStageInodeMember {
    pub fn new(
        inode_type: MultiStageInodeMemberType,
        grain: MultiStageGrain,
        time_shift: Option<MeasureTimeShifts>,
    ) -> Self {
        Self {
            inode_type,
            grain,
            time_shift,
            use_window_path: false,
        }
    }

    pub fn with_use_window_path(mut self, value: bool) -> Self {
        self.use_window_path = value;
        self
    }

    pub fn use_window_path(&self) -> bool {
        self.use_window_path
    }

    pub fn inode_type(&self) -> &MultiStageInodeMemberType {
        &self.inode_type
    }

    pub fn grain(&self) -> &MultiStageGrain {
        &self.grain
    }

    pub fn time_shift(&self) -> &Option<MeasureTimeShifts> {
        &self.time_shift
    }
}

/// Position of a CTE in the multi-stage tree: either an inner node
/// (depends on other CTEs) or a leaf (queries the underlying data
/// directly).
#[derive(Clone)]
pub enum MultiStageMemberType {
    Inode(MultiStageInodeMember),
    Leaf(MultiStageLeafMemberType),
}

impl MultiStageMemberType {
    pub fn is_multi_stage_dimension(&self) -> bool {
        if let Self::Inode(inode) = &self {
            return matches!(inode.inode_type(), MultiStageInodeMemberType::Dimension);
        }
        false
    }
}

/// One node in a multi-stage tree: its position (`member_type`),
/// the member symbol it renders, and a few rendering flags
/// (ungrouped, has-aggregates-on-top, is-without-member-leaf).
pub struct MultiStageMember {
    member_type: MultiStageMemberType,
    member_symbol: Rc<MemberSymbol>,
    is_without_member_leaf: bool, //FIXME hack, refactor needed
    is_ungrupped: bool,
    has_aggregates_on_top: bool,
}

impl MultiStageMember {
    pub fn new(
        member_type: MultiStageMemberType,
        evaluation_node: Rc<MemberSymbol>,
        is_ungrupped: bool,
        has_aggregates_on_top: bool,
    ) -> Rc<Self> {
        Rc::new(Self {
            member_type,
            member_symbol: evaluation_node,
            is_without_member_leaf: false,
            is_ungrupped,
            has_aggregates_on_top,
        })
    }

    /// Builds a leaf node whose base CTE selects only dimensions —
    /// the member itself is not computed by the leaf (e.g. for a
    /// `Rank` measure where the value comes purely from a window
    /// function applied on top).
    pub fn new_without_member_leaf(
        member_type: MultiStageMemberType,
        evaluation_node: Rc<MemberSymbol>,
        is_ungrupped: bool,
        has_aggregates_on_top: bool,
    ) -> Rc<Self> {
        Rc::new(Self {
            member_type,
            member_symbol: evaluation_node,
            is_without_member_leaf: true,
            is_ungrupped,
            has_aggregates_on_top,
        })
    }

    pub fn member_type(&self) -> &MultiStageMemberType {
        &self.member_type
    }

    pub fn evaluation_node(&self) -> &Rc<MemberSymbol> {
        &self.member_symbol
    }

    /// True when this node is a leaf whose CTE selects dimensions
    /// only, without computing the member's value (see
    /// `new_without_member_leaf`).
    pub fn is_without_member_leaf(&self) -> bool {
        self.is_without_member_leaf
    }

    pub fn full_name(&self) -> String {
        self.member_symbol.full_name()
    }

    pub fn is_ungrupped(&self) -> bool {
        self.is_ungrupped
    }

    pub fn has_aggregates_on_top(&self) -> bool {
        self.has_aggregates_on_top
    }
}
