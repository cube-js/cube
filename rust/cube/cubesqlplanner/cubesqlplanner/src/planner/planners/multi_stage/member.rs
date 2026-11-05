use crate::planner::{MeasureTimeShifts, MemberSymbol, MultiStageGrain};
use std::rc::Rc;

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
/// version produced in the leaf subquery) plus the chosen window type.
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

/// Semantic shape of a non-leaf multi-stage subquery: a rank window,
/// an aggregate (possibly window-rendered), a non-aggregating
/// calculation, a dimension calculation.
#[derive(Clone)]
pub enum MultiStageInodeMemberType {
    Rank,
    Aggregate,
    Calculate,
    Dimension,
}

/// Non-leaf node in a multi-stage tree. Bundles the semantic
/// `inode_type` (Rank / Aggregate / Calculate / Dimension) with the partition-shaping `grain` carried over from
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
