use crate::planner::sql_evaluator::{MeasureTimeShifts, MemberSymbol};
use std::rc::Rc;

#[derive(Clone)]
pub struct TimeSeriesDescription {
    pub time_dimension: Rc<MemberSymbol>,
    pub date_range_cte: Option<String>,
}

#[derive(Clone)]
pub enum MultiStageLeafMemberType {
    Measure,
    TimeSeries(Rc<TimeSeriesDescription>),
    TimeSeriesGetRange(Rc<MemberSymbol>),
}

#[derive(Clone)]
pub struct RegularRollingWindow {
    pub trailing: Option<String>,
    pub leading: Option<String>,
    pub offset: String,
}

#[derive(Clone)]
pub struct ToDateRollingWindow {
    pub granularity: String,
}

#[derive(Clone)]
pub enum RollingWindowType {
    Regular(RegularRollingWindow),
    ToDate(ToDateRollingWindow),
    RunningTotal,
}

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

    pub fn new_running_total(
        time_dimension: Rc<MemberSymbol>,
        base_time_dimension: Rc<MemberSymbol>,
    ) -> Self {
        Self {
            time_dimension,
            base_time_dimension,
            rolling_window: RollingWindowType::RunningTotal,
        }
    }
}

#[derive(Clone)]
pub enum MultiStageInodeMemberType {
    Rank,
    Aggregate,
    Calculate,
    RollingWindow(RollingWindowDescription),
}

#[derive(Clone)]
pub struct MultiStageInodeMember {
    inode_type: MultiStageInodeMemberType,
    reduce_by: Vec<Rc<MemberSymbol>>,
    add_group_by: Vec<Rc<MemberSymbol>>,
    group_by: Option<Vec<Rc<MemberSymbol>>>,
    time_shift: Option<MeasureTimeShifts>,
}

impl MultiStageInodeMember {
    pub fn new(
        inode_type: MultiStageInodeMemberType,
        reduce_by: Vec<Rc<MemberSymbol>>,
        add_group_by: Vec<Rc<MemberSymbol>>,
        group_by: Option<Vec<Rc<MemberSymbol>>>,
        time_shift: Option<MeasureTimeShifts>,
    ) -> Self {
        Self {
            inode_type,
            reduce_by,
            add_group_by,
            group_by,
            time_shift,
        }
    }

    pub fn inode_type(&self) -> &MultiStageInodeMemberType {
        &self.inode_type
    }

    pub fn reduce_by(&self) -> Vec<String> {
        self.reduce_by.iter().map(|s| s.full_name()).collect()
    }

    pub fn add_group_by(&self) -> Vec<String> {
        self.add_group_by.iter().map(|s| s.full_name()).collect()
    }

    pub fn reduce_by_symbols(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.reduce_by
    }

    pub fn add_group_by_symbols(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.add_group_by
    }

    pub fn group_by(&self) -> Option<Vec<String>> {
        self.group_by
            .as_ref()
            .map(|g| g.iter().map(|s| s.full_name()).collect())
    }

    pub fn group_by_symbols(&self) -> &Option<Vec<Rc<MemberSymbol>>> {
        &self.group_by
    }

    pub fn time_shift(&self) -> &Option<MeasureTimeShifts> {
        &self.time_shift
    }
}

#[derive(Clone)]
pub enum MultiStageMemberType {
    Inode(MultiStageInodeMember),
    Leaf(MultiStageLeafMemberType),
}

pub struct MultiStageMember {
    member_type: MultiStageMemberType,
    member_symbol: Rc<MemberSymbol>,
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
