use crate::cube_bridge::measure_definition::TimeShiftReference;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::BaseMember;
use crate::planner::BaseTimeDimension;
use cubenativeutils::CubeError;
use lazy_static::lazy_static;
use regex::Regex;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct MultiStageTimeShift {
    pub interval: String,
    pub time_dimension: String,
}

lazy_static! {
    static ref INTERVAL_MATCH_RE: Regex =
        Regex::new(r"^(-?\d+) (second|minute|hour|day|week|month|quarter|year)s?$").unwrap();
}
impl MultiStageTimeShift {
    pub fn try_from_reference(reference: &TimeShiftReference) -> Result<Self, CubeError> {
        let parsed_interval =
            if let Some(captures) = INTERVAL_MATCH_RE.captures(&reference.interval) {
                let duration = if let Some(duration) = captures.get(1) {
                    duration.as_str().parse::<i64>().ok()
                } else {
                    None
                };
                let granularity = if let Some(granularity) = captures.get(2) {
                    Some(granularity.as_str().to_owned())
                } else {
                    None
                };
                if let Some((duration, granularity)) = duration.zip(granularity) {
                    Some((duration, granularity))
                } else {
                    None
                }
            } else {
                None
            };
        if let Some((duration, granularity)) = parsed_interval {
            let duration = if reference.shift_type.as_ref().unwrap_or(&format!("prior")) == "next" {
                duration * (-1)
            } else {
                duration
            };

            Ok(Self {
                interval: format!("{duration} {granularity}"),
                time_dimension: reference.time_dimension.clone(),
            })
        } else {
            Err(CubeError::user(format!(
                "Invalid interval: {}",
                reference.interval
            )))
        }
    }
}

#[derive(Clone)]
pub enum MultiStageLeafMemberType {
    Measure,
    TimeSeries(Rc<BaseTimeDimension>),
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
}

#[derive(Clone)]
pub struct RollingWindowDescription {
    pub time_dimension: Rc<dyn BaseMember>,
    pub rolling_window: RollingWindowType,
}

impl RollingWindowDescription {
    pub fn new_regular(
        time_dimension: Rc<dyn BaseMember>,
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
            rolling_window: RollingWindowType::Regular(regular_window),
        }
    }

    pub fn new_to_date(time_dimension: Rc<dyn BaseMember>, granularity: String) -> Self {
        Self {
            time_dimension,
            rolling_window: RollingWindowType::ToDate(ToDateRollingWindow { granularity }),
        }
    }
}

#[derive(Clone)]
pub struct RunningTotalDescription {
    pub time_dimension: Rc<dyn BaseMember>,
}

#[derive(Clone)]
pub enum MultiStageInodeMemberType {
    Rank,
    Aggregate,
    Calculate,
    RollingWindow(RollingWindowDescription),
    RunningTotal(RunningTotalDescription),
}

#[derive(Clone)]
pub struct MultiStageInodeMember {
    inode_type: MultiStageInodeMemberType,
    reduce_by: Vec<String>,
    add_group_by: Vec<String>,
    group_by: Option<Vec<String>>,
    time_shifts: Vec<MultiStageTimeShift>,
}

impl MultiStageInodeMember {
    pub fn new(
        inode_type: MultiStageInodeMemberType,
        reduce_by: Vec<String>,
        add_group_by: Vec<String>,
        group_by: Option<Vec<String>>,
        time_shifts: Vec<MultiStageTimeShift>,
    ) -> Self {
        Self {
            inode_type,
            reduce_by,
            add_group_by,
            group_by,
            time_shifts,
        }
    }

    pub fn inode_type(&self) -> &MultiStageInodeMemberType {
        &self.inode_type
    }

    pub fn reduce_by(&self) -> &Vec<String> {
        &self.reduce_by
    }

    pub fn add_group_by(&self) -> &Vec<String> {
        &self.add_group_by
    }

    pub fn group_by(&self) -> &Option<Vec<String>> {
        &self.group_by
    }

    pub fn time_shifts(&self) -> &Vec<MultiStageTimeShift> {
        &self.time_shifts
    }
}

#[derive(Clone)]
pub enum MultiStageMemberType {
    Inode(MultiStageInodeMember),
    Leaf(MultiStageLeafMemberType),
}

pub struct MultiStageMember {
    member_type: MultiStageMemberType,
    evaluation_node: Rc<MemberSymbol>,
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
            evaluation_node,
            is_ungrupped,
            has_aggregates_on_top,
        })
    }

    pub fn member_type(&self) -> &MultiStageMemberType {
        &self.member_type
    }

    pub fn evaluation_node(&self) -> &Rc<MemberSymbol> {
        &self.evaluation_node
    }

    pub fn full_name(&self) -> String {
        self.evaluation_node.full_name()
    }

    pub fn is_ungrupped(&self) -> bool {
        self.is_ungrupped
    }

    pub fn has_aggregates_on_top(&self) -> bool {
        self.has_aggregates_on_top
    }
}
