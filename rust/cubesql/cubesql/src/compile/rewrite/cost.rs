use crate::compile::rewrite::{LogicalPlanLanguage, MemberErrorPriority, TimeDimensionGranularity};
use egg::{CostFunction, Id, Language};

pub struct BestCubePlan;

/// This cost struct maintains following structural relationships:
/// - `replacers` > other nodes - having replacers in structure means not finished processing
/// - `table_scans` > other nodes - having table scan means not detected cube scan
/// - `non_detected_cube_scans` > other nodes - minimize cube scans without members
/// - `filters` > `filter_members` - optimize for push down of filters
/// - `filter_members` > `cube_members` - optimize for `inDateRange` filter push down to time dimension
/// - match errors by priority - optimize for more specific errors
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct CubePlanCost {
    replacers: i64,
    table_scans: i64,
    non_detected_cube_scans: i64,
    filters: i64,
    structure_points: i64,
    filter_members: i64,
    cube_members: i64,
    errors: i64,
    ast_size: usize,
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
            cube_members: self.cube_members + other.cube_members,
            errors: self.errors + other.errors,
            structure_points: self.structure_points + other.structure_points,
            ast_size: self.ast_size + other.ast_size,
        }
    }
}

impl CostFunction<LogicalPlanLanguage> for BestCubePlan {
    type Cost = CubePlanCost;
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

        let filter_members = match enode {
            LogicalPlanLanguage::FilterMember(_) => 1,
            _ => 0,
        };

        let filters = match enode {
            LogicalPlanLanguage::Filter(_) => 1,
            _ => 0,
        };

        let cube_members = match enode {
            LogicalPlanLanguage::Measure(_) => 1,
            LogicalPlanLanguage::Dimension(_) => 1,
            LogicalPlanLanguage::LiteralMember(_) => 1,
            LogicalPlanLanguage::TimeDimensionGranularity(TimeDimensionGranularity(Some(_))) => 1,
            LogicalPlanLanguage::MemberError(_) => 1,
            _ => 0,
        };

        let this_replacers = match enode {
            LogicalPlanLanguage::MemberReplacer(_) => 1,
            LogicalPlanLanguage::FilterReplacer(_) => 1,
            LogicalPlanLanguage::TimeDimensionDateRangeReplacer(_) => 1,
            LogicalPlanLanguage::InnerAggregateSplitReplacer(_) => 1,
            LogicalPlanLanguage::OuterProjectionSplitReplacer(_) => 1,
            LogicalPlanLanguage::OuterAggregateSplitReplacer(_) => 1,
            LogicalPlanLanguage::MemberPushdownReplacer(_) => 1,
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
            _ => 0,
        };
        enode.children().iter().fold(
            CubePlanCost {
                replacers: this_replacers,
                table_scans,
                filters,
                filter_members,
                non_detected_cube_scans,
                cube_members,
                errors: this_errors,
                structure_points,
                ast_size: 1,
            },
            |cost, id| {
                let child = costs(*id);
                cost.add_child(&child)
            },
        )
    }
}
