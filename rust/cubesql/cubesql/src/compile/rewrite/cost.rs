use crate::compile::rewrite::LogicalPlanLanguage;
use egg::Language;
use egg::{CostFunction, Id};

pub struct BestCubePlan;

impl CostFunction<LogicalPlanLanguage> for BestCubePlan {
    type Cost = (
        /* Cube nodes */ i64,
        /* Replacers */ i64,
        /* Structure points */ i64,
        /* AST size */ usize,
    );
    fn cost<C>(&mut self, enode: &LogicalPlanLanguage, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        let this_cube_nodes = match enode {
            LogicalPlanLanguage::CubeScan(_) => -1,
            LogicalPlanLanguage::Measure(_) => -1,
            LogicalPlanLanguage::Dimension(_) => -1,
            LogicalPlanLanguage::TimeDimension(_) => -1,
            _ => 0,
        };

        let this_replacers = match enode {
            LogicalPlanLanguage::MemberReplacer(_) => 1,
            LogicalPlanLanguage::FilterReplacer(_) => 1,
            LogicalPlanLanguage::TimeDimensionDateRangeReplacer(_) => 1,
            _ => 0,
        };

        let this_cube_structure = match enode {
            // TODO needed to get rid of FilterOpFilters on upper level
            LogicalPlanLanguage::FilterOpFilters(_) => 1,
            _ => 0,
        };
        enode.children().iter().fold(
            (this_cube_nodes, this_replacers, this_cube_structure, 1),
            |(cube_nodes, replacers, structure, nodes), id| {
                let (child_cube_nodes, child_replacers, child_structure, child_nodes) = costs(*id);
                (
                    cube_nodes + child_cube_nodes,
                    replacers + child_replacers,
                    structure + child_structure,
                    nodes + child_nodes,
                )
            },
        )
    }
}
