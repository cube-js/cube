use crate::planner::multi_fact_join_groups::MultiFactJoinGroups;
use crate::planner::symbol_classification::FullKeyAggregateMeasures;
use crate::planner::{CubeId, JoinTree, MemberSymbol};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

/// Measures aggregated directly over one join.
#[derive(Clone)]
pub struct RegularGroup {
    join: Rc<JoinTree>,
    measures: Vec<Rc<MemberSymbol>>,
}

impl RegularGroup {
    pub fn join(&self) -> &Rc<JoinTree> {
        &self.join
    }

    pub fn measures(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.measures
    }
}

/// Measures of `key_cube` whose rows `join` multiplies, aggregated once per
/// primary key of that cube.
#[derive(Clone)]
pub struct MultipliedGroup {
    key_cube: CubeId,
    join: Rc<JoinTree>,
    measures: Vec<Rc<MemberSymbol>>,
}

impl MultipliedGroup {
    pub fn key_cube(&self) -> &CubeId {
        &self.key_cube
    }

    pub fn join(&self) -> &Rc<JoinTree> {
        &self.join
    }

    pub fn measures(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.measures
    }
}

/// The groups a non-simple query aggregates its measures in, each planned as
/// a subquery of its own, in the order they are planned.
#[derive(Clone, Default)]
pub struct MeasureGroups {
    regular: Vec<RegularGroup>,
    multiplied: Vec<MultipliedGroup>,
}

impl MeasureGroups {
    pub fn try_new(
        measures: &FullKeyAggregateMeasures,
        join_groups: &MultiFactJoinGroups,
    ) -> Result<Self, CubeError> {
        let mut regular = Vec::new();
        if !measures.regular_measures.is_empty() {
            for (join, measures) in join_groups
                .for_measures(&measures.regular_measures)?
                .groups()
                .iter()
            {
                regular.push(RegularGroup {
                    join: join.clone(),
                    measures: measures.clone(),
                });
            }
        }

        let mut multiplied = Vec::new();
        // `into_group_map_by` yields a HashMap; sort the cubes so subquery
        // numbering stays deterministic across runs.
        for (key_cube, measures) in measures
            .multiplied_measures
            .iter()
            .into_group_map_by(|m| m.cube_id().clone())
            .into_iter()
            .sorted_by(|(a, _), (b, _)| a.cmp(b))
        {
            let measures = measures
                .into_iter()
                .map(|m| m.measure().clone())
                .collect_vec();
            let groups = join_groups.for_measures(&measures)?;
            if groups.groups().is_empty() {
                return Err(CubeError::internal(format!(
                    "No join groups returned for aggregate measures of cube {key_cube}"
                )));
            }
            // Every measure of the cube must land in some group, otherwise it
            // would be missing from the subqueries the query merges.
            let grouped_count: usize = groups.groups().iter().map(|(_, ms)| ms.len()).sum();
            if grouped_count != measures.len() {
                return Err(CubeError::internal(format!(
                    "Join grouping dropped measures of cube {key_cube}: {grouped_count} grouped of {}",
                    measures.len()
                )));
            }
            // The key cube fixes the primary keys to deduplicate on, the join
            // tree the joins to build: measures of one cube can still need
            // different trees.
            for (join, measures) in groups.groups().iter() {
                multiplied.push(MultipliedGroup {
                    key_cube: key_cube.clone(),
                    join: join.clone(),
                    measures: measures.clone(),
                });
            }
        }

        Ok(Self {
            regular,
            multiplied,
        })
    }

    pub fn regular(&self) -> &[RegularGroup] {
        &self.regular
    }

    pub fn multiplied(&self) -> &[MultipliedGroup] {
        &self.multiplied
    }
}
