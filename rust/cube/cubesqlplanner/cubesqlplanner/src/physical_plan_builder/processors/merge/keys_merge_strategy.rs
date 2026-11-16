use super::super::grid_lookup::build_grid_lookup;
use super::super::keys_grid::build_keys_grid;
use super::MergeStrategy;
use crate::logical_plan::{LookupLeg, Merge};
use crate::physical_plan::From;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::physical_plan_builder::PushDownBuilderContext;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Merge for dialects without FULL JOIN: the distinct keys of every ref
/// (`UNION DISTINCT`), with each ref left-joined onto them by its keys.
pub(super) struct KeysMergeStrategy<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> KeysMergeStrategy<'a> {
    pub fn new(builder: &'a PhysicalPlanBuilder) -> Rc<Self> {
        Rc::new(Self { builder })
    }
}

impl MergeStrategy for KeysMergeStrategy<'_> {
    fn process(
        &self,
        merge: &Merge,
        context: &PushDownBuilderContext,
    ) -> Result<Rc<From>, CubeError> {
        let key_dims: Vec<Rc<MemberSymbol>> = merge.schema().all_dimensions().cloned().collect();
        let grid = build_keys_grid(self.builder, &key_dims, merge.subqueries(), context)?;
        let legs = merge
            .subqueries()
            .iter()
            .map(|subquery| LookupLeg::new(subquery.clone(), merge.schema()))
            .collect::<Vec<_>>();
        build_grid_lookup(self.builder, grid, &legs, context)
    }
}
