use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{LogicalJoin, MultiStageDimensionJoin};
use crate::physical_plan::{From, JoinBuilder, JoinCondition};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::SqlJoinCondition;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct LogicalJoinProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, LogicalJoin> for LogicalJoinProcessor<'a> {
    type PhysycalNode = Rc<From>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        logical_join: &LogicalJoin,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        // Partition multi-stage dim refs by their join shape:
        // - OnPrimaryKeys: attaches inside the cube-join chain after
        //   each matching cube (root or joined).
        // - OnOuterDimensions: attaches once at the tail of the join,
        //   keyed on the explicit `join_dimensions` carried by the ref.
        let (pk_refs, outer_refs): (Vec<_>, Vec<_>) = context
            .multi_stage_dimension_refs
            .iter()
            .cloned()
            .partition(|r| matches!(&r.join, MultiStageDimensionJoin::OnPrimaryKeys { .. }));

        if logical_join.root().is_none() {
            return Ok(From::new_empty());
        }

        let root = logical_join.root().clone().unwrap().cube().clone();
        if logical_join.joins().is_empty() && pk_refs.is_empty() && outer_refs.is_empty() {
            Ok(From::new_from_cube(
                root.clone(),
                Some(root.default_alias_with_prefix(&context.alias_prefix)),
            ))
        } else {
            let mut join_builder = JoinBuilder::new_from_cube(
                root.clone(),
                Some(root.default_alias_with_prefix(&context.alias_prefix)),
            );

            let root_alias = root.default_alias_with_prefix(&context.alias_prefix);
            for ms_ref in pk_refs
                .iter()
                .filter(|r| matches_pk_cube(&r.join, root.name()))
            {
                let pk_dims = match &ms_ref.join {
                    MultiStageDimensionJoin::OnPrimaryKeys { pk_dimensions, .. } => pk_dimensions,
                    _ => continue,
                };
                self.builder.add_multi_stage_dimension_pk_join(
                    &ms_ref.name,
                    pk_dims,
                    &root_alias,
                    &mut join_builder,
                    context,
                )?;
            }
            for join in logical_join.joins().iter() {
                let joined_alias = join
                    .cube()
                    .cube()
                    .default_alias_with_prefix(&context.alias_prefix);
                join_builder.left_join_cube(
                    join.cube().cube().clone(),
                    Some(joined_alias.clone()),
                    JoinCondition::new_base_join(SqlJoinCondition::try_new(join.on_sql().clone())?),
                );
                for ms_ref in pk_refs
                    .iter()
                    .filter(|r| matches_pk_cube(&r.join, join.cube().cube().name()))
                {
                    let pk_dims = match &ms_ref.join {
                        MultiStageDimensionJoin::OnPrimaryKeys { pk_dimensions, .. } => {
                            pk_dimensions
                        }
                        _ => continue,
                    };
                    self.builder.add_multi_stage_dimension_pk_join(
                        &ms_ref.name,
                        pk_dims,
                        &joined_alias,
                        &mut join_builder,
                        context,
                    )?;
                }
            }
            for ms_ref in outer_refs.iter() {
                let dims = match &ms_ref.join {
                    MultiStageDimensionJoin::OnOuterDimensions { dimensions } => dimensions,
                    _ => continue,
                };
                self.builder.add_multistage_outer_dimensions_join(
                    ms_ref,
                    dims,
                    &mut join_builder,
                    context,
                )?;
            }
            Ok(From::new_from_join(join_builder.build()))
        }
    }
}

fn matches_pk_cube(join: &MultiStageDimensionJoin, cube_name: &str) -> bool {
    match join {
        MultiStageDimensionJoin::OnPrimaryKeys {
            cube_name: target, ..
        } => target == cube_name,
        _ => false,
    }
}

impl ProcessableNode for LogicalJoin {
    type ProcessorType<'a> = LogicalJoinProcessor<'a>;
}
