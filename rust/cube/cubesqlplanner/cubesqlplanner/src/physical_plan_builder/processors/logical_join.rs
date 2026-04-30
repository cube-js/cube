use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::LogicalJoin;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{From, JoinBuilder, JoinCondition};
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
        let multi_stage_dimension = context.get_multi_stage_dimensions()?;
        if logical_join.root().is_none() {
            let res = if let Some(multi_stage_dimension) = &multi_stage_dimension {
                From::new_from_table_reference(
                    multi_stage_dimension.name.clone(),
                    multi_stage_dimension.schema.clone(),
                    None,
                )
            } else {
                From::new_empty()
            };
            return Ok(res);
        }

        let root = logical_join.root().clone().unwrap().cube().clone();
        if logical_join.joins().is_empty()
            && logical_join.dimension_subqueries().is_empty()
            && multi_stage_dimension.is_none()
        {
            Ok(From::new_from_cube(
                root.clone(),
                Some(root.default_alias_with_prefix(&context.alias_prefix)),
            ))
        } else {
            let mut join_builder = JoinBuilder::new_from_cube(
                root.clone(),
                Some(root.default_alias_with_prefix(&context.alias_prefix)),
            );

            for dimension_subquery in logical_join
                .dimension_subqueries() //TODO move dimension_subquery to
                .iter()
                .filter(|d| &d.subquery_dimension.cube_name() == root.name())
            {
                self.builder.add_subquery_join(
                    dimension_subquery.clone(),
                    &mut join_builder,
                    context,
                )?;
            }
            for join in logical_join.joins().iter() {
                join_builder.left_join_cube(
                    join.cube().cube().clone(),
                    Some(
                        join.cube()
                            .cube()
                            .default_alias_with_prefix(&context.alias_prefix),
                    ),
                    JoinCondition::new_base_join(SqlJoinCondition::try_new(join.on_sql().clone())?),
                );
                for dimension_subquery in logical_join
                    .dimension_subqueries()
                    .iter()
                    .filter(|d| &d.subquery_dimension.cube_name() == join.cube().cube().name())
                {
                    self.builder.add_subquery_join(
                        dimension_subquery.clone(),
                        &mut join_builder,
                        context,
                    )?;
                }
            }
            if let Some(multi_stage_dimension) = &multi_stage_dimension {
                self.builder.add_multistage_dimension_join(
                    multi_stage_dimension,
                    &mut join_builder,
                    &context,
                )?;
            }
            Ok(From::new_from_join(join_builder.build()))
        }
    }
}

impl ProcessableNode for LogicalJoin {
    type ProcessorType<'a> = LogicalJoinProcessor<'a>;
}
