use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::QueryInputs;
use crate::physical_plan::{From, JoinBuilder, JoinCondition, SingleSource};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::SqlJoinCondition;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct QueryInputsProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, QueryInputs> for QueryInputsProcessor<'a> {
    type PhysycalNode = Rc<From>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        query_inputs: &QueryInputs,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let multi_stage_dimension = context.get_multi_stage_dimensions()?;
        let Some(join) = query_inputs.join() else {
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
        };

        let root = join.root().clone();
        if join.joins().is_empty()
            && query_inputs.dimension_subqueries().is_empty()
            && query_inputs.subquery_joins().is_empty()
            && multi_stage_dimension.is_none()
        {
            Ok(From::new_from_cube(
                root.clone(),
                Some(root.default_alias_with_prefix(query_inputs.alias_prefix())),
            ))
        } else {
            let mut join_builder = JoinBuilder::new_from_cube(
                root.clone(),
                Some(root.default_alias_with_prefix(query_inputs.alias_prefix())),
            );

            for dimension_subquery in query_inputs
                .dimension_subqueries() //TODO move dimension_subquery to
                .iter()
                .filter(|d| &d.subquery_dimension.cube_id() == root.cube_id())
            {
                self.builder.add_subquery_join(
                    dimension_subquery.clone(),
                    &mut join_builder,
                    context,
                )?;
            }

            for item in join.joins().iter() {
                join_builder.left_join_cube(
                    item.cube().clone(),
                    Some(
                        item.cube()
                            .default_alias_with_prefix(query_inputs.alias_prefix()),
                    ),
                    JoinCondition::new_base_join(SqlJoinCondition::try_new(item.on_sql().clone())?),
                );
                for dimension_subquery in query_inputs
                    .dimension_subqueries()
                    .iter()
                    .filter(|d| &d.subquery_dimension.cube_id() == item.cube().cube_id())
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

            for subquery_join in query_inputs.subquery_joins().iter() {
                let source = SingleSource::RawSubquerySql(subquery_join.sql.clone());
                let on = JoinCondition::new_base_join(SqlJoinCondition::try_new(
                    subquery_join.on_sql.clone(),
                )?);

                if subquery_join.join_type.eq_ignore_ascii_case("INNER") {
                    join_builder.inner_join_source(source, subquery_join.alias.clone(), on);
                } else if subquery_join.join_type.eq_ignore_ascii_case("LEFT") {
                    join_builder.left_join_source(source, subquery_join.alias.clone(), on);
                } else {
                    return Err(CubeError::user(format!(
                        "Unsupported join type '{}' for sub-query join, expected INNER or LEFT",
                        subquery_join.join_type
                    )));
                }
            }

            Ok(From::new_from_join(join_builder.build()))
        }
    }
}

impl ProcessableNode for QueryInputs {
    type ProcessorType<'a> = QueryInputsProcessor<'a>;
}
