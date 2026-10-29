use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{DimensionInput, DimensionInputKeys, QueryInputs};
use crate::physical_plan::{From, JoinBuilder, JoinCondition, SingleSource};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::{CubeId, SqlJoinCondition};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct QueryInputsProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl QueryInputsProcessor<'_> {
    /// Joins the dimension inputs to the query inputs: one keyed by primary
    /// keys right after the cube owning its dimensions, one keyed by the grid
    /// after every cube. Without a join the grid input is the only thing read.
    pub fn process_add_dimension_inputs(
        &self,
        query_inputs: &QueryInputs,
        dimension_inputs: &[DimensionInput],
        context: &PushDownBuilderContext,
    ) -> Result<Rc<From>, CubeError> {
        let grid_inputs = dimension_inputs
            .iter()
            .filter(|input| input.keys_kind == DimensionInputKeys::Grid)
            .collect::<Vec<_>>();
        let Some(join) = query_inputs.join() else {
            if grid_inputs.len() != dimension_inputs.len() {
                return Err(CubeError::internal(
                    "A primary-key dimension input needs the cube owning its dimensions"
                        .to_string(),
                ));
            }
            let res = match grid_inputs.as_slice() {
                [] => From::new_empty(),
                [input] => {
                    let cte_name = input.subquery.name();
                    From::new_from_table_reference(
                        cte_name.clone(),
                        PhysicalPlanBuilder::exposed_schema(input),
                        None,
                    )
                }
                _ => {
                    return Err(CubeError::internal(
                        "A query without cubes reads at most one dimension input".to_string(),
                    ))
                }
            };
            return Ok(res);
        };

        let root = join.root().clone();
        if join.joins().is_empty()
            && dimension_inputs.is_empty()
            && query_inputs.subquery_joins().is_empty()
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

            for input in owned_by(dimension_inputs, root.cube_id()) {
                self.builder
                    .add_subquery_join(input, &mut join_builder, context)?;
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
                for input in owned_by(dimension_inputs, item.cube().cube_id()) {
                    self.builder
                        .add_subquery_join(input, &mut join_builder, context)?;
                }
            }

            for input in grid_inputs {
                self.builder
                    .add_multistage_dimension_join(input, &mut join_builder, context)?;
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

fn owned_by<'a>(
    dimension_inputs: &'a [DimensionInput],
    cube_id: &'a CubeId,
) -> impl Iterator<Item = &'a DimensionInput> {
    dimension_inputs.iter().filter(move |input| {
        input.keys_kind == DimensionInputKeys::PrimaryKeys
            && input
                .subquery
                .schema()
                .all_dimensions()
                .any(|dim| &dim.cube_id() == cube_id)
    })
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
        self.process_add_dimension_inputs(query_inputs, &[], context)
    }
}

impl ProcessableNode for QueryInputs {
    type ProcessorType<'a> = QueryInputsProcessor<'a>;
}
