use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{DimensionInput, QueryInputs};
use crate::physical_plan::{From, JoinBuilder, JoinCondition, SingleSource};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::collectors::collect_cube_names_from_symbols;
use crate::planner::{CubeId, SqlJoinCondition};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub struct QueryInputsProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl QueryInputsProcessor<'_> {
    /// Joins the dimension inputs to the query inputs. Without a join the grid
    /// input is the only thing read.
    pub fn process_add_dimension_inputs(
        &self,
        query_inputs: &QueryInputs,
        dimension_inputs: &[DimensionInput],
        context: &PushDownBuilderContext,
    ) -> Result<Rc<From>, CubeError> {
        let grid_inputs = dimension_inputs
            .iter()
            .filter(|input| !context.is_inline_subquery(input.subquery.id()))
            .collect::<Vec<_>>();
        let Some(join) = query_inputs.join() else {
            if grid_inputs.len() != dimension_inputs.len() {
                return Err(CubeError::internal(
                    "An inline dimension input needs the cube owning its dimensions".to_string(),
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

            let cubes = std::iter::once(root.cube_id())
                .chain(join.joins().iter().map(|item| item.cube().cube_id()))
                .collect_vec();
            let anchors = dimension_inputs
                .iter()
                .map(|input| anchor(input, &cubes))
                .collect::<Result<Vec<_>, _>>()?;
            let join_inputs_at = |position: usize, join_builder: &mut JoinBuilder| {
                dimension_inputs
                    .iter()
                    .zip(anchors.iter())
                    .filter(|(_, anchor)| **anchor == position)
                    .try_for_each(|(input, _)| {
                        self.builder
                            .join_dimension_input(input, join_builder, context)
                    })
            };

            join_inputs_at(0, &mut join_builder)?;
            for (index, item) in join.joins().iter().enumerate() {
                join_builder.left_join_cube(
                    item.cube().clone(),
                    Some(
                        item.cube()
                            .default_alias_with_prefix(query_inputs.alias_prefix()),
                    ),
                    JoinCondition::new_base_join(SqlJoinCondition::try_new(item.on_sql().clone())?),
                );
                join_inputs_at(index + 1, &mut join_builder)?;
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

// A dimension input joins right after the last cube its keys read from; with
// keys on no joined cube it joins after every cube.
fn anchor(input: &DimensionInput, cubes: &[&CubeId]) -> Result<usize, CubeError> {
    let mut anchor = 0;
    for cube in collect_cube_names_from_symbols(&input.keys)? {
        match cubes.iter().position(|joined| **joined == cube) {
            Some(position) => anchor = anchor.max(position),
            None => return Ok(cubes.len() - 1),
        }
    }
    Ok(anchor)
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
