use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{FromJoin, FromShape, JoinOn, LogicalFrom, Relation};
use crate::physical_plan::{From, JoinBuilder, JoinCondition, SingleSource};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::SqlJoinCondition;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct LogicalFromProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl LogicalFromProcessor<'_> {
    fn process_chain(
        &self,
        root: &Relation,
        joins: &[FromJoin],
        context: &PushDownBuilderContext,
    ) -> Result<Rc<From>, CubeError> {
        let mut join_builder = match root {
            Relation::Cube { cube, alias } if joins.is_empty() => {
                return Ok(From::new_from_cube(cube.clone(), Some(alias.clone())));
            }
            Relation::Cube { cube, alias } => {
                JoinBuilder::new_from_cube(cube.clone(), Some(alias.clone()))
            }
            Relation::Subquery(input) if joins.is_empty() => {
                return Ok(From::new_from_table_reference(
                    input.name().clone(),
                    context.get_cte_schema(input)?,
                    None,
                ));
            }
            Relation::PreAggregation(pre_aggregation) if joins.is_empty() => {
                return self.builder.process_node(pre_aggregation.as_ref(), context);
            }
            Relation::Subquery(_) | Relation::PreAggregation(_) | Relation::RawSql { .. } => {
                return Err(CubeError::internal(
                    "Only a cube can be the root of a join".to_string(),
                ));
            }
        };

        for join in joins.iter() {
            match (join.relation(), join.on()) {
                (Relation::Cube { cube, alias }, JoinOn::Sql(on_sql)) => {
                    join_builder.left_join_cube(
                        cube.clone(),
                        Some(alias.clone()),
                        JoinCondition::new_base_join(SqlJoinCondition::try_new(on_sql.clone())?),
                    );
                }
                // An inline subquery renders its body in place; a CTE is read by name.
                (Relation::Subquery(input), JoinOn::Keys { keys, .. }) => {
                    if context.is_inline(input.id()) {
                        self.builder
                            .add_subquery_join(input, keys, &mut join_builder, context)?;
                    } else {
                        self.builder.add_multistage_dimension_join(
                            input,
                            keys,
                            &mut join_builder,
                            context,
                        )?;
                    }
                }
                (
                    Relation::RawSql {
                        sql,
                        alias,
                        join_type,
                    },
                    JoinOn::Sql(on_sql),
                ) => {
                    let source = SingleSource::RawSubquerySql(sql.clone());
                    let on =
                        JoinCondition::new_base_join(SqlJoinCondition::try_new(on_sql.clone())?);
                    if join_type.eq_ignore_ascii_case("INNER") {
                        join_builder.inner_join_source(source, alias.clone(), on);
                    } else if join_type.eq_ignore_ascii_case("LEFT") {
                        join_builder.left_join_source(source, alias.clone(), on);
                    } else {
                        return Err(CubeError::user(format!(
                            "Unsupported join type '{}' for sub-query join, expected INNER or LEFT",
                            join_type
                        )));
                    }
                }
                _ => {
                    return Err(CubeError::internal(
                        "Unsupported relation and condition in a join".to_string(),
                    ))
                }
            }
        }
        Ok(From::new_from_join(join_builder.build()))
    }
}

impl<'a> LogicalNodeProcessor<'a, LogicalFrom> for LogicalFromProcessor<'a> {
    type PhysycalNode = Rc<From>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        from: &LogicalFrom,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        match from.shape() {
            FromShape::Empty => Ok(From::new_empty()),
            FromShape::Merge(merge) => self.builder.process_node(merge.as_ref(), context),
            FromShape::Chain { root, joins } => self.process_chain(root, joins, context),
        }
    }
}

impl ProcessableNode for LogicalFrom {
    type ProcessorType<'a> = LogicalFromProcessor<'a>;
}
