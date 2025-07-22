use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{CubeJoinItem, LogicalJoin, LogicalJoinItem, SimpleQuery};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{From, JoinBuilder, JoinCondition, QueryPlan, Select};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
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
        let query_tools = self.builder.query_tools();
        let root = logical_join.root.cube.clone();
        if logical_join.joins.is_empty() && logical_join.dimension_subqueries.is_empty() {
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
                .dimension_subqueries //TODO move dimension_subquery to
                .iter()
                .filter(|d| &d.subquery_dimension.cube_name() == root.name())
            {
                self.builder.add_subquery_join(
                    dimension_subquery.clone(),
                    &mut join_builder,
                    context,
                )?;
            }
            for join in logical_join.joins.iter() {
                match join {
                    LogicalJoinItem::CubeJoinItem(CubeJoinItem { cube, on_sql }) => {
                        join_builder.left_join_cube(
                            cube.cube.clone(),
                            Some(cube.cube.default_alias_with_prefix(&context.alias_prefix)),
                            JoinCondition::new_base_join(SqlJoinCondition::try_new(
                                query_tools.clone(),
                                on_sql.clone(),
                            )?),
                        );
                        for dimension_subquery in logical_join
                            .dimension_subqueries
                            .iter()
                            .filter(|d| &d.subquery_dimension.cube_name() == cube.cube.name())
                        {
                            self.builder.add_subquery_join(
                                dimension_subquery.clone(),
                                &mut join_builder,
                                context,
                            )?;
                        }
                    }
                }
            }
            Ok(From::new_from_join(join_builder.build()))
        }
    }
}

impl ProcessableNode for LogicalJoin {
    type ProcessorType<'a> = LogicalJoinProcessor<'a>;
}
