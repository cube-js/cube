use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{
    pretty_print, pretty_print_rc, FullKeyAggregate, SimpleQuery, SimpleQuerySource,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{
    Expr, Filter, From, JoinBuilder, JoinCondition, MemberExpression, QualifiedColumnName,
    QueryPlan, Select, SelectBuilder,
};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{BaseMember, MemberSymbolRef};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

pub struct FullKeyAggregateProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, FullKeyAggregate> for FullKeyAggregateProcessor<'a> {
    type PhysycalNode = Rc<From>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        full_key_aggregate: &FullKeyAggregate,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let query_tools = self.builder.query_tools();
        let mut joins = Vec::new();
        if let Some(resolver_multiplied_measures) = &full_key_aggregate.multiplied_measures_resolver
        {
            joins.append(
                &mut self
                    .builder
                    .process_node(resolver_multiplied_measures, context)?,
            );
        }
        /* for subquery_ref in full_key_aggregate.multi_stage_subquery_refs.iter() {
            if let Some(schema) = multi_stage_schemas.get(&subquery_ref.name) {
                joins.push(SingleSource::TableReference(
                    subquery_ref.name.clone(),
                    schema.clone(),
                ));
            } else {
                return Err(CubeError::internal(format!(
                    "MultiStageSubqueryRef not found: {}",
                    subquery_ref.name
                )));
            }
        } */

        if joins.is_empty() {
            return Err(CubeError::internal(format!(
                "FullKeyAggregate should have at least one source: {}",
                pretty_print(full_key_aggregate)
            )));
        }

        let dimensions_for_join = full_key_aggregate
            .join_dimensions
            .iter()
            .map(|dim| -> Result<Rc<dyn BaseMember>, CubeError> {
                dim.clone().as_base_member(query_tools.clone())
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut join_builder = JoinBuilder::new_from_source(joins[0].clone(), format!("q_0"));

        for (i, join) in joins.iter().enumerate().skip(1) {
            let right_alias = format!("q_{}", i);
            let left_schema = joins[i - 1].schema();
            let right_schema = joins[i].schema();
            // TODO every next join should join to all previous dimensions through OR: q_0.a = q_1.a, q_0.a = q_2.a OR q_1.a = q_2.a, ...
            let conditions = dimensions_for_join
                .iter()
                .map(|dim| {
                    (0..i)
                        .map(|left_i| {
                            let left_alias = format!("q_{}", left_i);
                            let alias_in_left_query = left_schema.resolve_member_alias(dim);
                            let left_ref = Expr::Reference(QualifiedColumnName::new(
                                Some(left_alias.clone()),
                                alias_in_left_query,
                            ));
                            let alias_in_right_query = right_schema.resolve_member_alias(dim);
                            let right_ref = Expr::Reference(QualifiedColumnName::new(
                                Some(right_alias.clone()),
                                alias_in_right_query,
                            ));
                            (left_ref, right_ref)
                        })
                        .collect::<Vec<_>>()
                })
                .collect_vec();
            let on = JoinCondition::new_dimension_join(conditions, true);
            let next_alias = format!("q_{}", i);

            join_builder.inner_join_source(join.clone(), next_alias, on);

            /*      TODO: Full join fails even in BigQuery, where it’s theoretically supported. Disabled for now — needs investigation.
            if full_key_aggregate.use_full_join_and_coalesce
                      && self.plan_sql_templates.supports_full_join()
                  {
                      join_builder.full_join_source(join.clone(), next_alias, on);
                  } else {
                      // TODO in case of full join is not supported there should be correct blending query that keeps NULL values
                      join_builder.inner_join_source(join.clone(), next_alias, on);
                  } */
        }

        let result = From::new_from_join(join_builder.build());
        Ok(result)
    }
}

impl ProcessableNode for FullKeyAggregate {
    type ProcessorType<'a> = FullKeyAggregateProcessor<'a>;
}
