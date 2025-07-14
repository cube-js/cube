use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{
    pretty_print, pretty_print_rc, FullKeyAggregate, ResolvedMultipliedMeasures, SimpleQuery,
    SimpleQuerySource,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{
    Expr, Filter, From, JoinBuilder, JoinCondition, MemberExpression, QualifiedColumnName,
    QueryPlan, Select, SelectBuilder, Union,
};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_evaluator::{MemberSymbol, ReferencesBuilder};
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{BaseMember, MemberSymbolRef};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

pub struct FullKeyAggregateProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> FullKeyAggregateProcessor<'a> {
    fn process_subqueries(
        &self,
        full_key_aggregate: &FullKeyAggregate,
        context: &PushDownBuilderContext,
    ) -> Result<(Vec<Rc<Select>>), CubeError> {
        let mut context = context.clone();
        context.required_measures = Some(full_key_aggregate.schema.measures.clone());
        let mut queries = Vec::new();
        if let Some(resolved_multiplied_measures) = &full_key_aggregate.multiplied_measures_resolver
        {
            match resolved_multiplied_measures {
                ResolvedMultipliedMeasures::ResolveMultipliedMeasures(
                    resolve_multiplied_measures,
                ) => {
                    for regular_measure_query in resolve_multiplied_measures
                        .regular_measure_subqueries
                        .iter()
                    {
                        let query = self
                            .builder
                            .process_node(regular_measure_query.as_ref(), &context)?;
                        queries.push(query);
                    }
                    for multiplied_measure_query in resolve_multiplied_measures
                        .aggregate_multiplied_subqueries
                        .iter()
                    {
                        let query = self
                            .builder
                            .process_node(multiplied_measure_query.as_ref(), &context)?;
                        queries.push(query);
                    }
                }
                ResolvedMultipliedMeasures::PreAggregation(simple_query) => todo!(),
            }
        }
        if queries.is_empty() {
            return Err(CubeError::internal(format!(
                "FullKeyAggregate should have at least one source: {}",
                pretty_print(full_key_aggregate)
            )));
        }

        Ok(queries)
    }
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

        let queries = self.process_subqueries(full_key_aggregate, context)?;

        let select = if queries.len() == 1 {
            queries[0].clone()
        } else {
            let from = From::new_from_union(
                Rc::new(Union::new_from_subselects(&queries)),
                "pk_aggregate_source".to_string(),
            );
            let references_builder = ReferencesBuilder::new(from.clone());
            let mut select_builder = SelectBuilder::new(from);

            for member in full_key_aggregate.schema.all_dimensions() {
                let alias = references_builder.resolve_alias_for_member(&member.full_name(), &None);
                if alias.is_none() {
                    return Err(CubeError::internal(format!(
                        "Source for {} not found in full key aggregate subqueries",
                        member.full_name()
                    )));
                }
                let reference = QualifiedColumnName::new(None, alias.unwrap());
                let member_ref = member.clone().as_base_member(query_tools.clone())?;
                select_builder.add_projection_member_reference(&member_ref, reference);
            }

            for member in full_key_aggregate.schema.measures.iter() {
                let alias = references_builder.resolve_alias_for_member(&member.full_name(), &None);
                if alias.is_none() {
                    return Err(CubeError::internal(format!(
                        "Source for {} not found in full key aggregate subqueries",
                        member.full_name()
                    )));
                }
                let reference = QualifiedColumnName::new(None, alias.unwrap());
                let member_ref = member.clone().as_base_member(query_tools.clone())?;
                select_builder.add_projection_group_any_member(&member_ref, reference);
            }

            let group_by = full_key_aggregate
                .schema
                .all_dimensions()
                .map(|symbol| -> Result<_, CubeError> {
                    Ok(Expr::Member(MemberExpression::new(
                        symbol.clone().as_base_member(query_tools.clone())?,
                    )))
                })
                .collect::<Result<Vec<_>, _>>()?;
            select_builder.set_group_by(group_by);
            let context = SqlNodesFactory::new();
            let select = select_builder.build(context);
            Rc::new(select)
        };

        let result = From::new_from_subselect(select, "fk_aggregate".to_string());
        Ok(result)
    }
}

impl ProcessableNode for FullKeyAggregate {
    type ProcessorType<'a> = FullKeyAggregateProcessor<'a>;
}
