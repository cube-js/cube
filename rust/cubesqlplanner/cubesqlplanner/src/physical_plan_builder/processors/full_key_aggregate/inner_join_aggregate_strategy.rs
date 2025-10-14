use super::FullKeyAggregateStrategy;
use crate::logical_plan::{FullKeyAggregate, LogicalJoin, ResolvedMultipliedMeasures};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::physical_plan_builder::PushDownBuilderContext;
use crate::plan::{
    Expr, From, FromSource, JoinBuilder, JoinCondition, QualifiedColumnName, SelectBuilder,
    SingleAliasedSource,
};
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub(super) struct InnerJoinFullKeyAggregateStrategy<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> InnerJoinFullKeyAggregateStrategy<'a> {
    pub fn new(builder: &'a PhysicalPlanBuilder) -> Rc<Self> {
        Rc::new(Self { builder })
    }
}

impl FullKeyAggregateStrategy for InnerJoinFullKeyAggregateStrategy<'_> {
    fn process(
        &self,
        full_key_aggregate: &FullKeyAggregate,
        context: &PushDownBuilderContext,
    ) -> Result<Rc<From>, CubeError> {
        let query_tools = self.builder.query_tools();
        let mut data_queries = vec![];
        if let Some(resolved_multiplied_measures) =
            full_key_aggregate.multiplied_measures_resolver()
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
                        data_queries.push(query);
                    }
                    for multiplied_measure_query in resolve_multiplied_measures
                        .aggregate_multiplied_subqueries
                        .iter()
                    {
                        let query = self
                            .builder
                            .process_node(multiplied_measure_query.as_ref(), &context)?;
                        data_queries.push(query);
                    }
                }
                ResolvedMultipliedMeasures::PreAggregation(pre_agg_query) => {
                    let query = self
                        .builder
                        .process_node(pre_agg_query.as_ref(), &context)?;
                    data_queries.push(query);
                }
            }
        }

        for multi_stage_ref in full_key_aggregate.multi_stage_subquery_refs().iter() {
            let multi_stage_schema = context.get_multi_stage_schema(multi_stage_ref.name())?;
            let multi_stage_source = SingleAliasedSource::new_from_table_reference(
                multi_stage_ref.name().clone(),
                multi_stage_schema.clone(),
                None,
            );
            let sql_context = SqlNodesFactory::new();

            let data_select_builder =
                SelectBuilder::new(From::new(FromSource::Single(multi_stage_source)));
            let data_select = Rc::new(data_select_builder.build(query_tools.clone(), sql_context));
            data_queries.push(data_select);
        }

        if data_queries.is_empty() {
            let empty_join = LogicalJoin::builder().build();
            return self.builder.process_node(&empty_join, context);
        }

        if data_queries.len() == 1 {
            let select = data_queries[0].clone();
            let result = From::new_from_subselect(select, "fk_aggregate".to_string());
            return Ok(result);
        }

        let mut join_builder =
            JoinBuilder::new_from_subselect(data_queries[0].clone(), "q_0".to_string());

        for (i, query) in data_queries.iter().skip(1).enumerate() {
            let prev_alias = format!("q_{}", i);
            let query_alias = format!("q_{}", i + 1);
            let conditions = full_key_aggregate
                .schema()
                .all_dimensions()
                .map(|dim| -> Result<_, CubeError> {
                    let alias_in_prev_query = data_queries[i].schema().resolve_member_alias(dim);
                    let prev_query_ref = Expr::Reference(QualifiedColumnName::new(
                        Some(prev_alias.clone()),
                        alias_in_prev_query,
                    ));
                    let alias_in_data_query = query.schema().resolve_member_alias(dim);
                    let data_query_ref = Expr::Reference(QualifiedColumnName::new(
                        Some(query_alias.clone()),
                        alias_in_data_query,
                    ));

                    Ok(vec![(prev_query_ref, data_query_ref)])
                })
                .collect::<Result<Vec<_>, _>>()?;

            join_builder.inner_join_subselect(
                query.clone(),
                query_alias.clone(),
                JoinCondition::new_dimension_join(conditions, true),
            );
        }

        let result = join_builder.build();
        Ok(From::new_from_join(result))
    }
}
