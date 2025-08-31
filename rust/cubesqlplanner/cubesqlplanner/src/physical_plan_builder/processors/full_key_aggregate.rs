use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{pretty_print, FullKeyAggregate, ResolvedMultipliedMeasures};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{
    Expr, From, FromSource, JoinBuilder, JoinCondition, QualifiedColumnName, SelectBuilder,
    SingleAliasedSource, Union,
};
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_evaluator::ReferencesBuilder;
use cubenativeutils::CubeError;
use std::rc::Rc;

trait FullKeyAggregateStrategy {
    fn process(
        &self,
        full_key_aggregate: &FullKeyAggregate,
        context: &PushDownBuilderContext,
    ) -> Result<Rc<From>, CubeError>;
}

struct KeysFullKeyAggregateStrategy<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> KeysFullKeyAggregateStrategy<'a> {
    pub fn new(builder: &'a PhysicalPlanBuilder) -> Rc<Self> {
        Rc::new(Self { builder })
    }
}

impl FullKeyAggregateStrategy for KeysFullKeyAggregateStrategy<'_> {
    fn process(
        &self,
        full_key_aggregate: &FullKeyAggregate,
        context: &PushDownBuilderContext,
    ) -> Result<Rc<From>, CubeError> {
        let query_tools = self.builder.query_tools();
        let mut keys_queries = vec![];
        let mut data_queries = vec![];
        let mut keys_context = context.clone();
        keys_context.dimensions_query = true;
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
                        let keys_query = self
                            .builder
                            .process_node(regular_measure_query.as_ref(), &keys_context)?;
                        keys_queries.push(keys_query);
                        let query = self
                            .builder
                            .process_node(regular_measure_query.as_ref(), &context)?;
                        data_queries.push(query);
                    }
                    for multiplied_measure_query in resolve_multiplied_measures
                        .aggregate_multiplied_subqueries
                        .iter()
                    {
                        let keys_query = self
                            .builder
                            .process_node(multiplied_measure_query.as_ref(), &keys_context)?;
                        keys_queries.push(keys_query);
                        let query = self
                            .builder
                            .process_node(multiplied_measure_query.as_ref(), &context)?;
                        data_queries.push(query);
                    }
                }
                ResolvedMultipliedMeasures::PreAggregation(pre_agg_query) => {
                    let keys_query = self
                        .builder
                        .process_node(pre_agg_query.as_ref(), &keys_context)?;
                    keys_queries.push(keys_query);
                    let query = self
                        .builder
                        .process_node(pre_agg_query.as_ref(), &context)?;
                    data_queries.push(query);
                }
            }
        }
        for multi_stage_ref in full_key_aggregate.multi_stage_subquery_refs.iter() {
            let multi_stage_schema = context.get_multi_stage_schema(&multi_stage_ref.name)?;
            let multi_stage_source = SingleAliasedSource::new_from_table_reference(
                multi_stage_ref.name.clone(),
                multi_stage_schema.clone(),
                None,
            );
            let mut keys_select_builder =
                SelectBuilder::new(From::new(FromSource::Single(multi_stage_source.clone())));
            for dim in full_key_aggregate.schema.all_dimensions() {
                let alias = multi_stage_schema.resolve_member_alias(dim);
                let reference = QualifiedColumnName::new(None, alias);
                keys_select_builder.add_projection_member_reference(dim, reference);
            }
            let sql_context = SqlNodesFactory::new();
            keys_select_builder.set_distinct();
            let keys_select =
                Rc::new(keys_select_builder.build(query_tools.clone(), sql_context.clone()));
            keys_queries.push(keys_select);

            let data_select_builder =
                SelectBuilder::new(From::new(FromSource::Single(multi_stage_source)));
            let data_select = Rc::new(data_select_builder.build(query_tools.clone(), sql_context));
            data_queries.push(data_select);
        }
        if data_queries.is_empty() {
            return Err(CubeError::internal(format!(
                "FullKeyAggregate should have at least one source: {}",
                pretty_print(full_key_aggregate)
            )));
        }

        if data_queries.len() == 1 {
            let select = data_queries[0].clone();
            let result = From::new_from_subselect(select, "fk_aggregate".to_string());
            return Ok(result);
        }

        let keys_from = From::new_from_union(
            Rc::new(Union::new_from_subselects(&keys_queries)),
            "pk_aggregate_keys_source".to_string(),
        );
        let references_builder = ReferencesBuilder::new(keys_from.clone());
        let mut keys_select_builder = SelectBuilder::new(keys_from);

        for member in full_key_aggregate.schema.all_dimensions() {
            let alias = references_builder.resolve_alias_for_member(&member.full_name(), &None);
            if alias.is_none() {
                return Err(CubeError::internal(format!(
                    "Source for {} not found in full key aggregate subqueries",
                    member.full_name()
                )));
            }
            let reference = QualifiedColumnName::new(None, alias.unwrap());
            keys_select_builder.add_projection_member_reference(member, reference);
        }
        keys_select_builder.set_distinct();

        let sql_context = SqlNodesFactory::new();
        let keys_select = Rc::new(keys_select_builder.build(query_tools.clone(), sql_context));

        let keys_alias = "fk_aggregate_keys".to_string();

        let mut join_builder =
            JoinBuilder::new_from_subselect(keys_select.clone(), keys_alias.clone());

        for (i, query) in data_queries.into_iter().enumerate() {
            let query_alias = format!("q_{}", i);
            let conditions = full_key_aggregate
                .schema
                .all_dimensions()
                .map(|dim| -> Result<_, CubeError> {
                    let alias_in_keys_query = keys_select.schema().resolve_member_alias(dim);
                    let keys_query_ref = Expr::Reference(QualifiedColumnName::new(
                        Some(keys_alias.clone()),
                        alias_in_keys_query,
                    ));
                    let alias_in_data_query = query.schema().resolve_member_alias(dim);
                    let data_query_ref = Expr::Reference(QualifiedColumnName::new(
                        Some(query_alias.clone()),
                        alias_in_data_query,
                    ));

                    Ok(vec![(keys_query_ref, data_query_ref)])
                })
                .collect::<Result<Vec<_>, _>>()?;

            join_builder.left_join_subselect(
                query,
                query_alias.clone(),
                JoinCondition::new_dimension_join(conditions, true),
            );
        }

        let result = join_builder.build();
        Ok(From::new_from_join(result))
    }
}

struct InnerJoinFullKeyAggregateStrategy<'a> {
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

        for multi_stage_ref in full_key_aggregate.multi_stage_subquery_refs.iter() {
            let multi_stage_schema = context.get_multi_stage_schema(&multi_stage_ref.name)?;
            let multi_stage_source = SingleAliasedSource::new_from_table_reference(
                multi_stage_ref.name.clone(),
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
            return Err(CubeError::internal(format!(
                "FullKeyAggregate should have at least one source: {}",
                pretty_print(full_key_aggregate)
            )));
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
                .schema
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
        let strategy: Rc<dyn FullKeyAggregateStrategy> =
            if full_key_aggregate.schema.has_dimensions() {
                KeysFullKeyAggregateStrategy::new(self.builder)
            } else {
                InnerJoinFullKeyAggregateStrategy::new(self.builder)
            };
        strategy.process(full_key_aggregate, context)
    }
}

impl ProcessableNode for FullKeyAggregate {
    type ProcessorType<'a> = FullKeyAggregateProcessor<'a>;
}
