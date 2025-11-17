use super::FullKeyAggregateStrategy;
use crate::logical_plan::{FullKeyAggregate, LogicalJoin, ResolvedMultipliedMeasures};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::physical_plan_builder::PushDownBuilderContext;
use crate::plan::{
    Expr, From, FromSource, JoinBuilder, JoinCondition, QualifiedColumnName, Select, SelectBuilder,
    SingleAliasedSource,
};
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_evaluator::{MemberSymbol, ReferencesBuilder};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub(super) struct FullJoinFullKeyAggregateStrategy<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> FullJoinFullKeyAggregateStrategy<'a> {
    pub fn new(builder: &'a PhysicalPlanBuilder) -> Rc<Self> {
        Rc::new(Self { builder })
    }

    fn full_join(
        &self,
        left_source: Rc<Select>,
        right_source: Rc<Select>,
        dimensions: &Vec<Rc<MemberSymbol>>,
    ) -> Result<Rc<From>, CubeError> {
        let left_alias = "q_l".to_string();
        let right_alias = "q_r".to_string();
        let mut join_builder =
            JoinBuilder::new_from_subselect(left_source.clone(), left_alias.clone());

        let conditions = dimensions
            .iter()
            .map(|dim| -> Result<_, CubeError> {
                let alias_in_left_query = left_source.schema().resolve_member_alias(dim);
                let left_query_ref = Expr::Reference(QualifiedColumnName::new(
                    Some(left_alias.clone()),
                    alias_in_left_query,
                ));
                let alias_in_right_query = right_source.schema().resolve_member_alias(dim);
                let right_query_ref = Expr::Reference(QualifiedColumnName::new(
                    Some(right_alias.clone()),
                    alias_in_right_query,
                ));

                Ok(vec![(left_query_ref, right_query_ref)])
            })
            .collect::<Result<Vec<_>, _>>()?;

        join_builder.full_join_subselect(
            right_source.clone(),
            right_alias.clone(),
            JoinCondition::new_dimension_join(conditions, true),
        );
        let result = join_builder.build();
        Ok(From::new_from_join(result))
    }

    fn select_over_join_pair(
        &self,
        from: Rc<From>,
        dimensions: &Vec<Rc<MemberSymbol>>,
        measures: &Vec<Rc<MemberSymbol>>,
        context: &PushDownBuilderContext,
    ) -> Result<Rc<Select>, CubeError> {
        let query_tools = self.builder.query_tools();
        let mut context_factory = SqlNodesFactory::new();
        let references_builder = ReferencesBuilder::new(from.clone());
        let mut select_builder = SelectBuilder::new(from);
        for dimension in dimensions.iter() {
            self.builder.process_query_dimension(
                dimension,
                &references_builder,
                &mut select_builder,
                &mut context_factory,
                &context,
            )?;
        }

        for measure in measures {
            references_builder.resolve_references_for_member(
                measure.clone(),
                &None,
                context_factory.render_references_mut(),
            )?;
            select_builder.add_projection_member(&measure, None);
        }
        let res = Rc::new(select_builder.build(query_tools.clone(), context_factory));
        Ok(res)
    }
}

impl FullKeyAggregateStrategy for FullJoinFullKeyAggregateStrategy<'_> {
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
                        data_queries.push((query, regular_measure_query.schema().measures.clone()));
                    }
                    for multiplied_measure_query in resolve_multiplied_measures
                        .aggregate_multiplied_subqueries
                        .iter()
                    {
                        let query = self
                            .builder
                            .process_node(multiplied_measure_query.as_ref(), &context)?;
                        data_queries
                            .push((query, multiplied_measure_query.schema.measures.clone()));
                    }
                }
                ResolvedMultipliedMeasures::PreAggregation(pre_agg_query) => {
                    let query = self
                        .builder
                        .process_node(pre_agg_query.as_ref(), &context)?;
                    data_queries.push((query, pre_agg_query.schema().measures.clone()));
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
            data_queries.push((data_select, multi_stage_ref.symbols().clone()));
        }

        if data_queries.is_empty() {
            let empty_join = LogicalJoin::builder().build();
            return self.builder.process_node(&empty_join, context);
        }

        if data_queries.len() == 1 {
            let (select, _) = data_queries[0].clone();
            let result = From::new_from_subselect(select, "fk_aggregate".to_string());
            return Ok(result);
        }

        let dimensions = full_key_aggregate
            .schema()
            .all_dimensions()
            .cloned()
            .collect_vec();
        let mut measures = vec![];

        let mut queries_iter = data_queries.into_iter();
        let (left_query, mut query_measures) = queries_iter.next().unwrap();
        measures.append(&mut query_measures);
        let (right_query, mut query_measures) = queries_iter.next().unwrap();
        measures.append(&mut query_measures);
        let mut result = self.full_join(left_query, right_query, &dimensions)?;
        for (query, mut query_measures) in queries_iter {
            let left_query = self.select_over_join_pair(result, &dimensions, &measures, context)?;
            result = self.full_join(left_query, query, &dimensions)?;
            measures.append(&mut query_measures);
        }
        let result_query = self.select_over_join_pair(result, &dimensions, &measures, context)?;

        Ok(From::new_from_subselect(
            result_query,
            "full_aggregate".to_string(),
        ))
    }
}
