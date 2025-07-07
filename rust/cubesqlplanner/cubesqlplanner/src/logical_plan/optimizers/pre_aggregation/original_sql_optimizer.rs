use super::PreAggregationsCompiler;
use super::*;
use crate::logical_plan::*;
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct OriginalSqlOptimizer {
    query_tools: Rc<QueryTools>,
    foud_pre_aggregations: HashMap<String, Rc<CompiledPreAggregation>>,
}

impl OriginalSqlOptimizer {
    pub fn new(query_tools: Rc<QueryTools>) -> Self {
        Self {
            query_tools,
            foud_pre_aggregations: HashMap::new(),
        }
    }

    pub fn try_optimize(&mut self, plan: &Rc<Query>) -> Result<Option<Rc<Query>>, CubeError> {
        let res = match plan.as_ref() {
            Query::SimpleQuery(query) => self
                .try_optimize_simple_query(query)?
                .map(|optimized| Rc::new(Query::SimpleQuery(optimized))),
            Query::FullKeyAggregateQuery(query) => self
                .try_optimize_full_key_aggregate_query(query)?
                .map(|optimized| Rc::new(Query::FullKeyAggregateQuery(optimized))),
        };
        Ok(res)
    }

    fn try_optimize_full_key_aggregate_query(
        &mut self,
        query: &FullKeyAggregateQuery,
    ) -> Result<Option<FullKeyAggregateQuery>, CubeError> {
        let optimized_source = self.try_optimize_full_key_aggregate(&query.source)?;
        if optimized_source.is_some() {
            Ok(Some(FullKeyAggregateQuery {
                multistage_members: query.multistage_members.clone(),
                schema: query.schema.clone(),
                filter: query.filter.clone(),
                modifers: query.modifers.clone(),
                source: optimized_source.unwrap_or_else(|| query.source.clone()),
            }))
        } else {
            Ok(None)
        }
    }

    fn try_optimize_full_key_aggregate(
        &mut self,
        full_key_aggregate: &Rc<FullKeyAggregate>,
    ) -> Result<Option<Rc<FullKeyAggregate>>, CubeError> {
        let res = if let Some(resolver) = &full_key_aggregate.multiplied_measures_resolver {
            if let Some(optimized_resolver) =
                self.try_optimize_resolved_multiplied_measures(resolver)?
            {
                Some(Rc::new(FullKeyAggregate {
                    multiplied_measures_resolver: Some(optimized_resolver),
                    multi_stage_subquery_refs: full_key_aggregate.multi_stage_subquery_refs.clone(),
                    join_dimensions: full_key_aggregate.join_dimensions.clone(),
                    use_full_join_and_coalesce: full_key_aggregate.use_full_join_and_coalesce,
                }))
            } else {
                None
            }
        } else {
            None
        };
        Ok(res)
    }

    fn try_optimize_resolved_multiplied_measures(
        &mut self,
        source: &ResolvedMultipliedMeasures,
    ) -> Result<Option<ResolvedMultipliedMeasures>, CubeError> {
        let res = match source {
            ResolvedMultipliedMeasures::ResolveMultipliedMeasures(resolve_multiplied_measures) => {
                self.try_optimize_multiplied_measures_resolver(resolve_multiplied_measures)?
                    .map(|resolver| ResolvedMultipliedMeasures::ResolveMultipliedMeasures(resolver))
            }
            ResolvedMultipliedMeasures::PreAggregation(_) => None,
        };
        Ok(res)
    }

    fn try_optimize_multiplied_measures_resolver(
        &mut self,
        resolver: &Rc<ResolveMultipliedMeasures>,
    ) -> Result<Option<Rc<ResolveMultipliedMeasures>>, CubeError> {
        let optimized_regular_measure_subqueries = resolver
            .regular_measure_subqueries
            .iter()
            .map(|subquery| self.try_optimize_simple_query(subquery))
            .collect::<Result<Vec<_>, _>>()?;
        let optimized_multiplied_subqueries = resolver
            .aggregate_multiplied_subqueries
            .iter()
            .map(|subquery| self.try_optimize_aggregate_multiplied_subquery(subquery))
            .collect::<Result<Vec<_>, _>>()?;
        let res = if optimized_regular_measure_subqueries
            .iter()
            .any(|subquery| subquery.is_some())
            || optimized_multiplied_subqueries
                .iter()
                .any(|subquery| subquery.is_some())
        {
            Some(Rc::new(ResolveMultipliedMeasures {
                schema: resolver.schema.clone(),
                filter: resolver.filter.clone(),
                regular_measure_subqueries: optimized_regular_measure_subqueries
                    .into_iter()
                    .zip(resolver.regular_measure_subqueries.iter())
                    .map(|(optimized, original)| {
                        optimized.map_or_else(|| original.clone(), |v| Rc::new(v))
                    })
                    .collect(),
                aggregate_multiplied_subqueries: optimized_multiplied_subqueries
                    .into_iter()
                    .zip(resolver.aggregate_multiplied_subqueries.iter())
                    .map(|(optimized, original)| optimized.unwrap_or_else(|| original.clone()))
                    .collect(),
            }))
        } else {
            None
        };
        Ok(res)
    }

    fn try_optimize_simple_query(
        &mut self,
        query: &SimpleQuery,
    ) -> Result<Option<SimpleQuery>, CubeError> {
        let optimized_source = self.try_optimize_simple_query_source(&query.source)?;
        let optimized_dimension_subqueries =
            self.try_optimize_dimension_subqueries(&query.dimension_subqueries)?;
        if optimized_source.is_some() || optimized_dimension_subqueries.is_some() {
            Ok(Some(SimpleQuery {
                source: optimized_source.unwrap_or_else(|| query.source.clone()),
                dimension_subqueries: optimized_dimension_subqueries
                    .unwrap_or_else(|| query.dimension_subqueries.clone()),
                schema: query.schema.clone(),
                filter: query.filter.clone(),
                modifers: query.modifers.clone(),
            }))
        } else {
            Ok(None)
        }
    }

    fn try_optimize_simple_query_source(
        &mut self,
        source: &SimpleQuerySource,
    ) -> Result<Option<SimpleQuerySource>, CubeError> {
        match source {
            SimpleQuerySource::LogicalJoin(join) => Ok(self
                .try_optimize_logical_join(join)?
                .map(|join| SimpleQuerySource::LogicalJoin(join))),
            SimpleQuerySource::PreAggregation(_) => Ok(None),
        }
    }

    fn try_optimize_aggregate_multiplied_subquery(
        &mut self,
        subquery: &Rc<AggregateMultipliedSubquery>,
    ) -> Result<Option<Rc<AggregateMultipliedSubquery>>, CubeError> {
        let optimized_keys_subquery = self.try_optimize_keys_subquery(&subquery.keys_subquery)?;
        let optimized_pk_cube = self.try_optimize_cube(subquery.pk_cube.clone())?;
        let optimized_source = match subquery.source.as_ref() {
            AggregateMultipliedSubquerySouce::Cube => None,
            AggregateMultipliedSubquerySouce::MeasureSubquery(measure_subquery) => self
                .try_optimize_measure_subquery(&measure_subquery)?
                .map(|measure_subquery| {
                    Rc::new(AggregateMultipliedSubquerySouce::MeasureSubquery(
                        measure_subquery,
                    ))
                }),
        };
        let optimized_dimension_subqueries =
            self.try_optimize_dimension_subqueries(&subquery.dimension_subqueries)?;
        if optimized_keys_subquery.is_some()
            || optimized_source.is_some()
            || optimized_dimension_subqueries.is_some()
            || optimized_pk_cube.is_some()
        {
            Ok(Some(Rc::new(AggregateMultipliedSubquery {
                keys_subquery: optimized_keys_subquery
                    .unwrap_or_else(|| subquery.keys_subquery.clone()),
                source: optimized_source.unwrap_or_else(|| subquery.source.clone()),
                pk_cube: optimized_pk_cube.unwrap_or_else(|| subquery.pk_cube.clone()),
                schema: subquery.schema.clone(),
                dimension_subqueries: optimized_dimension_subqueries
                    .unwrap_or_else(|| subquery.dimension_subqueries.clone()),
            })))
        } else {
            Ok(None)
        }
    }

    fn try_optimize_keys_subquery(
        &mut self,
        subquery: &Rc<KeysSubQuery>,
    ) -> Result<Option<Rc<KeysSubQuery>>, CubeError> {
        let optimized_source = self.try_optimize_logical_join(&subquery.source)?;
        let optimized_dimension_subqueries =
            self.try_optimize_dimension_subqueries(&subquery.dimension_subqueries)?;
        if optimized_source.is_some() || optimized_dimension_subqueries.is_some() {
            Ok(Some(Rc::new(KeysSubQuery {
                key_cube_name: subquery.key_cube_name.clone(),
                time_dimensions: subquery.time_dimensions.clone(),
                dimensions: subquery.dimensions.clone(),
                dimension_subqueries: optimized_dimension_subqueries
                    .unwrap_or_else(|| subquery.dimension_subqueries.clone()),
                primary_keys_dimensions: subquery.primary_keys_dimensions.clone(),
                filter: subquery.filter.clone(),
                source: optimized_source.unwrap_or_else(|| subquery.source.clone()),
            })))
        } else {
            Ok(None)
        }
    }

    fn try_optimize_measure_subquery(
        &mut self,
        subquery: &Rc<MeasureSubquery>,
    ) -> Result<Option<Rc<MeasureSubquery>>, CubeError> {
        let optimized_source = self.try_optimize_logical_join(&subquery.source)?;
        let optimized_dimension_subqueries =
            self.try_optimize_dimension_subqueries(&subquery.dimension_subqueries)?;
        if optimized_source.is_some() || optimized_dimension_subqueries.is_some() {
            Ok(Some(Rc::new(MeasureSubquery {
                primary_keys_dimensions: subquery.primary_keys_dimensions.clone(),
                measures: subquery.measures.clone(),
                dimension_subqueries: optimized_dimension_subqueries
                    .unwrap_or_else(|| subquery.dimension_subqueries.clone()),
                source: optimized_source.unwrap_or_else(|| subquery.source.clone()),
            })))
        } else {
            Ok(None)
        }
    }

    fn try_optimize_dimension_subqueries(
        &mut self,
        dimension_subqueries: &Vec<Rc<DimensionSubQuery>>,
    ) -> Result<Option<Vec<Rc<DimensionSubQuery>>>, CubeError> {
        let optimized = dimension_subqueries
            .iter()
            .map(|subquery| self.try_optimize_dimension_subquery(subquery))
            .collect::<Result<Vec<_>, _>>()?;
        let res = if optimized.iter().any(|subquery| subquery.is_some()) {
            Some(
                optimized
                    .into_iter()
                    .zip(dimension_subqueries.iter())
                    .map(|(optimized, original)| optimized.unwrap_or_else(|| original.clone()))
                    .collect(),
            )
        } else {
            None
        };
        Ok(res)
    }

    fn try_optimize_dimension_subquery(
        &mut self,
        subquery: &Rc<DimensionSubQuery>,
    ) -> Result<Option<Rc<DimensionSubQuery>>, CubeError> {
        if let Some(optimized) = self.try_optimize(&subquery.query)? {
            Ok(Some(Rc::new(DimensionSubQuery {
                query: optimized,
                primary_keys_dimensions: subquery.primary_keys_dimensions.clone(),
                subquery_dimension: subquery.subquery_dimension.clone(),
                measure_for_subquery_dimension: subquery.measure_for_subquery_dimension.clone(),
            })))
        } else {
            Ok(None)
        }
    }

    fn try_optimize_logical_join(
        &mut self,
        join: &Rc<LogicalJoin>,
    ) -> Result<Option<Rc<LogicalJoin>>, CubeError> {
        let optimized_root = self.try_optimize_cube(join.root.clone())?;
        let optimized_items = join
            .joins
            .iter()
            .map(|join_item| self.try_optimize_join_item(join_item))
            .collect::<Result<Vec<_>, _>>()?;

        let result =
            if optimized_root.is_some() || optimized_items.iter().any(|item| item.is_some()) {
                Some(Rc::new(LogicalJoin {
                    root: optimized_root.unwrap_or_else(|| join.root.clone()),
                    joins: optimized_items
                        .into_iter()
                        .zip(join.joins.iter())
                        .map(|(optimized, original)| optimized.unwrap_or_else(|| original.clone()))
                        .collect(),
                }))
            } else {
                None
            };
        Ok(result)
    }

    fn try_optimize_cube(&mut self, cube: Rc<Cube>) -> Result<Option<Rc<Cube>>, CubeError> {
        let res = if let Some(found_pre_aggregation) =
            self.find_origin_sql_pre_aggregation(&cube.name)?
        {
            Some(
                cube.with_original_sql_pre_aggregation(OriginalSqlPreAggregation {
                    name: found_pre_aggregation.name.clone(),
                }),
            )
        } else {
            None
        };
        Ok(res)
    }

    fn try_optimize_join_item(
        &mut self,
        join_item: &LogicalJoinItem,
    ) -> Result<Option<LogicalJoinItem>, CubeError> {
        match join_item {
            LogicalJoinItem::CubeJoinItem(cube_join_item) => {
                if let Some(optimized_cube) = self.try_optimize_cube(cube_join_item.cube.clone())? {
                    Ok(Some(LogicalJoinItem::CubeJoinItem(CubeJoinItem {
                        cube: optimized_cube,
                        on_sql: cube_join_item.on_sql.clone(),
                    })))
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn find_origin_sql_pre_aggregation(
        &mut self,
        cube_name: &String,
    ) -> Result<Option<Rc<CompiledPreAggregation>>, CubeError> {
        let res = if let Some(found_pre_aggregation) = self.foud_pre_aggregations.get(cube_name) {
            Some(found_pre_aggregation.clone())
        } else {
            let mut compiler = PreAggregationsCompiler::try_new(
                self.query_tools.clone(),
                &vec![cube_name.clone()],
            )?;
            compiler.compile_origin_sql_pre_aggregation(&cube_name)?
        };
        Ok(res)
    }
}
