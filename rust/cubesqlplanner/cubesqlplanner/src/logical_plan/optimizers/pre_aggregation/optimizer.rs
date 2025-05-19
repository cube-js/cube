use super::*;
use crate::cube_bridge::pre_aggregation_obj::PreAggregationObj;
use crate::logical_plan::*;
use crate::plan::FilterItem;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

#[derive(Clone, Debug, PartialEq)]
pub enum MatchState {
    Partial,
    Full,
    NotMatched,
}

impl MatchState {
    pub fn combine(&self, other: &MatchState) -> MatchState {
        if matches!(self, MatchState::NotMatched) || matches!(other, MatchState::NotMatched) {
            return MatchState::NotMatched;
        }
        if matches!(self, MatchState::Partial) || matches!(other, MatchState::Partial) {
            return MatchState::Partial;
        }
        return MatchState::Full;
    }
}

pub struct PreAggregationOptimizer {
    query_tools: Rc<QueryTools>,
    used_pre_aggregations: HashMap<(String, String), Rc<dyn PreAggregationObj>>,
}

impl PreAggregationOptimizer {
    pub fn new(query_tools: Rc<QueryTools>) -> Self {
        Self {
            query_tools,
            used_pre_aggregations: HashMap::new(),
        }
    }

    pub fn try_optimize(&mut self, plan: Rc<Query>) -> Result<Option<Rc<Query>>, CubeError> {
        let mut cube_names_collector = CubeNamesCollector::new();
        cube_names_collector.collect(&plan)?;
        let cube_names = cube_names_collector.result();

        let mut compiled_pre_aggregations = Vec::new();
        for cube_name in cube_names.iter() {
            let pre_aggregations = self
                .query_tools
                .cube_evaluator()
                .pre_aggregations_for_cube_as_array(cube_name.clone())?;
            for pre_aggregation in pre_aggregations.iter() {
                let compiled = CompiledPreAggregation::try_new(
                    self.query_tools.clone(),
                    cube_name,
                    pre_aggregation.clone(),
                )?;
                compiled_pre_aggregations.push(compiled);
            }
        }

        for pre_aggregation in compiled_pre_aggregations.iter() {
            let new_query = self.try_rewrite_query(plan.clone(), pre_aggregation)?;
            if new_query.is_some() {
                return Ok(new_query);
            }
        }

        Ok(None)
    }

    pub fn get_used_pre_aggregations(&self) -> Vec<Rc<dyn PreAggregationObj>> {
        self.used_pre_aggregations.values().cloned().collect()
    }

    fn try_rewrite_query(
        &mut self,
        query: Rc<Query>,
        pre_aggregation: &Rc<CompiledPreAggregation>,
    ) -> Result<Option<Rc<Query>>, CubeError> {
        match query.as_ref() {
            Query::SimpleQuery(query) => self.try_rewrite_simple_query(query, pre_aggregation),
            Query::FullKeyAggregateQuery(query) => {
                self.try_rewrite_full_key_aggregate_query(query, pre_aggregation)
            }
        }
    }

    fn try_rewrite_simple_query(
        &mut self,
        query: &SimpleQuery,
        pre_aggregation: &Rc<CompiledPreAggregation>,
    ) -> Result<Option<Rc<Query>>, CubeError> {
        if self.is_schema_and_filters_match(&query.schema, &query.filter, pre_aggregation)? {
            let mut new_query = SimpleQuery::clone(&query);
            new_query.source = SimpleQuerySource::PreAggregation(
                self.make_pre_aggregation_source(pre_aggregation)?,
            );
            Ok(Some(Rc::new(Query::SimpleQuery(new_query))))
        } else {
            Ok(None)
        }
    }

    fn try_rewrite_full_key_aggregate_query(
        &mut self,
        query: &FullKeyAggregateQuery,
        pre_aggregation: &Rc<CompiledPreAggregation>,
    ) -> Result<Option<Rc<Query>>, CubeError> {
        if !query.multistage_members.is_empty() {
            return self
                .try_rewrite_full_key_aggregate_query_with_multi_stages(query, pre_aggregation);
        }

        if self.is_schema_and_filters_match(&query.schema, &query.filter, pre_aggregation)? {
            let source = SimpleQuerySource::PreAggregation(
                self.make_pre_aggregation_source(pre_aggregation)?,
            );
            let new_query = SimpleQuery {
                schema: query.schema.clone(),
                dimension_subqueries: vec![],
                filter: query.filter.clone(),
                offset: query.offset,
                limit: query.limit,
                ungrouped: query.ungrouped,
                order_by: query.order_by.clone(),
                source,
            };
            Ok(Some(Rc::new(Query::SimpleQuery(new_query))))
        } else {
            Ok(None)
        }
    }

    fn try_rewrite_full_key_aggregate_query_with_multi_stages(
        &mut self,
        query: &FullKeyAggregateQuery,
        pre_aggregation: &Rc<CompiledPreAggregation>,
    ) -> Result<Option<Rc<Query>>, CubeError> {
        let used_multi_stage_symbols = self.collect_multi_stage_symbols(&query.source);
        let mut multi_stages_queries = query.multistage_members.clone();
        let mut rewrited_multistage = multi_stages_queries
            .iter()
            .map(|query| (query.name.clone(), false))
            .collect::<HashMap<_, _>>();

        for (_, multi_stage_name) in used_multi_stage_symbols.iter() {
            self.try_rewrite_multistage(
                multi_stage_name,
                &mut multi_stages_queries,
                &mut rewrited_multistage,
                pre_aggregation,
            )?;
        }
        let all_multi_stage_rewrited = rewrited_multistage.values().all(|v| *v);
        if !all_multi_stage_rewrited {
            return Ok(None);
        }

        let source = if let Some(resolver_multiplied_measures) =
            &query.source.multiplied_measures_resolver
        {
            if let ResolvedMultipliedMeasures::ResolveMultipliedMeasures(
                resolver_multiplied_measures,
            ) = resolver_multiplied_measures
            {
                if self.is_schema_and_filters_match(
                    &resolver_multiplied_measures.schema,
                    &resolver_multiplied_measures.filter,
                    &pre_aggregation,
                )? {
                    let pre_aggregation_source =
                        self.make_pre_aggregation_source(pre_aggregation)?;

                    let pre_aggregation_query = SimpleQuery {
                        schema: resolver_multiplied_measures.schema.clone(),
                        dimension_subqueries: vec![],
                        filter: resolver_multiplied_measures.filter.clone(),
                        offset: None,
                        limit: None,
                        ungrouped: false,
                        order_by: vec![],
                        source: SimpleQuerySource::PreAggregation(pre_aggregation_source),
                    };
                    Rc::new(FullKeyAggregate {
                        join_dimensions: query.source.join_dimensions.clone(),
                        use_full_join_and_coalesce: query.source.use_full_join_and_coalesce,
                        multiplied_measures_resolver: Some(
                            ResolvedMultipliedMeasures::PreAggregation(Rc::new(
                                pre_aggregation_query,
                            )),
                        ),
                        multi_stage_subquery_refs: query.source.multi_stage_subquery_refs.clone(),
                    })
                } else {
                    return Ok(None);
                }
            } else {
                query.source.clone()
            }
        } else {
            query.source.clone()
        };

        let result = FullKeyAggregateQuery {
            multistage_members: multi_stages_queries,
            schema: query.schema.clone(),
            filter: query.filter.clone(),
            offset: query.offset,
            limit: query.limit,
            ungrouped: query.ungrouped,
            order_by: query.order_by.clone(),
            source,
        };
        Ok(Some(Rc::new(Query::FullKeyAggregateQuery(result))))
    }

    fn try_rewrite_multistage(
        &mut self,
        multi_stage_name: &String,
        multi_stage_queries: &mut Vec<Rc<LogicalMultiStageMember>>,
        rewrited_multistage: &mut HashMap<String, bool>,
        pre_aggregation: &Rc<CompiledPreAggregation>,
    ) -> Result<(), CubeError> {
        if rewrited_multistage
            .get(multi_stage_name)
            .cloned()
            .unwrap_or(false)
        {
            return Ok(());
        }

        if let Some(multi_stage_item) = multi_stage_queries
            .iter()
            .cloned()
            .find(|query| &query.name == multi_stage_name)
        {
            match &multi_stage_item.member_type {
                MultiStageMemberLogicalType::LeafMeasure(multi_stage_leaf_measure) => self
                    .try_rewrite_multistage_leaf_measure(
                        multi_stage_name,
                        multi_stage_leaf_measure,
                        multi_stage_queries,
                        rewrited_multistage,
                        pre_aggregation,
                    )?,
                MultiStageMemberLogicalType::MeasureCalculation(
                    multi_stage_measure_calculation,
                ) => self.try_rewrite_multistage_measure_calculation(
                    multi_stage_name,
                    multi_stage_measure_calculation,
                    multi_stage_queries,
                    rewrited_multistage,
                    pre_aggregation,
                )?,
                MultiStageMemberLogicalType::GetDateRange(multi_stage_get_date_range) => self
                    .try_rewrite_multistage_get_date_range(
                        multi_stage_name,
                        multi_stage_get_date_range,
                        multi_stage_queries,
                        rewrited_multistage,
                        pre_aggregation,
                    )?,
                MultiStageMemberLogicalType::TimeSeries(multi_stage_time_series) => self
                    .try_rewrite_multistage_time_series(
                        multi_stage_name,
                        multi_stage_time_series,
                        multi_stage_queries,
                        rewrited_multistage,
                        pre_aggregation,
                    )?,
                MultiStageMemberLogicalType::RollingWindow(multi_stage_rolling_window) => self
                    .try_rewrite_multistage_rolling_window(
                        multi_stage_name,
                        multi_stage_rolling_window,
                        multi_stage_queries,
                        rewrited_multistage,
                        pre_aggregation,
                    )?,
            }
        }

        Ok(())
    }

    fn try_rewrite_multistage_measure_calculation(
        &mut self,
        multi_stage_name: &String,
        multi_stage_measure_calculation: &MultiStageMeasureCalculation,
        multi_stage_queries: &mut Vec<Rc<LogicalMultiStageMember>>,
        rewrited_multistage: &mut HashMap<String, bool>,
        pre_aggregation: &Rc<CompiledPreAggregation>,
    ) -> Result<(), CubeError> {
        let used_multi_stage_symbols =
            self.collect_multi_stage_symbols(&multi_stage_measure_calculation.source);
        for (_, multi_stage_name) in used_multi_stage_symbols.iter() {
            self.try_rewrite_multistage(
                multi_stage_name,
                multi_stage_queries,
                rewrited_multistage,
                pre_aggregation,
            )?;
        }
        rewrited_multistage.insert(multi_stage_name.clone(), true);
        Ok(())
    }

    fn try_rewrite_multistage_rolling_window(
        &mut self,
        multi_stage_name: &String,
        multi_stage_rolling_window: &MultiStageRollingWindow,
        multi_stage_queries: &mut Vec<Rc<LogicalMultiStageMember>>,
        rewrited_multistage: &mut HashMap<String, bool>,
        pre_aggregation: &Rc<CompiledPreAggregation>,
    ) -> Result<(), CubeError> {
        self.try_rewrite_multistage(
            &multi_stage_rolling_window.time_series_input.name,
            multi_stage_queries,
            rewrited_multistage,
            pre_aggregation,
        )?;
        self.try_rewrite_multistage(
            &multi_stage_rolling_window.measure_input.name,
            multi_stage_queries,
            rewrited_multistage,
            pre_aggregation,
        )?;
        rewrited_multistage.insert(multi_stage_name.clone(), true);
        Ok(())
    }

    fn try_rewrite_multistage_time_series(
        &mut self,
        multi_stage_name: &String,
        multi_stage_time_series: &MultiStageTimeSeries,
        multi_stage_queries: &mut Vec<Rc<LogicalMultiStageMember>>,
        rewrited_multistage: &mut HashMap<String, bool>,
        pre_aggregation: &Rc<CompiledPreAggregation>,
    ) -> Result<(), CubeError> {
        if let Some(get_date_range_ref) = &multi_stage_time_series.get_date_range_multistage_ref {
            self.try_rewrite_multistage(
                &get_date_range_ref,
                multi_stage_queries,
                rewrited_multistage,
                pre_aggregation,
            )?;
        }
        rewrited_multistage.insert(multi_stage_name.clone(), true);
        Ok(())
    }

    fn try_rewrite_multistage_get_date_range(
        &mut self,
        _multi_stage_name: &String,
        _multi_stage_get_date_range: &MultiStageGetDateRange,
        _multi_stage_queries: &mut Vec<Rc<LogicalMultiStageMember>>,
        _rewrited_multistage: &mut HashMap<String, bool>,
        _pre_aggregation: &Rc<CompiledPreAggregation>,
    ) -> Result<(), CubeError> {
        Ok(()) //TODO
    }

    fn try_rewrite_multistage_leaf_measure(
        &mut self,
        multi_stage_name: &String,
        multi_stage_leaf_measure: &MultiStageLeafMeasure,
        multi_stage_queries: &mut Vec<Rc<LogicalMultiStageMember>>,
        rewrited_multistage: &mut HashMap<String, bool>,
        pre_aggregation: &Rc<CompiledPreAggregation>,
    ) -> Result<(), CubeError> {
        if let Some(rewritten) =
            self.try_rewrite_query(multi_stage_leaf_measure.query.clone(), pre_aggregation)?
        {
            let new_leaf = MultiStageLeafMeasure {
                measure: multi_stage_leaf_measure.measure.clone(),
                render_measure_as_state: multi_stage_leaf_measure.render_measure_as_state.clone(),
                render_measure_for_ungrouped: multi_stage_leaf_measure
                    .render_measure_for_ungrouped
                    .clone(),
                time_shifts: multi_stage_leaf_measure.time_shifts.clone(),
                query: rewritten,
            };
            let new_multistage = Rc::new(LogicalMultiStageMember {
                name: multi_stage_name.clone(),
                member_type: MultiStageMemberLogicalType::LeafMeasure(new_leaf),
            });

            rewrited_multistage.insert(multi_stage_name.clone(), true);
            if let Some(query) = multi_stage_queries
                .iter_mut()
                .find(|query| &query.name == multi_stage_name)
            {
                *query = new_multistage;
            }
            Ok(())
        } else {
            Ok(())
        }
    }

    fn collect_multi_stage_symbols(&self, source: &FullKeyAggregate) -> HashMap<String, String> {
        let mut symbols = HashMap::new();
        for source in source.multi_stage_subquery_refs.iter() {
            for symbol in source.symbols.iter() {
                symbols.insert(symbol.full_name(), source.name.clone());
            }
        }
        symbols
    }

    fn make_pre_aggregation_source(
        &mut self,
        pre_aggregation: &Rc<CompiledPreAggregation>,
    ) -> Result<Rc<PreAggregation>, CubeError> {
        let pre_aggregation_obj = self.query_tools.base_tools().get_pre_aggregation_by_name(
            pre_aggregation.cube_name.clone(),
            pre_aggregation.name.clone(),
        )?;
        if let Some(table_name) = &pre_aggregation_obj.static_data().table_name {
            let schema = LogicalSchema {
                time_dimensions: vec![],
                dimensions: pre_aggregation
                    .dimensions
                    .iter()
                    .cloned()
                    .chain(
                        pre_aggregation
                            .time_dimensions
                            .iter()
                            .map(|(d, _)| d.clone()),
                    )
                    .collect(),
                measures: pre_aggregation.measures.iter().cloned().collect(),
                multiplied_measures: HashSet::new(),
            };
            let pre_aggregation = PreAggregation {
                name: pre_aggregation.name.clone(),
                time_dimensions: pre_aggregation.time_dimensions.clone(),
                dimensions: pre_aggregation.dimensions.clone(),
                measures: pre_aggregation.measures.clone(),
                schema: Rc::new(schema),
                external: pre_aggregation.external.unwrap_or_default(),
                granularity: pre_aggregation.granularity.clone(),
                table_name: table_name.clone(),
                cube_name: pre_aggregation.cube_name.clone(),
            };
            self.used_pre_aggregations.insert(
                (
                    pre_aggregation.cube_name.clone(),
                    pre_aggregation.name.clone(),
                ),
                pre_aggregation_obj.clone(),
            );
            Ok(Rc::new(pre_aggregation))
        } else {
            Err(CubeError::internal(format!(
                "Cannot find pre aggregation object for cube {} and name {}",
                pre_aggregation.cube_name, pre_aggregation.name
            )))
        }
    }

    fn is_schema_and_filters_match(
        &self,
        schema: &Rc<LogicalSchema>,
        filters: &Rc<LogicalFilter>,
        pre_aggregation: &CompiledPreAggregation,
    ) -> Result<bool, CubeError> {
        let helper = OptimizerHelper::new();

        let match_state = self.match_dimensions(
            &schema.dimensions,
            &schema.time_dimensions,
            &filters.dimensions_filters,
            &filters.time_dimensions_filters,
            &filters.segments,
            pre_aggregation,
        )?;

        let all_measures = helper.all_measures(schema, filters);
        if !schema.multiplied_measures.is_empty() && match_state == MatchState::Partial {
            return Ok(false);
        }
        if match_state == MatchState::NotMatched {
            return Ok(false);
        }
        let measures_match = self.try_match_measures(
            &all_measures,
            pre_aggregation,
            match_state == MatchState::Partial,
        )?;
        Ok(measures_match)
    }

    fn try_match_measures(
        &self,
        measures: &Vec<Rc<MemberSymbol>>,
        pre_aggregation: &CompiledPreAggregation,
        only_addictive: bool,
    ) -> Result<bool, CubeError> {
        let matcher = MeasureMatcher::new(pre_aggregation, only_addictive);
        for measure in measures.iter() {
            if !matcher.try_match(measure)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn match_dimensions(
        &self,
        dimensions: &Vec<Rc<MemberSymbol>>,
        time_dimensions: &Vec<Rc<MemberSymbol>>,
        filters: &Vec<FilterItem>,
        time_dimension_filters: &Vec<FilterItem>,
        segments: &Vec<FilterItem>,
        pre_aggregation: &CompiledPreAggregation,
    ) -> Result<MatchState, CubeError> {
        let mut matcher = DimensionMatcher::new(self.query_tools.clone(), pre_aggregation);
        matcher.try_match(
            dimensions,
            time_dimensions,
            filters,
            time_dimension_filters,
            segments,
        )?;
        let result = matcher.result();
        Ok(result)
    }
}
