use super::PreAggregationsCompiler;
use super::*;
use crate::logical_plan::visitor::{LogicalPlanRewriter, NodeRewriteResult};
use crate::logical_plan::*;
use crate::plan::FilterItem;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

pub struct PreAggregationOptimizer {
    query_tools: Rc<QueryTools>,
    allow_multi_stage: bool,
    used_pre_aggregations: HashMap<(String, String), Rc<PreAggregation>>,
}

impl PreAggregationOptimizer {
    pub fn new(query_tools: Rc<QueryTools>, allow_multi_stage: bool) -> Self {
        Self {
            query_tools,
            allow_multi_stage,
            used_pre_aggregations: HashMap::new(),
        }
    }

    pub fn try_optimize(&mut self, plan: Rc<Query>) -> Result<Option<Rc<Query>>, CubeError> {
        let cube_names = collect_cube_names_from_node(&plan)?;
        let mut compiler = PreAggregationsCompiler::try_new(self.query_tools.clone(), &cube_names)?;

        let compiled_pre_aggregations = compiler.compile_all_pre_aggregations()?;

        for pre_aggregation in compiled_pre_aggregations.iter() {
            let new_query = self.try_rewrite_query(plan.clone(), pre_aggregation)?;
            if new_query.is_some() {
                return Ok(new_query);
            }
        }

        Ok(None)
    }

    pub fn get_used_pre_aggregations(&self) -> Vec<Rc<PreAggregation>> {
        self.used_pre_aggregations.values().cloned().collect()
    }

    fn try_rewrite_query(
        &mut self,
        query: Rc<Query>,
        pre_aggregation: &Rc<CompiledPreAggregation>,
    ) -> Result<Option<Rc<Query>>, CubeError> {
        if query.multistage_members.is_empty() {
            self.try_rewrite_simple_query(&query, pre_aggregation)
        } else if !self.allow_multi_stage {
            Ok(None)
        } else {
            self.try_rewrite_query_with_multistages(&query, pre_aggregation)
        }
    }

    fn try_rewrite_simple_query(
        &mut self,
        query: &Rc<Query>,
        pre_aggregation: &Rc<CompiledPreAggregation>,
    ) -> Result<Option<Rc<Query>>, CubeError> {
        if self.is_schema_and_filters_match(&query.schema, &query.filter, pre_aggregation)? {
            let mut new_query = query.as_ref().clone();
            new_query.source =
                QuerySource::PreAggregation(self.make_pre_aggregation_source(pre_aggregation)?);
            Ok(Some(Rc::new(new_query)))
        } else {
            Ok(None)
        }
    }

    fn try_rewrite_query_with_multistages(
        &mut self,
        query: &Rc<Query>,
        pre_aggregation: &Rc<CompiledPreAggregation>,
    ) -> Result<Option<Rc<Query>>, CubeError> {
        let rewriter = LogicalPlanRewriter::new();
        let mut has_unrewritten_leaf = false;

        let mut rewritten_multistages = Vec::new();
        for multi_stage in &query.multistage_members {
            let rewritten = rewriter.rewrite_top_down_with(multi_stage.clone(), |plan_node| {
                let res = match plan_node {
                    PlanNode::MultiStageLeafMeasure(multi_stage_leaf_measure) => {
                        if let Some(rewritten) = self.try_rewrite_query(
                            multi_stage_leaf_measure.query.clone(),
                            pre_aggregation,
                        )? {
                            let new_leaf = Rc::new(MultiStageLeafMeasure {
                                measure: multi_stage_leaf_measure.measure.clone(),
                                render_measure_as_state: multi_stage_leaf_measure
                                    .render_measure_as_state
                                    .clone(),
                                render_measure_for_ungrouped: multi_stage_leaf_measure
                                    .render_measure_for_ungrouped
                                    .clone(),
                                time_shifts: multi_stage_leaf_measure.time_shifts.clone(),
                                query: rewritten,
                            });
                            NodeRewriteResult::rewritten(new_leaf.as_plan_node())
                        } else {
                            has_unrewritten_leaf = true;
                            NodeRewriteResult::stop()
                        }
                    }
                    PlanNode::LogicalMultiStageMember(_) => NodeRewriteResult::pass(),
                    _ => NodeRewriteResult::stop(),
                };
                Ok(res)
            })?;
            rewritten_multistages.push(rewritten);
        }

        if has_unrewritten_leaf {
            return Ok(None);
        }

        let source = if let QuerySource::FullKeyAggregate(full_key_aggregate) = &query.source {
            let fk_source = if let Some(resolver_multiplied_measures) =
                &full_key_aggregate.multiplied_measures_resolver
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

                        let pre_aggregation_query = Query {
                            schema: resolver_multiplied_measures.schema.clone(),
                            filter: resolver_multiplied_measures.filter.clone(),
                            modifers: Rc::new(LogicalQueryModifiers {
                                offset: None,
                                limit: None,
                                ungrouped: false,
                                order_by: vec![],
                            }),
                            source: QuerySource::PreAggregation(pre_aggregation_source),
                            multistage_members: vec![],
                        };
                        Some(ResolvedMultipliedMeasures::PreAggregation(Rc::new(
                            pre_aggregation_query,
                        )))
                    } else {
                        return Ok(None);
                    }
                } else {
                    Some(resolver_multiplied_measures.clone())
                }
            } else {
                None
            };
            let mut result = full_key_aggregate.as_ref().clone();
            result.multiplied_measures_resolver = fk_source;
            QuerySource::FullKeyAggregate(Rc::new(result))
        } else {
            query.source.clone()
        };

        let result = Query {
            multistage_members: rewritten_multistages,
            schema: query.schema.clone(),
            filter: query.filter.clone(),
            modifers: query.modifers.clone(),
            source,
        };

        Ok(Some(Rc::new(result)))
    }

    /* fn try_rewrite_multistage(
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
            .find(|&query| &query.name == multi_stage_name)
            .cloned()
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
    } */

    fn make_pre_aggregation_source(
        &mut self,
        pre_aggregation: &Rc<CompiledPreAggregation>,
    ) -> Result<Rc<PreAggregation>, CubeError> {
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
            measures: pre_aggregation.measures.to_vec(),
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
            source: pre_aggregation.source.clone(),
            cube_name: pre_aggregation.cube_name.clone(),
        };
        let result = Rc::new(pre_aggregation);
        self.used_pre_aggregations.insert(
            (result.cube_name.clone(), result.name.clone()),
            result.clone(),
        );
        Ok(result)
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
