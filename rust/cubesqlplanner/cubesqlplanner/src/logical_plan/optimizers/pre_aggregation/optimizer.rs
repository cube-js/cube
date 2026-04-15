use super::PreAggregationsCompiler;
use super::*;
use crate::logical_plan::visitor::{LogicalPlanRewriter, NodeRewriteResult};
use crate::logical_plan::*;
use crate::plan::FilterItem;
use crate::planner::filter::FilterOp;
use crate::planner::join_hints::JoinHints;
use crate::planner::multi_fact_join_groups::{MeasuresJoinHints, MultiFactJoinGroups};
use crate::planner::planners::multi_stage::TimeShiftState;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::time_dimension::QueryDateTime;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

pub struct PreAggregationUsage {
    pub index: usize,
    pub pre_aggregation: Rc<PreAggregation>,
    pub date_range: Option<(String, String)>,
}

impl PreAggregationUsage {
    pub fn name(&self) -> &String {
        self.pre_aggregation.name()
    }

    pub fn cube_name(&self) -> &String {
        self.pre_aggregation.cube_name()
    }

    pub fn external(&self) -> bool {
        self.pre_aggregation.external()
    }
}

pub struct PreAggregationOptimizer {
    query_tools: Rc<QueryTools>,
    allow_multi_stage: bool,
    usages: Vec<PreAggregationUsage>,
    usage_counter: usize,
}

impl PreAggregationOptimizer {
    pub fn new(query_tools: Rc<QueryTools>, allow_multi_stage: bool) -> Self {
        Self {
            query_tools,
            allow_multi_stage,
            usages: Vec::new(),
            usage_counter: 0,
        }
    }

    pub fn try_optimize(
        &mut self,
        plan: Rc<Query>,
        disable_external_pre_aggregations: bool,
        pre_aggregation_id: Option<&str>,
    ) -> Result<Option<Rc<Query>>, CubeError> {
        let cube_names = collect_cube_names_from_node(&plan)?;
        let mut compiler = PreAggregationsCompiler::try_new(self.query_tools.clone(), &cube_names)?;

        let compiled_pre_aggregations =
            compiler.compile_all_pre_aggregations(disable_external_pre_aggregations)?;

        let filtered_pre_aggregations: Vec<_> = if let Some(id) = pre_aggregation_id {
            compiled_pre_aggregations
                .iter()
                .filter(|pa| format!("{}.{}", pa.cube_name, pa.name) == id)
                .cloned()
                .collect()
        } else {
            compiled_pre_aggregations
        };

        if !plan.multistage_members().is_empty() && self.allow_multi_stage {
            return self.try_rewrite_query_with_multistages(&plan, &filtered_pre_aggregations);
        }

        for pre_aggregation in filtered_pre_aggregations.iter() {
            let new_query = self.try_rewrite_simple_query(&plan, pre_aggregation, None)?;
            if new_query.is_some() {
                return Ok(new_query);
            }
        }

        Ok(None)
    }

    pub fn get_usages(&self) -> &Vec<PreAggregationUsage> {
        &self.usages
    }

    pub fn take_usages(&mut self) -> Vec<PreAggregationUsage> {
        std::mem::take(&mut self.usages)
    }

    fn try_rewrite_simple_query(
        &mut self,
        query: &Rc<Query>,
        pre_aggregation: &Rc<CompiledPreAggregation>,
        date_range: Option<(String, String)>,
    ) -> Result<Option<Rc<Query>>, CubeError> {
        if let Some(matched_measures) =
            self.is_schema_and_filters_match(&query.schema(), &query.filter(), pre_aggregation)?
        {
            let mut new_query = query.as_ref().clone();
            new_query.set_source(
                self.make_pre_aggregation_source(pre_aggregation, &matched_measures, date_range)?
                    .into(),
            );
            Ok(Some(Rc::new(new_query)))
        } else {
            Ok(None)
        }
    }

    fn try_rewrite_leaf_query(
        &mut self,
        query: Rc<Query>,
        compiled_pre_aggregations: &[Rc<CompiledPreAggregation>],
        time_shifts: &TimeShiftState,
    ) -> Result<Option<Rc<Query>>, CubeError> {
        if !query.multistage_members().is_empty() {
            // Nested multi-stage: recurse with full list
            return self.try_rewrite_query_with_multistages(&query, compiled_pre_aggregations);
        }

        for pre_aggregation in compiled_pre_aggregations.iter() {
            let external = pre_aggregation.external.unwrap_or(false);
            let date_range =
                Self::extract_date_range(&query.filter(), &self.query_tools, time_shifts, external);
            let result = self.try_rewrite_simple_query(&query, pre_aggregation, date_range)?;
            if result.is_some() {
                return Ok(result);
            }
        }
        Ok(None)
    }

    fn try_rewrite_query_with_multistages(
        &mut self,
        query: &Rc<Query>,
        compiled_pre_aggregations: &[Rc<CompiledPreAggregation>],
    ) -> Result<Option<Rc<Query>>, CubeError> {
        let rewriter = LogicalPlanRewriter::new();
        let mut has_unrewritten_leaf = false;

        // Save state in case we need to rollback
        let saved_usages_len = self.usages.len();
        let saved_counter = self.usage_counter;

        let mut rewritten_multistages = Vec::new();
        for multi_stage in query.multistage_members() {
            let rewritten = rewriter.rewrite_top_down_with(multi_stage.clone(), |plan_node| {
                let res = match plan_node {
                    PlanNode::MultiStageLeafMeasure(multi_stage_leaf_measure) => {
                        if let Some(rewritten) = self.try_rewrite_leaf_query(
                            multi_stage_leaf_measure.query.clone(),
                            compiled_pre_aggregations,
                            &multi_stage_leaf_measure.time_shifts,
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
            // Rollback usages added during failed attempt
            self.usages.truncate(saved_usages_len);
            self.usage_counter = saved_counter;
            return Ok(None);
        }

        let source = if let QuerySource::FullKeyAggregate(full_key_aggregate) = query.source() {
            let fk_source = if let Some(resolver_multiplied_measures) =
                full_key_aggregate.multiplied_measures_resolver()
            {
                if let ResolvedMultipliedMeasures::ResolveMultipliedMeasures(
                    resolver_multiplied_measures,
                ) = resolver_multiplied_measures
                {
                    // Try each pre-aggregation for the multiplied measures resolver
                    let mut result_source = None;
                    for pre_aggregation in compiled_pre_aggregations.iter() {
                        if let Some(matched_measures) = self.is_schema_and_filters_match(
                            &resolver_multiplied_measures.schema,
                            &resolver_multiplied_measures.filter,
                            pre_aggregation,
                        )? {
                            let date_range = Self::extract_date_range(
                                &resolver_multiplied_measures.filter,
                                &self.query_tools,
                                &TimeShiftState::default(),
                                pre_aggregation.external.unwrap_or(false),
                            );
                            let pre_aggregation_source = self.make_pre_aggregation_source(
                                pre_aggregation,
                                &matched_measures,
                                date_range,
                            )?;

                            let pre_aggregation_query = Query::builder()
                                .schema(resolver_multiplied_measures.schema.clone())
                                .filter(resolver_multiplied_measures.filter.clone())
                                .modifers(Rc::new(LogicalQueryModifiers {
                                    offset: None,
                                    limit: None,
                                    ungrouped: false,
                                    order_by: vec![],
                                }))
                                .source(pre_aggregation_source.into())
                                .build();
                            result_source = Some(ResolvedMultipliedMeasures::PreAggregation(
                                Rc::new(pre_aggregation_query),
                            ));
                            break;
                        }
                    }
                    if result_source.is_none() {
                        // Rollback
                        self.usages.truncate(saved_usages_len);
                        self.usage_counter = saved_counter;
                        return Ok(None);
                    }
                    result_source
                } else {
                    Some(resolver_multiplied_measures.clone())
                }
            } else {
                None
            };
            let result = FullKeyAggregate::builder()
                .schema(full_key_aggregate.schema().clone())
                .use_full_join_and_coalesce(full_key_aggregate.use_full_join_and_coalesce())
                .multiplied_measures_resolver(fk_source)
                .multi_stage_subquery_refs(full_key_aggregate.multi_stage_subquery_refs().clone())
                .build();
            Rc::new(result).into()
        } else {
            query.source().clone()
        };

        // Reject mixed external/non-external pre-aggregation usages
        let new_usages = &self.usages[saved_usages_len..];
        if !new_usages.is_empty() {
            let first_external = new_usages[0].external();
            if new_usages.iter().any(|u| u.external() != first_external) {
                self.usages.truncate(saved_usages_len);
                self.usage_counter = saved_counter;
                return Ok(None);
            }
        }

        let result = Query::builder()
            .multistage_members(rewritten_multistages)
            .schema(query.schema().clone())
            .filter(query.filter().clone())
            .modifers(query.modifers().clone())
            .source(source)
            .build();

        Ok(Some(Rc::new(result)))
    }

    fn make_pre_aggregation_source(
        &mut self,
        pre_aggregation: &Rc<CompiledPreAggregation>,
        matched_measures: &HashSet<String>,
        date_range: Option<(String, String)>,
    ) -> Result<Rc<PreAggregation>, CubeError> {
        let usage_index = self.usage_counter;
        self.usage_counter += 1;

        let filtered_measures: Vec<Rc<MemberSymbol>> = pre_aggregation
            .measures
            .iter()
            .filter(|m| matched_measures.contains(&m.full_name()))
            .cloned()
            .collect();
        let schema = LogicalSchema {
            time_dimensions: vec![],
            dimensions: pre_aggregation
                .dimensions
                .iter()
                .cloned()
                .chain(pre_aggregation.time_dimensions.iter().cloned())
                .chain(pre_aggregation.segments.iter().cloned())
                .collect(),
            measures: filtered_measures.clone(),
            multiplied_measures: HashSet::new(),
        };

        // Set usage_index on the source table so the physical plan can generate unique placeholders
        let source = Self::source_with_usage_index(&pre_aggregation.source, usage_index);

        // Measures are filtered to only those actually consumed during matching.
        // This prevents calculated measures (e.g. amount_per_count) from getting a
        // direct column reference when they should be decomposed to base measures.
        // Dimensions are intentionally NOT filtered: unlike measures (where
        // sum(precomputed_ratio) != sum(a)/sum(b)), extra dimension references
        // are harmless — they're simply unused if the query doesn't select them.
        let pre_aggregation_node = PreAggregation::builder()
            .name(pre_aggregation.name.clone())
            .time_dimensions(pre_aggregation.time_dimensions.clone())
            .dimensions(pre_aggregation.dimensions.clone())
            .segments(pre_aggregation.segments.clone())
            .measures(filtered_measures)
            .schema(Rc::new(schema))
            .external(pre_aggregation.external.unwrap_or_default())
            .granularity(pre_aggregation.granularity.clone())
            .source(source)
            .cube_name(pre_aggregation.cube_name.clone())
            .usage_index(Some(usage_index))
            .build();
        let result = Rc::new(pre_aggregation_node);

        self.usages.push(PreAggregationUsage {
            index: usage_index,
            pre_aggregation: result.clone(),
            date_range,
        });

        Ok(result)
    }

    fn source_with_usage_index(
        source: &Rc<PreAggregationSource>,
        usage_index: usize,
    ) -> Rc<PreAggregationSource> {
        match source.as_ref() {
            PreAggregationSource::Single(table) => {
                Rc::new(PreAggregationSource::Single(PreAggregationTable {
                    usage_index: Some(usage_index),
                    ..table.clone()
                }))
            }
            PreAggregationSource::Union(union) => {
                let items = union
                    .items
                    .iter()
                    .map(|t| {
                        Rc::new(PreAggregationTable {
                            usage_index: Some(usage_index),
                            ..t.as_ref().clone()
                        })
                    })
                    .collect();
                Rc::new(PreAggregationSource::Union(PreAggregationUnion { items }))
            }
            PreAggregationSource::Join(_) => {
                // Join pre-aggregations: usage_index is set on the PreAggregation node itself
                source.clone()
            }
        }
    }

    fn extract_date_range(
        filter: &LogicalFilter,
        query_tools: &Rc<QueryTools>,
        time_shifts: &TimeShiftState,
        external: bool,
    ) -> Option<(String, String)> {
        let precision = query_tools
            .base_tools()
            .driver_tools(external)
            .ok()
            .and_then(|dt| dt.timestamp_precision().ok())
            .unwrap_or(3);
        for item in &filter.time_dimensions_filters {
            if let FilterItem::Item(base_filter) = item {
                if let FilterOp::DateRange(date_range_op) = base_filter.operation() {
                    if let Ok((from, to)) = date_range_op.formatted_date_range(precision) {
                        // Apply time shift for this dimension if present.
                        // SQL renders `column + interval`, so actual data range is `date - interval`.
                        if let Some(interval) = time_shifts
                            .dimensions_shifts
                            .get(&base_filter.member_name())
                            .and_then(|s| s.interval.as_ref())
                        {
                            let tz = query_tools.timezone();
                            let neg = -interval.clone();
                            let shifted_from = QueryDateTime::from_date_str(tz, &from)
                                .and_then(|dt| dt.add_interval(&neg))
                                .map(|dt| dt.default_format())
                                .unwrap_or(from);
                            let shifted_to = QueryDateTime::from_date_str(tz, &to)
                                .and_then(|dt| dt.add_interval(&neg))
                                .map(|dt| dt.default_format())
                                .unwrap_or(to);
                            return Some((shifted_from, shifted_to));
                        }
                        return Some((from, to));
                    }
                }
            }
        }
        None
    }

    fn is_schema_and_filters_match(
        &self,
        schema: &Rc<LogicalSchema>,
        filters: &Rc<LogicalFilter>,
        pre_aggregation: &CompiledPreAggregation,
    ) -> Result<Option<HashSet<String>>, CubeError> {
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
            return Ok(None);
        }
        if match_state == MatchState::NotMatched {
            return Ok(None);
        }
        let matched = self.try_match_measures(
            &all_measures,
            pre_aggregation,
            match_state == MatchState::Partial,
        )?;
        if matched.is_none() {
            return Ok(None);
        }

        if !self.are_join_paths_matching(schema, &all_measures, pre_aggregation)? {
            return Ok(None);
        }

        Ok(matched)
    }

    fn are_join_paths_matching(
        &self,
        schema: &Rc<LogicalSchema>,
        measures: &[Rc<MemberSymbol>],
        pre_aggregation: &CompiledPreAggregation,
    ) -> Result<bool, CubeError> {
        let query_hints = MeasuresJoinHints::builder(&JoinHints::new())
            .add_dimensions(&schema.dimensions)
            .add_dimensions(&schema.time_dimensions)
            .build(measures)?;
        let query_groups = MultiFactJoinGroups::try_new(self.query_tools.clone(), query_hints)?;
        let pre_aggr_groups = &pre_aggregation.multi_fact_join_groups;

        for dim in schema
            .dimensions
            .iter()
            .chain(schema.time_dimensions.iter())
        {
            let query_path = query_groups.resolve_join_path_for_dimension(dim);
            let pre_aggr_path = pre_aggr_groups.resolve_join_path_for_dimension(dim);
            match (query_path, pre_aggr_path) {
                (Some(qp), Some(pp)) if qp != pp => return Ok(false),
                _ => {}
            }
        }

        for measure in measures.iter() {
            let query_path = query_groups.resolve_join_path_for_measure(measure);
            let pre_aggr_path = pre_aggr_groups.resolve_join_path_for_measure(measure);
            match (query_path, pre_aggr_path) {
                (Some(qp), Some(pp)) if qp != pp => return Ok(false),
                _ => {}
            }
        }

        Ok(true)
    }

    fn try_match_measures(
        &self,
        measures: &Vec<Rc<MemberSymbol>>,
        pre_aggregation: &CompiledPreAggregation,
        only_addictive: bool,
    ) -> Result<Option<HashSet<String>>, CubeError> {
        let mut matcher = MeasureMatcher::new(pre_aggregation, only_addictive);
        for measure in measures.iter() {
            if !matcher.try_match(measure)? {
                return Ok(None);
            }
        }
        Ok(Some(matcher.matched_measures().clone()))
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
