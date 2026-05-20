use super::PreAggregationsCompiler;
use super::*;
use crate::logical_plan::*;
use crate::planner::filter::FilterItem;
use crate::planner::filter::FilterOp;
use crate::planner::join_hints::JoinHints;
use crate::planner::multi_fact_join_groups::{MeasuresJoinHints, MultiFactJoinGroups};
use crate::planner::planners::multi_stage::TimeShiftState;
use crate::planner::query_tools::QueryTools;
use crate::planner::time_dimension::QueryDateTime;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::{HashMap, HashSet, VecDeque};
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
        plan: Rc<LogicalPlan>,
        disable_external_pre_aggregations: bool,
        pre_aggregation_id: Option<&str>,
    ) -> Result<Option<Rc<LogicalPlan>>, CubeError> {
        let cube_names = collect_cube_names_from_plan(&plan)?;
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

        self.try_rewrite_plan(
            &plan,
            &filtered_pre_aggregations,
            &TimeShiftState::default(),
        )
    }

    pub fn get_usages(&self) -> &Vec<PreAggregationUsage> {
        &self.usages
    }

    pub fn take_usages(&mut self) -> Vec<PreAggregationUsage> {
        std::mem::take(&mut self.usages)
    }

    /// Try to rewrite a whole `LogicalPlan`. Attempts a single-source
    /// match against `plan.root` first (collapses the whole plan to a
    /// `PreAggregationLeaf` and drops bundled CTEs); falls back to
    /// walking the CTE graph from the root's FK refs.
    fn try_rewrite_plan(
        &mut self,
        plan: &Rc<LogicalPlan>,
        compiled_pre_aggregations: &[Rc<CompiledPreAggregation>],
        time_shifts: &TimeShiftState,
    ) -> Result<Option<Rc<LogicalPlan>>, CubeError> {
        let root = plan.root();
        for pre_aggregation in compiled_pre_aggregations.iter() {
            let external = pre_aggregation.external.unwrap_or(false);
            let date_range =
                Self::extract_date_range(&root.filter(), &self.query_tools, time_shifts, external);
            if let Some(rewritten_root) =
                self.try_rewrite_simple_query(root, pre_aggregation, date_range)?
            {
                // Root collapsed to PreAggregationLeaf — bundled CTEs orphan.
                return Ok(Some(LogicalPlan::new(vec![], rewritten_root)));
            }
        }

        if self.allow_multi_stage && !plan.ctes().is_empty() {
            return self.try_rewrite_plan_via_graph(plan, compiled_pre_aggregations);
        }

        Ok(None)
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
            let source =
                self.make_pre_aggregation_source(pre_aggregation, &matched_measures, date_range)?;
            let new_query = Query::builder()
                .schema(query.schema().clone())
                .filter(query.filter().clone())
                .modifers(query.modifers().clone())
                .source(source.into())
                .kind(QueryKind::PreAggregationLeaf)
                .build();
            Ok(Some(Rc::new(new_query)))
        } else {
            Ok(None)
        }
    }

    fn try_rewrite_schema_and_filter(
        &mut self,
        schema: &Rc<LogicalSchema>,
        filter: &Rc<LogicalFilter>,
        compiled_pre_aggregations: &[Rc<CompiledPreAggregation>],
    ) -> Result<Option<Rc<Query>>, CubeError> {
        for pre_aggregation in compiled_pre_aggregations.iter() {
            let external = pre_aggregation.external.unwrap_or(false);
            let date_range = Self::extract_date_range(
                filter,
                &self.query_tools,
                &TimeShiftState::default(),
                external,
            );
            if let Some(matched_measures) =
                self.is_schema_and_filters_match(schema, filter, pre_aggregation)?
            {
                let source = self.make_pre_aggregation_source(
                    pre_aggregation,
                    &matched_measures,
                    date_range,
                )?;
                let new_query = Query::builder()
                    .schema(schema.clone())
                    .filter(filter.clone())
                    .modifers(Rc::new(LogicalQueryModifiers::default()))
                    .source(source.into())
                    .kind(QueryKind::PreAggregationLeaf)
                    .build();
                return Ok(Some(Rc::new(new_query)));
            }
        }
        Ok(None)
    }

    /// Walk the CTE graph from `plan.root`'s FK refs by name. Each
    /// reachable member's body is rewritten according to its role; refs
    /// only declared in unreachable members are pruned out of the result.
    fn try_rewrite_plan_via_graph(
        &mut self,
        plan: &Rc<LogicalPlan>,
        compiled_pre_aggregations: &[Rc<CompiledPreAggregation>],
    ) -> Result<Option<Rc<LogicalPlan>>, CubeError> {
        let saved_usages_len = self.usages.len();
        let saved_counter = self.usage_counter;

        let root_filter = plan.root().filter().clone();
        let name_to_idx: HashMap<&str, usize> = plan
            .ctes()
            .iter()
            .enumerate()
            .map(|(i, m)| (m.name.as_str(), i))
            .collect();

        let mut rewritten: HashMap<String, MultiStageMemberBody> = HashMap::new();
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        for r in Self::query_source_refs(plan.root()) {
            queue.push_back(r);
        }

        while let Some(name) = queue.pop_front() {
            if !visited.insert(name.clone()) {
                continue;
            }
            let Some(&idx) = name_to_idx.get(name.as_str()) else {
                // Ref not backed by a member in this pool (shouldn't
                // happen with well-formed plans). Skip.
                continue;
            };
            let member = &plan.ctes()[idx];
            match self.visit_member_body(
                &member.body,
                &root_filter,
                compiled_pre_aggregations,
                &mut queue,
            )? {
                CteRewriteOutcome::Rewritten(new_body) => {
                    rewritten.insert(name, new_body);
                }
                CteRewriteOutcome::Keep => {
                    rewritten.insert(name, member.body.clone());
                }
                CteRewriteOutcome::NotMatched => {
                    self.usages.truncate(saved_usages_len);
                    self.usage_counter = saved_counter;
                    return Ok(None);
                }
            }
        }

        // Reject mixed external/non-external pre-aggregation usages.
        let new_usages = &self.usages[saved_usages_len..];
        if !new_usages.is_empty() {
            let first_external = new_usages[0].external();
            if new_usages.iter().any(|u| u.external() != first_external) {
                self.usages.truncate(saved_usages_len);
                self.usage_counter = saved_counter;
                return Ok(None);
            }
        }

        // Preserve original CTE order; drop members that were unreachable
        // from the root after rewrites (they're orphans of replaced bodies).
        let new_ctes: Vec<_> = plan
            .ctes()
            .iter()
            .filter_map(|m| {
                rewritten.get(&m.name).map(|body| {
                    Rc::new(LogicalMultiStageMember {
                        name: m.name.clone(),
                        body: body.clone(),
                    })
                })
            })
            .collect();

        Ok(Some(LogicalPlan::new(new_ctes, plan.root().clone())))
    }

    /// Compute the rewrite outcome for a single CTE body and push the
    /// names of further refs it transitively reaches into `queue` (only
    /// when the body is kept — replaced bodies break the chain).
    fn visit_member_body(
        &mut self,
        body: &MultiStageMemberBody,
        root_filter: &Rc<LogicalFilter>,
        compiled_pre_aggregations: &[Rc<CompiledPreAggregation>],
        queue: &mut VecDeque<String>,
    ) -> Result<CteRewriteOutcome, CubeError> {
        match body {
            MultiStageMemberBody::Query(q) => match q.kind().pre_agg_rewrite() {
                PreAggregationRewriteRole::NoRewrite => Ok(CteRewriteOutcome::Keep),
                PreAggregationRewriteRole::PassThrough => {
                    for r in Self::query_source_refs(q) {
                        queue.push_back(r);
                    }
                    Ok(CteRewriteOutcome::Keep)
                }
                PreAggregationRewriteRole::Leaf => {
                    let time_shifts = q.modifers().time_shifts.clone();
                    let mut matched: Option<Rc<Query>> = None;
                    for pre_aggregation in compiled_pre_aggregations.iter() {
                        let external = pre_aggregation.external.unwrap_or(false);
                        let date_range = Self::extract_date_range(
                            &q.filter(),
                            &self.query_tools,
                            &time_shifts,
                            external,
                        );
                        if let Some(rewritten) =
                            self.try_rewrite_simple_query(q, pre_aggregation, date_range)?
                        {
                            matched = Some(rewritten);
                            break;
                        }
                    }
                    if let Some(rewritten) = matched {
                        Ok(CteRewriteOutcome::Rewritten(MultiStageMemberBody::Query(
                            rewritten,
                        )))
                    } else {
                        Ok(CteRewriteOutcome::NotMatched)
                    }
                }
                PreAggregationRewriteRole::WholeSubtree => {
                    if let Some(rewritten) = self.try_rewrite_schema_and_filter(
                        q.schema(),
                        root_filter,
                        compiled_pre_aggregations,
                    )? {
                        Ok(CteRewriteOutcome::Rewritten(MultiStageMemberBody::Query(
                            rewritten,
                        )))
                    } else {
                        Ok(CteRewriteOutcome::NotMatched)
                    }
                }
            },
            MultiStageMemberBody::TimeSeries(ts) => {
                if let Some(get_range) = ts.get_date_range_multistage_ref() {
                    queue.push_back(get_range.clone());
                }
                Ok(CteRewriteOutcome::Keep)
            }
            MultiStageMemberBody::RollingWindow(rw) => {
                queue.push_back(rw.time_series_input.name().clone());
                queue.push_back(rw.measure_input.name().clone());
                Ok(CteRewriteOutcome::Keep)
            }
        }
    }

    /// Returns the CTE names a Query consumes through its source. Only
    /// `FullKeyAggregate` sources hold refs; everything else points at
    /// base tables / joins.
    fn query_source_refs(query: &Rc<Query>) -> Vec<String> {
        let QuerySource::FullKeyAggregate(fk) = query.source() else {
            return Vec::new();
        };
        let mut refs: Vec<String> = fk.data_inputs().iter().map(|r| r.name().clone()).collect();
        if let Some(keys_ref) = fk.keys_subquery_ref() {
            refs.push(keys_ref.name().clone());
        }
        refs
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

enum CteRewriteOutcome {
    Rewritten(MultiStageMemberBody),
    Keep,
    NotMatched,
}
