use super::super::rolling_base_scan::{same_filter_items, same_members};
use crate::logical_plan::*;
use crate::planner::filter::{FilterGroup, FilterGroupOperator, FilterItem};
use crate::planner::planners::multi_stage::EvaluationContext;
use crate::planner::query_properties::member_chain_eq;
use crate::planner::query_tools::QueryTools;
use crate::planner::MemberSymbol;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// Folds `sql: "{base}"` multi-stage measures whose leaves read one rollup
/// under different time filters into one scan of conditional aggregates; a key
/// with no rows in a measure's window still reads NULL. Runs after
/// pre-aggregation matching: it only folds leaves that read a rollup.
pub struct FilteredLeafMergeOptimizer<'a> {
    /// Date range of every usage that states one, by usage index.
    usage_ranges: HashMap<usize, (String, String)>,
    query_tools: &'a QueryTools,
}

/// How the folded plan reads the rollup usages matched before it: the scan
/// reads one usage per group in place of the others, with every measure of
/// the group.
#[derive(Default)]
pub struct FoldedUsages {
    retired: HashSet<usize>,
    widened: HashMap<usize, Rc<PreAggregation>>,
}

impl FoldedUsages {
    pub fn apply(&self, usages: Vec<PreAggregationUsage>) -> Vec<PreAggregationUsage> {
        usages
            .into_iter()
            .filter(|usage| !self.retired.contains(&usage.index))
            .map(|usage| match self.widened.get(&usage.index) {
                Some(pre_aggregation) => PreAggregationUsage {
                    pre_aggregation: pre_aggregation.clone(),
                    ..usage
                },
                None => usage,
            })
            .collect()
    }
}

struct Group {
    candidates: Vec<Candidate>,
    /// Position of the candidate whose usage covers every other's range.
    covering: usize,
}

/// A passthrough measure stage over its own leaf.
struct Candidate {
    calculation_name: String,
    leaf_position: usize,
    leaf: Rc<MultiStageLeafMeasure>,
    measure: Rc<MemberSymbol>,
    base: Rc<MemberSymbol>,
    pre_aggregation: Rc<PreAggregation>,
}

impl<'a> FilteredLeafMergeOptimizer<'a> {
    pub fn new(usages: &'a [PreAggregationUsage], query_tools: &'a QueryTools) -> Self {
        let usage_ranges = usages
            .iter()
            .filter(|usage| !usage.unbounded)
            .filter_map(|usage| Some((usage.index, usage.date_range.clone()?)))
            .collect();
        Self {
            usage_ranges,
            query_tools,
        }
    }

    pub fn optimize(&self, root: Rc<RootQuery>) -> (Rc<RootQuery>, FoldedUsages) {
        let mut folded_usages = FoldedUsages::default();
        let QuerySource::FullKeyAggregate(root_aggregate) = root.query().source() else {
            return (root, folded_usages);
        };
        if root_aggregate.keys_input().is_some() {
            return (root, folded_usages);
        }

        let reference_counts = Self::reference_counts(&root);
        let candidates = root_aggregate
            .multi_stage_subquery_refs()
            .iter()
            .filter_map(|r| self.candidate(&root, r.name(), &reference_counts))
            .collect::<Vec<_>>();

        let groups = self.group(candidates);
        if groups.is_empty() {
            return (root, folded_usages);
        }

        let mut removed: HashSet<String> = HashSet::new();
        let mut replaced_leaf: HashMap<usize, Rc<LogicalMultiStageMember>> = HashMap::new();
        let mut ref_renames: HashMap<String, Rc<MultiStageSubqueryRef>> = HashMap::new();
        for group in groups.iter() {
            let (kept_position, merged, merged_ref, pre_aggregation) = self.merge(&root, group);
            if let Some(index) = pre_aggregation.usage_index() {
                folded_usages.widened.insert(index, pre_aggregation);
            }
            for (position, candidate) in group.candidates.iter().enumerate() {
                if position != group.covering {
                    folded_usages
                        .retired
                        .extend(candidate.pre_aggregation.usage_index());
                }
                removed.insert(candidate.calculation_name.clone());
                let leaf_name = root.ctes()[candidate.leaf_position].name.clone();
                if candidate.leaf_position != kept_position {
                    removed.insert(leaf_name);
                }
                ref_renames.insert(candidate.calculation_name.clone(), merged_ref.clone());
            }
            replaced_leaf.insert(kept_position, merged);
        }

        let ctes = root
            .ctes()
            .iter()
            .enumerate()
            .filter(|(_, cte)| !removed.contains(&cte.name))
            .map(|(position, cte)| replaced_leaf.get(&position).cloned().unwrap_or(cte.clone()))
            .collect::<Vec<_>>();

        let mut refs: Vec<Rc<MultiStageSubqueryRef>> = Vec::new();
        for r in root_aggregate.multi_stage_subquery_refs().iter() {
            let next = ref_renames.get(r.name()).cloned().unwrap_or(r.clone());
            if !refs.iter().any(|existing| existing.name() == next.name()) {
                refs.push(next);
            }
        }
        if let Some(inlined) = Self::inline_sole_scan(&root, &ctes, &refs) {
            return (inlined, folded_usages);
        }

        let aggregate = FullKeyAggregate::builder()
            .schema(root_aggregate.schema().clone())
            .use_full_join_and_coalesce(root_aggregate.use_full_join_and_coalesce())
            .multi_stage_subquery_refs(refs)
            .build();
        let query = root.query();
        let query = Query::builder()
            .schema(query.schema().clone())
            .filter(query.filter().clone())
            .modifers(query.modifers().clone())
            .source(QuerySource::FullKeyAggregate(Rc::new(aggregate)))
            .build();

        let optimized = Rc::new(
            RootQuery::builder()
                .ctes(ctes)
                .query(Rc::new(query))
                .build(),
        );
        (optimized, folded_usages)
    }

    // Cube Store applies TopK only when ORDER BY and LIMIT sit directly on
    // the rollup scan, not on a CTE or subquery over it.
    fn inline_sole_scan(
        root: &Rc<RootQuery>,
        ctes: &[Rc<LogicalMultiStageMember>],
        refs: &[Rc<MultiStageSubqueryRef>],
    ) -> Option<Rc<RootQuery>> {
        let [sole] = refs else {
            return None;
        };
        let scan_cte = ctes.iter().find(|cte| cte.name == *sole.name())?;
        let MultiStageMemberLogicalType::LeafMeasure(leaf) = &scan_cte.member_type else {
            return None;
        };
        let scan = &leaf.query;
        let root_query = root.query();
        if scan.conditional_measures().is_empty()
            || !root_query.filter().measures_filter.is_empty()
            || root_query.modifers().ungrouped
            || !same_members(&root_query.schema().dimensions, &scan.schema().dimensions)
            || !same_members(
                &root_query.schema().time_dimensions,
                &scan.schema().time_dimensions,
            )
            || !same_members(&root_query.schema().measures, &scan.schema().measures)
        {
            return None;
        }
        let query = Query::builder()
            .schema(root_query.schema().clone())
            .filter(scan.filter().clone())
            .modifers(root_query.modifers().clone())
            .source(scan.source().clone())
            .conditional_measures(scan.conditional_measures().clone())
            .build();
        let remaining = ctes
            .iter()
            .filter(|cte| cte.name != scan_cte.name)
            .cloned()
            .collect::<Vec<_>>();
        Some(Rc::new(
            RootQuery::builder()
                .ctes(remaining)
                .query(Rc::new(query))
                .build(),
        ))
    }

    fn reference_counts(root: &Rc<RootQuery>) -> HashMap<String, usize> {
        fn walk(node: &PlanNode, counts: &mut HashMap<String, usize>) {
            for name in node.referenced_cte_names() {
                *counts.entry(name).or_insert(0) += 1;
            }
            for input in node.inputs() {
                walk(&input, counts);
            }
        }
        let mut counts = HashMap::new();
        walk(&root.as_plan_node(), &mut counts);
        counts
    }

    fn cte_position(root: &Rc<RootQuery>, name: &str) -> Option<usize> {
        root.ctes().iter().position(|cte| cte.name == name)
    }

    // The stage named `name` when it only forwards a base measure computed by
    // a leaf of its own over a rollup.
    fn candidate(
        &self,
        root: &Rc<RootQuery>,
        name: &str,
        reference_counts: &HashMap<String, usize>,
    ) -> Option<Candidate> {
        if reference_counts.get(name) != Some(&1) {
            return None;
        }
        let cte = &root.ctes()[Self::cte_position(root, name)?];
        let MultiStageMemberLogicalType::MeasureCalculation(calculation) = &cte.member_type else {
            return None;
        };
        if *calculation.calculation_type() != MultiStageCalculationType::Calculate
            || *calculation.window_function_to_use() != MultiStageCalculationWindowFunction::None
            || calculation.schema().measures.len() != 1
            || calculation.source().keys_input().is_some()
            || calculation.source().multi_stage_subquery_refs().len() != 1
        {
            return None;
        }
        let measure = calculation.schema().measures[0].clone();
        let leaf_ref = &calculation.source().multi_stage_subquery_refs()[0];
        if reference_counts.get(leaf_ref.name()) != Some(&1) {
            return None;
        }
        let leaf_position = Self::cte_position(root, leaf_ref.name())?;
        let MultiStageMemberLogicalType::LeafMeasure(leaf) =
            &root.ctes()[leaf_position].member_type
        else {
            return None;
        };
        if !leaf.query.conditional_measures().is_empty()
            || leaf.measures.len() != 1
            || leaf.query.schema().measures.len() != 1
            || !member_chain_eq(&leaf.measures[0], &leaf.query.schema().measures[0])
            || !Self::is_plain_evaluation(&leaf.evaluation_context)
            || leaf.query.filter().time_dimensions_filters.is_empty()
            || !leaf.query.filter().measures_filter.is_empty()
            || leaf.query.modifers().ungrouped
            || leaf.query.modifers().limit.is_some()
            || leaf.query.modifers().offset.is_some()
        {
            return None;
        }
        let QuerySource::PreAggregation(pre_aggregation) = leaf.query.source() else {
            return None;
        };
        let base = leaf.measures[0].clone();
        if !Self::is_foldable_base(&base) || !Self::forwards(&measure, &base) {
            return None;
        }
        // A mask replaces the whole aggregate, row condition included. The
        // query may select the measure through a view, whose member carries
        // a mask of its own.
        let selected_as = root
            .query()
            .schema()
            .measures
            .iter()
            .filter(|m| member_chain_eq(m, &measure));
        if std::iter::once(&measure)
            .chain(std::iter::once(&base))
            .chain(selected_as)
            .any(|symbol| self.masked_in_chain(symbol))
        {
            return None;
        }
        if !same_members(
            &calculation.schema().dimensions,
            &leaf.query.schema().dimensions,
        ) || !same_members(
            &calculation.schema().time_dimensions,
            &leaf.query.schema().time_dimensions,
        ) {
            return None;
        }
        Some(Candidate {
            calculation_name: name.to_string(),
            leaf_position,
            leaf: leaf.clone(),
            measure,
            base,
            pre_aggregation: pre_aggregation.clone(),
        })
    }

    fn masked_in_chain(&self, symbol: &Rc<MemberSymbol>) -> bool {
        let mut current = Some(symbol.clone());
        while let Some(member) = current {
            if self.query_tools.is_member_masked(&member.full_name()) {
                return true;
            }
            current = member.reference_member();
        }
        false
    }

    fn is_plain_evaluation(context: &EvaluationContext) -> bool {
        let EvaluationContext {
            measure_for_ungrouped,
            time_shifts,
        } = context;
        !*measure_for_ungrouped && time_shifts.is_empty()
    }

    // A sum, count, min or max over a stored column keeps its value over the
    // rows a condition lets through, and reads NULL over none.
    fn is_foldable_base(base: &Rc<MemberSymbol>) -> bool {
        let Ok(measure) = base.as_measure() else {
            return false;
        };
        !measure.is_multi_stage()
            && matches!(measure.measure_type(), "sum" | "count" | "min" | "max")
    }

    // Whether `measure` is `sql: "{base}"` and nothing else.
    fn forwards(measure: &Rc<MemberSymbol>, base: &Rc<MemberSymbol>) -> bool {
        let Ok(symbol) = measure.as_measure() else {
            return false;
        };
        if symbol.measure_type() != "number"
            || symbol.is_rolling_window()
            || symbol.time_shift().is_some()
            || symbol.case().is_some()
            || !symbol.measure_filters().is_empty()
            || symbol.render_modifier().is_some()
        {
            return false;
        }
        symbol
            .kind()
            .member_sql()
            .and_then(|sql| sql.resolve_direct_reference())
            .is_some_and(|target| member_chain_eq(&target, base))
    }

    // Two leaves read the same rollup at the same grain and differ at most in
    // their time filter and the measure they aggregate.
    fn foldable_together(a: &Candidate, b: &Candidate) -> bool {
        let (qa, qb) = (&a.leaf.query, &b.leaf.query);
        a.pre_aggregation.name() == b.pre_aggregation.name()
            && a.pre_aggregation.cube_name() == b.pre_aggregation.cube_name()
            && same_members(&qa.schema().dimensions, &qb.schema().dimensions)
            && same_members(&qa.schema().time_dimensions, &qb.schema().time_dimensions)
            && same_filter_items(
                &qa.filter().dimensions_filters,
                &qb.filter().dimensions_filters,
            )
            && same_filter_items(&qa.filter().segments, &qb.filter().segments)
    }

    fn usage_range(&self, candidate: &Candidate) -> Option<&(String, String)> {
        self.usage_ranges
            .get(&candidate.pre_aggregation.usage_index()?)
    }

    // Groups of two or more candidates that fold into one scan, each with the
    // member whose partitions cover every other's.
    fn group(&self, candidates: Vec<Candidate>) -> Vec<Group> {
        let mut groups: Vec<Vec<Candidate>> = Vec::new();
        for candidate in candidates {
            if self.usage_range(&candidate).is_none() {
                continue;
            }
            match groups
                .iter_mut()
                .find(|group| Self::foldable_together(&group[0], &candidate))
            {
                Some(group) => group.push(candidate),
                None => groups.push(vec![candidate]),
            }
        }
        groups
            .into_iter()
            .filter(|candidates| candidates.len() > 1)
            .filter_map(|candidates| {
                let covering = self.covering_member(&candidates)?;
                Some(Group {
                    candidates,
                    covering,
                })
            })
            .collect()
    }

    fn covering_member(&self, group: &[Candidate]) -> Option<usize> {
        let ranges = group
            .iter()
            .map(|c| self.usage_range(c))
            .collect::<Option<Vec<_>>>()?;
        ranges.iter().position(|(from, to)| {
            ranges
                .iter()
                .all(|(other_from, other_to)| from <= other_from && to >= other_to)
        })
    }

    // The folded leaf, the position it takes in the CTE list, the reference
    // the root query reads it through, and the rollup usage it reads.
    fn merge(
        &self,
        root: &Rc<RootQuery>,
        group: &Group,
    ) -> (
        usize,
        Rc<LogicalMultiStageMember>,
        Rc<MultiStageSubqueryRef>,
        Rc<PreAggregation>,
    ) {
        let candidates = &group.candidates;
        let covering = &candidates[group.covering];
        let first_query = &candidates[0].leaf.query;
        let kept_position = candidates.iter().map(|c| c.leaf_position).min().unwrap();
        let name = root.ctes()[kept_position].name.clone();

        let conditions = candidates
            .iter()
            .map(|c| Self::all_of(c.leaf.query.filter().time_dimensions_filters.clone()))
            .collect::<Vec<_>>();
        let conditional_measures = candidates
            .iter()
            .zip(conditions.iter())
            .map(|(c, condition)| ConditionalMeasure {
                measure: c.measure.clone(),
                base: c.base.clone(),
                condition: condition.clone(),
            })
            .collect::<Vec<_>>();

        let filter = LogicalFilter {
            dimensions_filters: first_query.filter().dimensions_filters.clone(),
            time_dimensions_filters: vec![FilterItem::Group(Rc::new(FilterGroup::new(
                FilterGroupOperator::Or,
                conditions,
            )))],
            measures_filter: vec![],
            segments: first_query.filter().segments.clone(),
        };
        let measures = candidates
            .iter()
            .map(|c| c.measure.clone())
            .collect::<Vec<_>>();
        let schema = LogicalSchema {
            time_dimensions: first_query.schema().time_dimensions.clone(),
            dimensions: first_query.schema().dimensions.clone(),
            measures: measures.clone(),
        }
        .into_rc();
        let pre_aggregation = Self::reading_every_base(&covering.pre_aggregation, candidates);
        let query = Query::builder()
            .schema(schema.clone())
            .filter(Rc::new(filter))
            .modifers(Rc::new(LogicalQueryModifiers {
                offset: None,
                limit: None,
                ungrouped: false,
                order_by: vec![],
            }))
            .source(QuerySource::PreAggregation(pre_aggregation.clone()))
            .conditional_measures(conditional_measures)
            .build();

        let merged = Rc::new(LogicalMultiStageMember {
            name: name.clone(),
            member_type: MultiStageMemberLogicalType::LeafMeasure(Rc::new(MultiStageLeafMeasure {
                measures: measures.clone(),
                evaluation_context: EvaluationContext::default(),
                query: Rc::new(query),
            })),
        });
        let merged_ref = Rc::new(
            MultiStageSubqueryRef::builder()
                .name(name)
                .symbols(measures)
                .schema(schema)
                .build(),
        );
        (kept_position, merged, merged_ref, pre_aggregation)
    }

    // Each leaf's usage lists only the measure it reads, and a measure missing
    // from the list would render from the cube's own SQL, so the covering
    // usage takes the stored measures of the whole group.
    fn reading_every_base(
        covering: &Rc<PreAggregation>,
        group: &[Candidate],
    ) -> Rc<PreAggregation> {
        let mut measures = covering.measures().clone();
        let mut schema_measures = covering.schema().measures.clone();
        for candidate in group.iter() {
            for measure in candidate.pre_aggregation.measures().iter() {
                if !measures.iter().any(|m| member_chain_eq(m, measure)) {
                    measures.push(measure.clone());
                }
            }
            for measure in candidate.pre_aggregation.schema().measures.iter() {
                if !schema_measures.iter().any(|m| member_chain_eq(m, measure)) {
                    schema_measures.push(measure.clone());
                }
            }
        }
        let schema = LogicalSchema {
            time_dimensions: covering.schema().time_dimensions.clone(),
            dimensions: covering.schema().dimensions.clone(),
            measures: schema_measures,
        };
        Rc::new(covering.with_measures(measures, schema.into_rc()))
    }

    fn all_of(items: Vec<FilterItem>) -> FilterItem {
        if items.len() == 1 {
            items.into_iter().next().unwrap()
        } else {
            FilterItem::Group(Rc::new(FilterGroup::new(FilterGroupOperator::And, items)))
        }
    }
}
