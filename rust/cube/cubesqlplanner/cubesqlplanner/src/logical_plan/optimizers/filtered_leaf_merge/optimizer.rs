use super::super::rolling_base_scan::{same_filter_items, same_members};
use crate::logical_plan::*;
use crate::planner::filter::{FilterGroup, FilterGroupOperator, FilterItem};
use crate::planner::planners::multi_stage::EvaluationContext;
use crate::planner::query_properties::member_chain_eq;
use crate::planner::query_tools::QueryTools;
use crate::planner::{InputId, MemberSymbol};
use cubenativeutils::CubeError;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// Folds `sql: "{base}"` multi-stage measures whose leaves read one rollup
/// under different time filters into one scan of conditional aggregates; a key
/// with no rows in a measure's window still reads NULL.
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
    calculation_id: InputId,
    leaf_position: usize,
    leaf: Rc<Query>,
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

    pub fn optimize(
        &self,
        root: Rc<RootQuery>,
    ) -> Result<(Rc<RootQuery>, FoldedUsages), CubeError> {
        let mut folded_usages = FoldedUsages::default();
        let InputBase::Merge(root_aggregate) = root.query().source().base() else {
            return Ok((root, folded_usages));
        };

        let reference_counts = Self::reference_counts(&root);
        let candidates = root_aggregate
            .subqueries()
            .iter()
            .filter_map(|r| self.candidate(&root, r.id(), &reference_counts))
            .collect::<Vec<_>>();

        let groups = self.group(candidates);
        if groups.is_empty() {
            return Ok((root, folded_usages));
        }

        let mut removed: HashSet<InputId> = HashSet::new();
        let mut replaced_leaf: HashMap<usize, Rc<Subquery>> = HashMap::new();
        let mut forwards: Vec<(InputId, Rc<SubqueryRef>)> = Vec::new();
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
                removed.insert(candidate.calculation_id);
                let leaf_id = root.subqueries()[candidate.leaf_position].id;
                if candidate.leaf_position != kept_position {
                    removed.insert(leaf_id);
                }
                forwards.push((candidate.calculation_id, merged_ref.clone()));
            }
            replaced_leaf.insert(kept_position, merged);
        }

        let subqueries = root
            .subqueries()
            .iter()
            .enumerate()
            .filter(|(_, subquery)| !removed.contains(&subquery.id))
            .map(|(position, subquery)| {
                replaced_leaf
                    .get(&position)
                    .cloned()
                    .unwrap_or(subquery.clone())
            })
            .collect::<Vec<_>>();

        let mut optimized = Rc::new(
            RootQuery::builder()
                .subqueries(subqueries)
                .query(root.query().clone())
                .build(),
        );
        for (from, to) in forwards.iter() {
            optimized = optimized.forward(*from, to)?;
        }
        if let Some(inlined) = Self::inline_sole_scan(&optimized) {
            return Ok((inlined, folded_usages));
        }
        Ok((optimized, folded_usages))
    }

    // Cube Store applies TopK only when ORDER BY and LIMIT sit directly on
    // the rollup scan, not on a subquery over it.
    fn inline_sole_scan(root: &Rc<RootQuery>) -> Option<Rc<RootQuery>> {
        let InputBase::Merge(merge) = root.query().source().base() else {
            return None;
        };
        let [sole] = merge.subqueries().as_slice() else {
            return None;
        };
        let scan_subquery = root
            .subqueries()
            .iter()
            .find(|subquery| subquery.id == sole.id())?;
        let SubqueryBody::LeafMeasure(leaf) = &scan_subquery.body else {
            return None;
        };
        let scan = leaf;
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
        let remaining = root
            .subqueries()
            .iter()
            .filter(|subquery| subquery.id != scan_subquery.id)
            .cloned()
            .collect::<Vec<_>>();
        Some(Rc::new(
            RootQuery::builder()
                .subqueries(remaining)
                .query(Rc::new(query))
                .build(),
        ))
    }

    fn reference_counts(root: &Rc<RootQuery>) -> HashMap<InputId, usize> {
        fn walk(node: &PlanNode, counts: &mut HashMap<InputId, usize>) {
            for id in node.referenced_inputs() {
                *counts.entry(id).or_insert(0) += 1;
            }
            for input in node.inputs() {
                walk(&input, counts);
            }
        }
        let mut counts = HashMap::new();
        walk(&root.as_plan_node(), &mut counts);
        counts
    }

    fn subquery_position(root: &Rc<RootQuery>, id: InputId) -> Option<usize> {
        root.subqueries()
            .iter()
            .position(|subquery| subquery.id == id)
    }

    // The stage `id` when it only forwards a base measure computed by
    // a leaf of its own over a rollup.
    fn candidate(
        &self,
        root: &Rc<RootQuery>,
        id: InputId,
        reference_counts: &HashMap<InputId, usize>,
    ) -> Option<Candidate> {
        if reference_counts.get(&id) != Some(&1) {
            return None;
        }
        let subquery = &root.subqueries()[Self::subquery_position(root, id)?];
        let SubqueryBody::MeasureCalculation(calculation) = &subquery.body else {
            return None;
        };
        if *calculation.calculation_type() != MultiStageCalculationType::Calculate
            || *calculation.window_function_to_use() != MultiStageCalculationWindowFunction::None
            || calculation.schema().measures.len() != 1
            || !calculation.source().keys().is_empty()
            || calculation.source().subqueries().len() != 1
        {
            return None;
        }
        let measure = calculation.schema().measures[0].clone();
        let leaf_ref = &calculation.source().subqueries()[0];
        if reference_counts.get(&leaf_ref.id()) != Some(&1) {
            return None;
        }
        let leaf_position = Self::subquery_position(root, leaf_ref.id())?;
        let SubqueryBody::LeafMeasure(leaf) = &root.subqueries()[leaf_position].body else {
            return None;
        };
        if !leaf.conditional_measures().is_empty()
            || leaf.schema().measures.len() != 1
            || !Self::is_plain_evaluation(leaf.evaluation_context())
            || leaf.filter().time_dimensions_filters.is_empty()
            || !leaf.filter().measures_filter.is_empty()
            || leaf.modifers().ungrouped
            || leaf.modifers().limit.is_some()
            || leaf.modifers().offset.is_some()
        {
            return None;
        }
        let InputBase::PreAggregation(pre_aggregation) = leaf.source().base() else {
            return None;
        };
        let base = leaf.schema().measures[0].clone();
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
        if !same_members(&calculation.schema().dimensions, &leaf.schema().dimensions)
            || !same_members(
                &calculation.schema().time_dimensions,
                &leaf.schema().time_dimensions,
            )
        {
            return None;
        }
        Some(Candidate {
            calculation_id: id,
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

    fn is_plain_evaluation(context: &Option<EvaluationContext>) -> bool {
        let Some(context) = context else {
            return true;
        };
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
        let (qa, qb) = (&a.leaf, &b.leaf);
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

    // The folded leaf, the position it takes in the subquery list, the reference
    // the root query reads it through, and the rollup usage it reads.
    fn merge(
        &self,
        root: &Rc<RootQuery>,
        group: &Group,
    ) -> (usize, Rc<Subquery>, Rc<SubqueryRef>, Rc<PreAggregation>) {
        let candidates = &group.candidates;
        let covering = &candidates[group.covering];
        let first_query = &candidates[0].leaf;
        let kept_position = candidates.iter().map(|c| c.leaf_position).min().unwrap();
        let kept = &root.subqueries()[kept_position];
        let (id, name) = (kept.id, kept.name.clone());

        let conditions = candidates
            .iter()
            .map(|c| Self::all_of(c.leaf.filter().time_dimensions_filters.clone()))
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
                distinct: false,
                order_by: vec![],
            }))
            .source(QuerySource::new(pre_aggregation.clone()))
            .conditional_measures(conditional_measures)
            .build();

        let merged = Rc::new(Subquery::new(
            id,
            name.clone(),
            SubqueryBody::LeafMeasure(
                Rc::new(query).with_evaluation_context(Some(EvaluationContext::default())),
            ),
        ));
        let merged_ref = Rc::new(
            SubqueryRef::builder()
                .id(id)
                .name(name)
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
