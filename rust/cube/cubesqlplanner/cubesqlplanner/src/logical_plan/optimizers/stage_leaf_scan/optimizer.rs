use super::super::common::{
    collapse_subqueries, reads_a_measure_twice_anew, InputReaders, SubqueryCollapse,
};
use super::super::filtered_leaf_merge::FoldedUsages;
use super::super::rolling_base_scan::{same_from, selects_same_rows, InlineBodies};
use crate::logical_plan::*;
use crate::planner::query_properties::member_chain_eq;
use crate::planner::{InputId, MemberSymbol};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

/// Merges the leaves of multi-stage calculations that read the same rows, so
/// measures differing only in the column they aggregate share one scan. Runs
/// after pre-aggregation matching: a wider scan may fit no single rollup.
pub struct StageLeafScanOptimizer<'a> {
    usages: HashMap<usize, &'a PreAggregationUsage>,
}

// One scan and the measures that will ride on it; `position` is the kept
// subquery's place in the plan's subquery list.
struct MergeGroup {
    position: usize,
    leaf: Rc<Query>,
    measures: Vec<Rc<MemberSymbol>>,
    absorbed: Vec<Absorbed>,
}

struct Absorbed {
    id: InputId,
    pre_aggregation: Option<Rc<PreAggregation>>,
}

impl<'a> StageLeafScanOptimizer<'a> {
    pub fn new(usages: &'a [PreAggregationUsage]) -> Self {
        Self {
            usages: usages.iter().map(|usage| (usage.index, usage)).collect(),
        }
    }

    pub fn optimize(
        &self,
        root: Rc<RootQuery>,
    ) -> Result<(Rc<RootQuery>, FoldedUsages), CubeError> {
        let mut folded_usages = FoldedUsages::default();
        let mut collapses = Vec::new();
        for group in self.group_shareable_scans(&root) {
            if group.absorbed.is_empty() {
                continue;
            }
            let Some(widened) = Self::widen_leaf(&group, &mut folded_usages)? else {
                continue;
            };
            collapses.push(SubqueryCollapse {
                position: group.position,
                schema: widened.schema().clone(),
                body: SubqueryBody::Query(widened),
                absorbed: group.absorbed.iter().map(|scan| scan.id).collect(),
            });
        }
        if collapses.is_empty() {
            return Ok((root, folded_usages));
        }
        let result = collapse_subqueries(&root, collapses)?;
        if reads_a_measure_twice_anew(&root, &result) {
            return Ok((root, FoldedUsages::default()));
        }
        Ok((result, folded_usages))
    }

    fn group_shareable_scans(&self, root: &Rc<RootQuery>) -> Vec<MergeGroup> {
        let readers = InputReaders::collect(root);
        let inline_bodies: InlineBodies = root
            .subqueries()
            .iter()
            .filter(|subquery| subquery.inline)
            .filter_map(|subquery| match &subquery.body {
                SubqueryBody::Query(body) => Some((subquery.id, body.clone())),
                _ => None,
            })
            .collect();
        let mut groups: Vec<MergeGroup> = Vec::new();

        for (position, subquery) in root.subqueries().iter().enumerate() {
            let SubqueryBody::Query(leaf) = &subquery.body else {
                continue;
            };
            // A leaf selecting a bare dimension grid has no measure to share,
            // and a calc group enumerated for one measure multiplies the rows
            // every other one aggregates.
            if subquery.inline
                || !readers.read_only_by_stages(subquery.id)
                || !readers.read_only_by_member(subquery.id)
                || leaf.schema().measures.is_empty()
                || !leaf.from().calc_groups().is_empty()
            {
                continue;
            }
            match groups
                .iter_mut()
                .find(|group| self.reads_same_rows(&group.leaf, leaf, &inline_bodies))
            {
                Some(group) => {
                    for measure in leaf.schema().measures.iter() {
                        if !group
                            .measures
                            .iter()
                            .any(|carried| member_chain_eq(carried, measure))
                        {
                            group.measures.push(measure.clone());
                        }
                    }
                    group.absorbed.push(Absorbed {
                        id: subquery.id,
                        pre_aggregation: leaf.from().as_pre_aggregation().cloned(),
                    });
                }
                None => groups.push(MergeGroup {
                    position,
                    leaf: leaf.clone(),
                    measures: leaf.schema().measures.clone(),
                    absorbed: Vec::new(),
                }),
            }
        }

        groups
    }

    fn reads_same_rows(&self, a: &Query, b: &Query, inline_bodies: &InlineBodies) -> bool {
        if !selects_same_rows(a, b) || !same_unordered_modifiers(a.modifers(), b.modifers()) {
            return false;
        }
        match (a.from().as_pre_aggregation(), b.from().as_pre_aggregation()) {
            // The widened leaf reads the rollup alone.
            (Some(a_rollup), Some(b_rollup)) => {
                a.from().joins().is_empty()
                    && b.from().joins().is_empty()
                    && self.same_rollup_read(a_rollup, b_rollup)
            }
            (None, None) => same_from(a.from(), b.from(), inline_bodies),
            _ => false,
        }
    }

    // Two usages of one rollup read the same partitions when they state the
    // same range; a usage this pass was not told about is never the same.
    fn same_rollup_read(&self, a: &PreAggregation, b: &PreAggregation) -> bool {
        let usage = |pre_aggregation: &PreAggregation| {
            pre_aggregation
                .usage_index()
                .and_then(|index| self.usages.get(&index))
        };
        let (Some(a_usage), Some(b_usage)) = (usage(a), usage(b)) else {
            return false;
        };
        a.name() == b.name()
            && a.cube_name() == b.cube_name()
            && a.external() == b.external()
            && a.granularity() == b.granularity()
            && a_usage.date_range == b_usage.date_range
            && a_usage.unbounded == b_usage.unbounded
    }

    // The kept leaf projecting every measure of its group, or `None` when the
    // widened leaf would no longer read the rows the kept one does.
    fn widen_leaf(
        group: &MergeGroup,
        folded_usages: &mut FoldedUsages,
    ) -> Result<Option<Rc<Query>>, CubeError> {
        let leaf = &group.leaf;
        let schema = LogicalSchema {
            time_dimensions: leaf.schema().time_dimensions.clone(),
            dimensions: leaf.schema().dimensions.clone(),
            measures: group.measures.clone(),
        }
        .into_rc();

        let kept_rollup = leaf.from().as_pre_aggregation();
        let widened_rollup = kept_rollup.map(|kept| Self::reading_every_measure(kept, group));
        let source = match &widened_rollup {
            Some(rollup) => leaf.reading_rows_of(LogicalFrom::pre_aggregation(rollup.clone())?)?,
            None => leaf.clone(),
        };
        let widened = source.with_schema_and_modifers(schema, leaf.modifers().clone())?;
        if !widened.from().calc_groups().is_empty() {
            return Ok(None);
        }

        if let (Some(rollup), Some(kept_index)) = (
            widened_rollup,
            kept_rollup.and_then(|kept| kept.usage_index()),
        ) {
            for index in group
                .absorbed
                .iter()
                .filter_map(|scan| scan.pre_aggregation.as_ref()?.usage_index())
            {
                folded_usages.fold(index, kept_index, rollup.clone());
            }
        }
        Ok(Some(widened))
    }

    // A usage lists only the stored measures its own leaf reads, and a measure
    // missing from the list would render from the cube's own SQL.
    fn reading_every_measure(kept: &Rc<PreAggregation>, group: &MergeGroup) -> Rc<PreAggregation> {
        let mut measures = kept.measures().clone();
        let mut schema_measures = kept.schema().measures.clone();
        let add_missing = |to: &mut Vec<Rc<MemberSymbol>>, from: &[Rc<MemberSymbol>]| {
            for measure in from.iter() {
                if !to.iter().any(|m| member_chain_eq(m, measure)) {
                    to.push(measure.clone());
                }
            }
        };
        for rollup in group
            .absorbed
            .iter()
            .filter_map(|scan| scan.pre_aggregation.as_ref())
        {
            add_missing(&mut measures, rollup.measures());
            add_missing(&mut schema_measures, &rollup.schema().measures);
        }
        let schema = LogicalSchema {
            time_dimensions: kept.schema().time_dimensions.clone(),
            dimensions: kept.schema().dimensions.clone(),
            measures: schema_measures,
        };
        Rc::new(kept.with_measures(measures, schema.into_rc()))
    }
}

// A leaf's order names the measure it aggregates, and without a limit it
// decides nothing its readers see.
fn same_unordered_modifiers(a: &LogicalQueryModifiers, b: &LogicalQueryModifiers) -> bool {
    let LogicalQueryModifiers {
        offset,
        limit,
        ungrouped,
        distinct,
        order_by: _,
    } = a;
    offset.is_none()
        && limit.is_none()
        && b.offset.is_none()
        && b.limit.is_none()
        && *ungrouped == b.ungrouped
        && *distinct == b.distinct
}
