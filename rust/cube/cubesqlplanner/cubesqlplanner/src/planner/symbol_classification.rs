use super::query_properties::QueryProperties;
use super::MemberSymbol;
use crate::planner::collectors::{
    collect_boundaries, collect_measure_leaves, crosses_multi_stage, Boundary, MeasureLeaf,
};
use crate::planner::symbols::transforms;
use crate::planner::{CubeId, JoinTree, MemberId};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// A measure paired with the cube it should be aggregated under. The bound
/// cube can differ from the measure's own cube for member expressions
/// referencing a dimension.
#[derive(Debug, Clone)]
pub struct MultipliedMeasure {
    measure: Rc<MemberSymbol>,
    cube_id: CubeId,
}

impl MultipliedMeasure {
    pub fn new(measure: Rc<MemberSymbol>, cube_id: CubeId) -> Rc<Self> {
        Rc::new(Self { measure, cube_id })
    }

    pub fn measure(&self) -> &Rc<MemberSymbol> {
        &self.measure
    }

    pub fn cube_id(&self) -> &CubeId {
        &self.cube_id
    }
}

/// Measures classified by how the planner must compute them: directly
/// aggregated alongside the rest of the query, wrapped in a multiplied
/// subquery, or planned as a multi-stage CTE.
#[derive(Default, Clone, Debug)]
pub struct FullKeyAggregateMeasures {
    pub multiplied_measures: Vec<Rc<MultipliedMeasure>>,
    pub regular_measures: Vec<Rc<MemberSymbol>>,
    pub multi_stage_measures: Vec<Rc<MemberSymbol>>,
    // A leaf measure's distinct render form (a `count` rewritten to `MultipliedCount`).
    render_forms: HashMap<MemberId, Rc<MemberSymbol>>,
}

impl FullKeyAggregateMeasures {
    /// Rewrite a selected measure into its render form: every multiplied
    /// `count` in its dependency tree is substituted with its distinct form.
    pub fn render(&self, measure: &Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError> {
        transforms::substitute_by_name(measure, &self.render_forms)
    }
}

/// How a query computes each of its symbols: the boundaries every symbol crosses,
/// and its measures split into multi-stage, per-row and leaves of their own.
/// Built over symbols already specialized; only resolving leaves needs a join.
pub struct SymbolClassification {
    // Keyed by `all_used_symbols()`; `has_multi_stage` is asked only about those.
    boundaries: HashMap<MemberId, Vec<Boundary>>,
    measures: FullKeyAggregateMeasures,
    splits_leaves_by_fact: bool,
}

/// The joins the leaves of a measure are computed over, one per leaf.
pub struct LeafJoins {
    pub joins: Vec<Rc<JoinTree>>,
    /// The leaves belong to different facts, each computed over its own.
    pub by_fact: bool,
}

/// How the query computes one of its measures.
#[derive(Clone, Debug)]
pub enum MeasureForm {
    MultiStage,
    /// An expression over dimensions without aggregation: row multiplication
    /// has nothing to distort.
    PerRow,
    /// Aggregated by the query itself, split into these leaves.
    Leaves(Vec<MeasureLeaf>),
}

impl SymbolClassification {
    /// `leaf_joins` is asked only for measures with leaves.
    pub fn try_new(
        query: &QueryProperties,
        leaf_joins: impl Fn(&Rc<MemberSymbol>, &[Rc<MemberSymbol>]) -> Result<LeafJoins, CubeError>,
    ) -> Result<Self, CubeError> {
        let mut boundaries = HashMap::new();
        for symbol in query.all_used_symbols()? {
            boundaries.insert(symbol.id().clone(), collect_boundaries(&symbol)?);
        }
        let ignore_cumulative = query.ignore_cumulative() || query.is_pre_aggregation_query();
        let mut forms = Vec::new();
        for measure in query.all_used_measures()? {
            let measure_boundaries = boundaries.get(measure.id()).ok_or_else(|| {
                CubeError::internal(format!(
                    "Measure {} was not classified",
                    measure.full_name()
                ))
            })?;
            let form = if crosses_multi_stage(measure_boundaries, ignore_cumulative) {
                MeasureForm::MultiStage
            } else if query.ungrouped() && measure.is_dimension_only_expression()? {
                MeasureForm::PerRow
            } else {
                MeasureForm::Leaves(collect_measure_leaves(&measure)?)
            };
            forms.push((measure, form));
        }
        let (measures, splits_leaves_by_fact) = Self::classify_measures(&forms, leaf_joins)?;
        Ok(Self {
            boundaries,
            measures,
            splits_leaves_by_fact,
        })
    }

    /// Whether any part of the symbol is computed as a multi-stage stage.
    pub fn has_multi_stage(
        &self,
        symbol: &Rc<MemberSymbol>,
        ignore_cumulative: bool,
    ) -> Result<bool, CubeError> {
        let boundaries = self.boundaries.get(symbol.id()).ok_or_else(|| {
            CubeError::internal(format!("Symbol {} was not classified", symbol.full_name()))
        })?;
        Ok(crosses_multi_stage(boundaries, ignore_cumulative))
    }

    pub fn measures(&self) -> &FullKeyAggregateMeasures {
        &self.measures
    }

    /// Whether the leaves of a measure are computed apart, one fact each.
    pub fn splits_leaves_by_fact(&self) -> bool {
        self.splits_leaves_by_fact
    }

    /// Moves `measures` computed as multi-stage stages out of the leaves.
    pub fn compute_as_stages(&mut self, measures: &[Rc<MemberSymbol>]) {
        let ids = measures
            .iter()
            .map(|m| m.clone().resolve_reference_chain().id().clone())
            .collect::<HashSet<_>>();
        let is_stage =
            |m: &Rc<MemberSymbol>| ids.contains(m.clone().resolve_reference_chain().id());
        let result = &mut self.measures;
        result.regular_measures.retain(|m| !is_stage(m));
        result.multiplied_measures.retain(|m| !is_stage(&m.measure));
        for measure in measures {
            if !result
                .multi_stage_measures
                .iter()
                .any(|m| m.id() == measure.id())
            {
                result.multi_stage_measures.push(measure.clone());
            }
        }
    }

    fn classify_measures(
        forms: &[(Rc<MemberSymbol>, MeasureForm)],
        leaf_joins: impl Fn(&Rc<MemberSymbol>, &[Rc<MemberSymbol>]) -> Result<LeafJoins, CubeError>,
    ) -> Result<(FullKeyAggregateMeasures, bool), CubeError> {
        let mut result = FullKeyAggregateMeasures::default();
        let mut splits_leaves_by_fact = false;
        for (m, form) in forms {
            let leaves = match form {
                MeasureForm::MultiStage => {
                    result.multi_stage_measures.push(m.clone());
                    continue;
                }
                MeasureForm::PerRow => {
                    result.regular_measures.push(m.clone());
                    continue;
                }
                MeasureForm::Leaves(leaves) => leaves,
            };
            let leaf_measures = leaves.iter().map(|l| l.measure.clone()).collect_vec();
            let LeafJoins { joins, by_fact } = leaf_joins(m, &leaf_measures)?;
            splits_leaves_by_fact |= by_fact;
            for (leaf, join) in leaves.iter().zip(joins.iter()) {
                let item = leaf.resolve(join)?;
                if !item.multiplied {
                    result.regular_measures.push(item.measure.clone());
                    continue;
                }
                // An expression over dimensions has no other form: it
                // moves to a multiplied subquery as it is.
                let Ok(measure) = item.measure.as_measure() else {
                    result
                        .multiplied_measures
                        .push(MultipliedMeasure::new(item.measure.clone(), item.cube_id));
                    continue;
                };
                // The leaf's render form (a distinct `MultipliedCount`
                // for a count) — the same symbol whether it stays in the
                // main query or moves to a multiplied subquery.
                let rendered = match transforms::regular_in_multiplied(&measure) {
                    Some(regular) => {
                        result.regular_measures.push(regular.clone());
                        regular
                    }
                    None => {
                        let rendered = transforms::into_multiplied(&measure);
                        result
                            .multiplied_measures
                            .push(MultipliedMeasure::new(rendered.clone(), item.cube_id));
                        rendered
                    }
                };
                result
                    .render_forms
                    .insert(item.measure.id().clone(), rendered);
            }
        }
        result.multi_stage_measures = result
            .multi_stage_measures
            .into_iter()
            .unique_by(|itm| itm.id().clone())
            .collect();
        result.regular_measures = result
            .regular_measures
            .into_iter()
            .unique_by(|itm| itm.id().clone())
            .collect();
        result.multiplied_measures = result
            .multiplied_measures
            .into_iter()
            .unique_by(|itm| itm.measure.id().clone())
            .collect();
        // FIXME: a leaf of a measure whose leaves read other cubes is judged in
        // the measure's whole join, while its subquery joins only the leaf's
        // cubes. A measure also computed as a regular one is read from there.
        let regular = result
            .regular_measures
            .iter()
            .map(|m| m.clone().resolve_reference_chain().id().clone())
            .collect::<HashSet<_>>();
        result
            .multiplied_measures
            .retain(|m| !regular.contains(m.measure.clone().resolve_reference_chain().id()));
        Ok((result, splits_leaves_by_fact))
    }
}
