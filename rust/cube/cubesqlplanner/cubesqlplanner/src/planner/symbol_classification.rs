use super::query_properties::QueryProperties;
use super::MemberSymbol;
use crate::planner::collectors::{
    collect_boundaries, collect_measure_leaves, crosses_multi_stage, Boundary, MeasureLeaf,
};
use crate::planner::MemberId;
use cubenativeutils::CubeError;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// How a query computes each of its symbols: the boundaries every symbol crosses,
/// and its measures split into multi-stage ones and the ones it aggregates
/// itself. Built over symbols already specialized, without a join.
pub struct SymbolClassification {
    // Keyed by `all_used_symbols()`; `has_multi_stage` is asked only about those.
    boundaries: HashMap<MemberId, Vec<Boundary>>,
    multi_stage_measures: Vec<Rc<MemberSymbol>>,
    aggregated: Vec<(Rc<MemberSymbol>, MeasureForm)>,
}

/// How the query aggregates one of its measures itself.
#[derive(Clone, Debug)]
pub enum MeasureForm {
    /// An expression over dimensions without aggregation: row multiplication
    /// has nothing to distort.
    PerRow,
    /// Aggregated by the query itself, split into these leaves.
    Leaves(Vec<MeasureLeaf>),
}

impl SymbolClassification {
    pub fn try_new(query: &QueryProperties) -> Result<Self, CubeError> {
        let mut boundaries = HashMap::new();
        for symbol in query.all_used_symbols()? {
            boundaries.insert(symbol.id().clone(), collect_boundaries(&symbol)?);
        }
        let ignore_cumulative = query.ignore_cumulative() || query.is_pre_aggregation_query();
        let mut multi_stage_measures: Vec<Rc<MemberSymbol>> = Vec::new();
        let mut aggregated: Vec<(Rc<MemberSymbol>, MeasureForm)> = Vec::new();
        for measure in query.all_used_measures()? {
            let measure_boundaries = boundaries.get(measure.id()).ok_or_else(|| {
                CubeError::internal(format!(
                    "Measure {} was not classified",
                    measure.full_name()
                ))
            })?;
            if crosses_multi_stage(measure_boundaries, ignore_cumulative) {
                if !multi_stage_measures.iter().any(|m| m.id() == measure.id()) {
                    multi_stage_measures.push(measure);
                }
                continue;
            }
            if aggregated.iter().any(|(m, _)| m.id() == measure.id()) {
                continue;
            }
            let form = if query.ungrouped() && measure.is_dimension_only_expression()? {
                MeasureForm::PerRow
            } else {
                MeasureForm::Leaves(collect_measure_leaves(&measure)?)
            };
            aggregated.push((measure, form));
        }
        Ok(Self {
            boundaries,
            multi_stage_measures,
            aggregated,
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

    pub fn multi_stage_measures(&self) -> &[Rc<MemberSymbol>] {
        &self.multi_stage_measures
    }

    /// The measures the query aggregates itself, each with how it does.
    pub fn aggregated(&self) -> &[(Rc<MemberSymbol>, MeasureForm)] {
        &self.aggregated
    }

    /// Moves `measures` computed as multi-stage stages out of the aggregated
    /// ones; true when that removed any aggregated measure or leaf.
    pub fn compute_as_stages(&mut self, measures: &[Rc<MemberSymbol>]) -> bool {
        let ids = measures
            .iter()
            .map(|m| m.clone().resolve_reference_chain().id().clone())
            .collect::<HashSet<_>>();
        let is_stage =
            |m: &Rc<MemberSymbol>| ids.contains(m.clone().resolve_reference_chain().id());
        let before = self.aggregated.len();
        self.aggregated.retain(|(m, _)| !is_stage(m));
        let mut changed = self.aggregated.len() != before;
        for (_, form) in self.aggregated.iter_mut() {
            if let MeasureForm::Leaves(leaves) = form {
                let before = leaves.len();
                leaves.retain(|leaf| !is_stage(&leaf.measure));
                changed |= leaves.len() != before;
            }
        }
        for measure in measures {
            if !self
                .multi_stage_measures
                .iter()
                .any(|m| m.id() == measure.id())
            {
                self.multi_stage_measures.push(measure.clone());
            }
        }
        changed
    }
}
