use super::query_properties::QueryProperties;
use super::symbols::transforms;
use super::{MemberId, MemberSymbol};
use crate::planner::collectors::has_multi_stage_members;
use cubenativeutils::CubeError;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// The view dimensions a measure means differently, mapped to what they mean
/// for it: the overrides of a view measure whose dimension the query uses.
/// Ordered by dimension, so equal sets compare equal.
#[derive(Clone, Default)]
pub struct AppliedOverrides {
    items: Vec<(MemberId, MemberId)>,
    targets: Vec<Rc<MemberSymbol>>,
}

impl PartialEq for AppliedOverrides {
    fn eq(&self, other: &Self) -> bool {
        self.items == other.items
    }
}

impl AppliedOverrides {
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// The overrides of `measure` whose view dimension is in `used`. A
    /// dimension is matched by its own id, so a member that only references
    /// the same model member is a different dimension.
    pub fn for_measure(measure: &Rc<MemberSymbol>, used: &HashSet<MemberId>) -> Self {
        let Ok(reference) = measure.as_ref_symbol() else {
            return Self::default();
        };
        let (items, targets) = reference
            .dimension_overrides()
            .iter()
            .filter(|o| used.contains(&o.dimension))
            .map(|o| {
                (
                    (o.dimension.clone(), o.target.id().clone()),
                    o.target.clone(),
                )
            })
            .unzip();
        Self { items, targets }
    }

    /// `query` with each overridden view dimension standing for its override,
    /// under its own name, wherever the query groups, filters or orders by it.
    pub fn apply(&self, query: &QueryProperties) -> Result<Rc<QueryProperties>, CubeError> {
        let mut replacements = HashMap::new();
        for ((dimension, _), target) in self.items.iter().zip(&self.targets) {
            let replacement = query_dimension(query, dimension)
                .map(|symbol| symbol.as_ref_symbol())
                .transpose()?
                .map(|reference| MemberSymbol::new_ref(reference.with_target(target.clone())));
            if let Some(replacement) = replacement {
                replacements.insert(dimension.clone(), replacement);
            }
        }
        query.map_dimension_symbols(&|symbol| transforms::substitute_by_name(symbol, &replacements))
    }
}

/// Measures of a query that agree on the dimension overrides applying to them.
pub struct OverridePartition {
    overrides: AppliedOverrides,
    measures: Vec<Rc<MemberSymbol>>,
}

impl OverridePartition {
    pub fn measures(&self) -> &[Rc<MemberSymbol>] {
        &self.measures
    }

    pub fn has_overrides(&self) -> bool {
        !self.overrides.is_empty()
    }

    /// `query` with this partition's overrides applied.
    pub fn apply(&self, query: &QueryProperties) -> Result<Rc<QueryProperties>, CubeError> {
        self.overrides.apply(query)
    }

    /// The leg of `query` computing this partition's measures.
    pub fn leg(&self, query: &QueryProperties) -> Result<Rc<QueryProperties>, CubeError> {
        self.apply(&query.leg_for_measures(self.measures.clone()))
    }
}

/// The query's measures split by the dimension overrides that apply to them,
/// in the order the measures come. A query without measures has no partitions.
pub fn partition_by_dimension_overrides(
    query: &QueryProperties,
) -> Result<Vec<OverridePartition>, CubeError> {
    let used = used_dimensions(query)
        .into_iter()
        .map(|symbol| symbol.id().clone())
        .collect::<HashSet<_>>();
    let mut partitions: Vec<OverridePartition> = Vec::new();
    for measure in query.all_used_measures()? {
        let overrides = AppliedOverrides::for_measure(&measure, &used);
        if !overrides.is_empty() && has_multi_stage_members(&measure.peel_refs(), false)? {
            return Err(CubeError::user(format!(
                "Dimension overrides of multi-stage measure {} are not supported yet",
                measure.full_name()
            )));
        }
        match partitions.iter_mut().find(|p| p.overrides == overrides) {
            Some(partition) => partition.measures.push(measure),
            None => partitions.push(OverridePartition {
                overrides,
                measures: vec![measure],
            }),
        }
    }
    if query.ungrouped() && partitions.len() > 1 {
        return Err(CubeError::user(
            "An ungrouped query can't have measures that need different dimensions".to_string(),
        ));
    }
    Ok(partitions)
}

/// Whether every override of `measures` that applies to `dimensions` is
/// applied to them. A query merging measures of different overrides is not:
/// only its legs are.
pub fn are_overrides_applied(
    measures: &[Rc<MemberSymbol>],
    dimensions: &[Rc<MemberSymbol>],
) -> bool {
    let dimensions = dimensions
        .iter()
        .map(time_dimension_base)
        .collect::<Vec<_>>();
    let used = dimensions
        .iter()
        .map(|symbol| symbol.id().clone())
        .collect::<HashSet<_>>();
    measures.iter().all(|measure| {
        let overrides = AppliedOverrides::for_measure(measure, &used);
        overrides
            .items
            .iter()
            .zip(&overrides.targets)
            .all(|((dimension, _), target)| {
                dimensions
                    .iter()
                    .filter(|symbol| symbol.id() == dimension)
                    .all(|symbol| {
                        symbol
                            .as_ref_symbol()
                            .ok()
                            .and_then(|reference| reference.target_member().cloned())
                            .is_some_and(|member| member.id() == target.id())
                    })
            })
    })
}

// The view dimensions a query groups, filters or orders by. A time dimension
// counts as the dimension it is a granularity of.
fn used_dimensions(query: &QueryProperties) -> Vec<Rc<MemberSymbol>> {
    let mut members = query.dimensions().clone();
    members.extend(query.time_dimensions().iter().cloned());
    for item in query
        .dimensions_filters()
        .iter()
        .chain(query.time_dimensions_filters())
    {
        item.find_all_member_evaluators(&mut members);
    }
    members.extend(query.extract_dimensions_from_order());
    members.iter().map(time_dimension_base).collect()
}

fn time_dimension_base(member: &Rc<MemberSymbol>) -> Rc<MemberSymbol> {
    match member.as_time_dimension() {
        Ok(time_dimension) => time_dimension.base_symbol().clone(),
        Err(_) => member.clone(),
    }
}

fn query_dimension(query: &QueryProperties, id: &MemberId) -> Option<Rc<MemberSymbol>> {
    used_dimensions(query)
        .into_iter()
        .find(|symbol| symbol.id() == id)
}
