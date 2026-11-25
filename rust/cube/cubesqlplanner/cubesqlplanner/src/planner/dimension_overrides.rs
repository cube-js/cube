use super::query_properties::QueryProperties;
use super::symbols::transforms;
use super::{MemberId, MemberSymbol};
use crate::planner::collectors::has_multi_stage_members;
use cubenativeutils::CubeError;
use itertools::Itertools;
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

    fn describe(&self) -> String {
        self.items
            .iter()
            .map(|(dimension, target)| format!("{dimension} -> {target}"))
            .join(", ")
    }
}

/// Applies the dimension overrides of the query's view measures. All measures
/// must agree on them; measures that need different dimensions are not
/// supported yet.
pub fn apply_dimension_overrides(
    query: &Rc<QueryProperties>,
) -> Result<Rc<QueryProperties>, CubeError> {
    let measures = query.all_used_measures()?;
    if measures.is_empty() {
        return Ok(query.clone());
    }
    let used = used_dimensions(query)
        .into_iter()
        .map(|symbol| symbol.id().clone())
        .collect::<HashSet<_>>();
    let applied = measures
        .iter()
        .map(|measure| (measure, AppliedOverrides::for_measure(measure, &used)))
        .collect_vec();
    if applied.iter().all(|(_, overrides)| overrides.is_empty()) {
        return Ok(query.clone());
    }
    for (measure, overrides) in applied.iter() {
        if !overrides.is_empty() && has_multi_stage_members(&measure.peel_refs(), false)? {
            return Err(CubeError::user(format!(
                "Dimension overrides of multi-stage measure {} are not supported yet",
                measure.full_name()
            )));
        }
    }
    let (first_measure, first) = &applied[0];
    if let Some((measure, overrides)) = applied.iter().find(|(_, o)| o != first) {
        return Err(CubeError::user(format!(
            "Measures {} and {} need different dimensions in one query ({} vs {}), which is not supported yet",
            first_measure.full_name(),
            measure.full_name(),
            describe_or_none(first),
            describe_or_none(overrides),
        )));
    }
    first.apply(query)
}

fn describe_or_none(overrides: &AppliedOverrides) -> String {
    if overrides.is_empty() {
        "no overrides".to_string()
    } else {
        overrides.describe()
    }
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
    members
        .into_iter()
        .map(|member| match member.as_time_dimension() {
            Ok(time_dimension) => time_dimension.base_symbol().clone(),
            Err(_) => member,
        })
        .collect()
}

fn query_dimension(query: &QueryProperties, id: &MemberId) -> Option<Rc<MemberSymbol>> {
    used_dimensions(query)
        .into_iter()
        .find(|symbol| symbol.id() == id)
}
