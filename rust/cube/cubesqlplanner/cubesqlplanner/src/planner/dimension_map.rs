use super::query_properties::QueryProperties;
use super::symbols::transforms;
use super::{MemberId, MemberSymbol};
use cubenativeutils::CubeError;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// The view dimensions a measure means differently, mapped to what they mean
/// for it: the mappings of a view measure whose dimension the query uses.
/// Ordered by dimension, so equal sets compare equal.
#[derive(Clone, Default)]
pub struct AppliedMappings {
    items: Vec<(MemberId, MemberId)>,
    targets: Vec<Rc<MemberSymbol>>,
}

impl PartialEq for AppliedMappings {
    fn eq(&self, other: &Self) -> bool {
        self.items == other.items
    }
}

impl AppliedMappings {
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// The mappings of `measure` whose view dimension is in `used`. A
    /// dimension is matched by its own id, so a member that only references
    /// the same model member is a different dimension.
    pub fn for_measure(measure: &Rc<MemberSymbol>, used: &HashSet<MemberId>) -> Self {
        let Ok(reference) = measure.as_ref_symbol() else {
            return Self::default();
        };
        let (items, targets) = reference
            .dimension_map()
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

    /// `query` with each mapped view dimension standing for its mapping,
    /// under its own name, wherever the query groups, filters or orders by it.
    pub fn apply(&self, query: &QueryProperties) -> Result<Rc<QueryProperties>, CubeError> {
        let used = used_dimensions(query);
        let mut replacements = HashMap::new();
        for ((dimension, _), target) in self.items.iter().zip(&self.targets) {
            let Some(symbol) = used.iter().find(|symbol| symbol.id() == dimension) else {
                continue;
            };
            let reference = symbol.as_ref_symbol()?;
            if reference.mask_sql().is_some() {
                return Err(CubeError::user(format!(
                    "Masked dimension {} can't be mapped yet",
                    symbol.full_name()
                )));
            }
            let replacement = MemberSymbol::new_ref(reference.with_target(target.clone()));
            replacements.insert(dimension.clone(), replacement);
        }
        query.map_dimension_symbols(&|symbol| transforms::substitute_by_name(symbol, &replacements))
    }

    fn has_dimension(&self, dimension: &MemberId) -> bool {
        self.items.iter().any(|(d, _)| d == dimension)
    }

    // `symbol` is a mapped dimension still referencing another member
    // than its mapping.
    fn is_unapplied_to(&self, symbol: &Rc<MemberSymbol>) -> bool {
        self.items.iter().any(|(dimension, target)| {
            dimension == symbol.id()
                && !symbol
                    .as_ref_symbol()
                    .ok()
                    .and_then(|reference| reference.target_member().cloned())
                    .is_some_and(|member| member.id() == target)
        })
    }
}

/// Measures of a query that agree on the dimension mappings applying to them.
pub struct MappingPartition {
    mappings: AppliedMappings,
    measures: Vec<Rc<MemberSymbol>>,
}

impl MappingPartition {
    pub fn measures(&self) -> &[Rc<MemberSymbol>] {
        &self.measures
    }

    pub fn has_mappings(&self) -> bool {
        !self.mappings.is_empty()
    }

    /// `query` with this partition's mappings applied.
    pub fn apply(&self, query: &QueryProperties) -> Result<Rc<QueryProperties>, CubeError> {
        self.mappings.apply(query)
    }

    /// The leg of `query` computing this partition's measures.
    pub fn leg(&self, query: &QueryProperties) -> Result<Rc<QueryProperties>, CubeError> {
        self.apply(&query.leg_for_measures(self.measures.clone()))
    }
}

/// The query's measures split by the dimension mappings that apply to them,
/// in the order the measures come. A query without measures has no partitions.
pub fn partition_by_dimension_map(
    query: &QueryProperties,
) -> Result<Vec<MappingPartition>, CubeError> {
    let used_symbols = used_dimensions(query);
    let used = used_symbols
        .iter()
        .map(|symbol| symbol.id().clone())
        .collect::<HashSet<_>>();
    let mut partitions: Vec<MappingPartition> = Vec::new();
    let mut leaves: Vec<Rc<MemberSymbol>> = Vec::new();
    for measure in query.all_used_measures()? {
        for leaf in view_composite_leaves(&measure) {
            if !leaves.iter().any(|l| l.id() == leaf.id()) {
                leaves.push(leaf);
            }
        }
    }
    for measure in leaves {
        if let Some(nested) = nested_mapped_measure(&measure, &used) {
            return Err(CubeError::user(format!(
                "Measure {} is built on {}, whose dimension mappings apply to this query, which is not supported yet",
                measure.full_name(),
                nested.full_name()
            )));
        }
        let mappings = AppliedMappings::for_measure(&measure, &used);
        check_no_dimension_left_behind(&measure, &mappings, &used_symbols)?;
        match partitions.iter_mut().find(|p| p.mappings == mappings) {
            Some(partition) => partition.measures.push(measure),
            None => partitions.push(MappingPartition {
                mappings,
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

/// A measure a view declares over view measures only, computed over the
/// merged legs of its leaves rather than in one of them.
pub fn is_view_composite(measure: &Rc<MemberSymbol>) -> bool {
    let Ok(symbol) = measure.as_measure() else {
        return false;
    };
    let dependencies = measure.get_dependencies();
    symbol.is_view()
        && !symbol.is_multi_stage()
        && !dependencies.is_empty()
        && dependencies
            .iter()
            .all(|d| d.peel_refs().as_measure().is_ok() && d.cube_id() == measure.cube_id())
}

// The measures `measure` is computed from: itself, or the leaves of the view
// composites it is.
fn view_composite_leaves(measure: &Rc<MemberSymbol>) -> Vec<Rc<MemberSymbol>> {
    if !is_view_composite(measure) {
        return vec![measure.clone()];
    }
    measure
        .get_dependencies()
        .iter()
        .flat_map(view_composite_leaves)
        .collect()
}

/// Whether every mapping of `measures` that applies to `dimensions` is
/// applied to them. A query merging measures of different mappings is not:
/// only its legs are.
pub fn are_mappings_applied(
    measures: &[Rc<MemberSymbol>],
    dimensions: &[Rc<MemberSymbol>],
) -> bool {
    let dimensions = dimension_trees(dimensions.to_vec());
    let used = dimensions
        .iter()
        .map(|symbol| symbol.id().clone())
        .collect::<HashSet<_>>();
    measures.iter().all(|measure| {
        let mappings = AppliedMappings::for_measure(measure, &used);
        mappings
            .items
            .iter()
            .zip(&mappings.targets)
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

/// Whether a mapping of `measures` applies to `dimensions`: the query is a
/// leg of measures under dimension mappings.
pub fn has_mappings_for(measures: &[Rc<MemberSymbol>], dimensions: &[Rc<MemberSymbol>]) -> bool {
    let used = dimension_trees(dimensions.to_vec())
        .iter()
        .map(|symbol| symbol.id().clone())
        .collect::<HashSet<_>>();
    measures
        .iter()
        .any(|measure| !AppliedMappings::for_measure(measure, &used).is_empty())
}

// The dimensions a query groups, filters or orders by, with the dimensions
// they are built on: a dimension a view declares over a mapped one is
// mapped with it.
fn used_dimensions(query: &QueryProperties) -> Vec<Rc<MemberSymbol>> {
    let mut members = query.dimensions().clone();
    members.extend(query.time_dimensions().iter().cloned());
    for item in query
        .dimensions_filters()
        .iter()
        .chain(query.time_dimensions_filters())
        .chain(query.segments())
    {
        item.find_all_member_evaluators(&mut members);
    }
    members.extend(query.extract_dimensions_from_order());
    dimension_trees(members)
}

// A view dimension's reference is not walked: the model member behind it can't
// name a view dimension.
fn dimension_trees(roots: Vec<Rc<MemberSymbol>>) -> Vec<Rc<MemberSymbol>> {
    let mut result = Vec::new();
    let mut seen = HashSet::new();
    let mut pending = roots;
    while let Some(member) = pending.pop() {
        if !seen.insert(member.id().clone()) {
            continue;
        }
        // Its dependencies see through the view dimension it is a granularity of.
        if let Ok(time_dimension) = member.as_time_dimension() {
            pending.push(time_dimension.base_symbol().clone());
        }
        if !matches!(member.as_ref(), MemberSymbol::Ref(_)) {
            pending.extend(
                member
                    .get_dependencies()
                    .into_iter()
                    .filter(|dependency| dependency.peel_refs().as_measure().is_err()),
            );
        }
        result.push(member);
    }
    result
}

// A view measure with mappings applying to the query, found under another
// measure: its mappings would be lost there.
fn nested_mapped_measure(
    measure: &Rc<MemberSymbol>,
    used: &HashSet<MemberId>,
) -> Option<Rc<MemberSymbol>> {
    let mut seen = HashSet::new();
    let mut pending = measure.get_dependencies();
    while let Some(member) = pending.pop() {
        if !seen.insert(member.id().clone()) {
            continue;
        }
        if !AppliedMappings::for_measure(&member, used).is_empty() {
            return Some(member);
        }
        pending.extend(member.get_dependencies());
    }
    None
}

// A dimension of the cube whose other dimensions a measure maps would be
// read over the default join: on another date, say, than the mapped ones.
fn check_no_dimension_left_behind(
    measure: &Rc<MemberSymbol>,
    mappings: &AppliedMappings,
    used: &[Rc<MemberSymbol>],
) -> Result<(), CubeError> {
    let referenced_cube = |symbol: &Rc<MemberSymbol>| {
        symbol
            .as_ref_symbol()
            .ok()
            .and_then(|reference| reference.target_member().cloned())
            .map(|target| target.cube_id())
    };
    // Once applied, a mapped dimension references its mapping and says
    // nothing about the cube it was taken from. The measure's own cube is left
    // out: its other columns have no role of their own.
    let own_cube = measure.peel_refs().cube_id();
    let mapped_cubes = used
        .iter()
        .filter(|symbol| mappings.is_unapplied_to(symbol))
        .filter_map(referenced_cube)
        .filter(|cube| *cube != own_cube)
        .collect::<Vec<_>>();
    for symbol in used.iter() {
        if mappings.has_dimension(symbol.id()) {
            continue;
        }
        if let Some(cube) = referenced_cube(symbol).filter(|cube| mapped_cubes.contains(cube)) {
            return Err(CubeError::user(format!(
                "Dimension {} is not mapped for measure {}, which maps other dimensions of {}",
                symbol.full_name(),
                measure.full_name(),
                cube
            )));
        }
    }
    Ok(())
}
