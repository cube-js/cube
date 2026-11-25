use super::query_properties::QueryProperties;
use super::symbols::transforms;
use super::{MemberId, MemberSymbol};
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
        let used = used_dimensions(query);
        let mut replacements = HashMap::new();
        for ((dimension, _), target) in self.items.iter().zip(&self.targets) {
            let Some(symbol) = used.iter().find(|symbol| symbol.id() == dimension) else {
                continue;
            };
            let reference = symbol.as_ref_symbol()?;
            if reference.mask_sql().is_some() {
                return Err(CubeError::user(format!(
                    "Masked dimension {} can't be overridden yet",
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

    // `symbol` is an overridden dimension still referencing another member
    // than its override.
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
    let used_symbols = used_dimensions(query);
    let used = used_symbols
        .iter()
        .map(|symbol| symbol.id().clone())
        .collect::<HashSet<_>>();
    let mut partitions: Vec<OverridePartition> = Vec::new();
    for measure in query.all_used_measures()? {
        if let Some(nested) = nested_overridden_measure(&measure, &used) {
            return Err(CubeError::user(format!(
                "Measure {} is built on {}, whose dimension overrides apply to this query, which is not supported yet",
                measure.full_name(),
                nested.full_name()
            )));
        }
        let overrides = AppliedOverrides::for_measure(&measure, &used);
        check_no_dimension_left_behind(&measure, &overrides, &used_symbols)?;
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
    let dimensions = dimension_trees(dimensions.to_vec());
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

/// Whether an override of `measures` applies to `dimensions`: the query is a
/// leg of measures under dimension overrides.
pub fn has_overrides_for(measures: &[Rc<MemberSymbol>], dimensions: &[Rc<MemberSymbol>]) -> bool {
    let used = dimension_trees(dimensions.to_vec())
        .iter()
        .map(|symbol| symbol.id().clone())
        .collect::<HashSet<_>>();
    measures
        .iter()
        .any(|measure| !AppliedOverrides::for_measure(measure, &used).is_empty())
}

// The dimensions a query groups, filters or orders by, with the dimensions
// they are built on: a dimension a view declares over an overridden one is
// overridden with it.
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

// A view measure with overrides applying to the query, found under another
// measure: its overrides would be lost there.
fn nested_overridden_measure(
    measure: &Rc<MemberSymbol>,
    used: &HashSet<MemberId>,
) -> Option<Rc<MemberSymbol>> {
    let mut seen = HashSet::new();
    let mut pending = measure.get_dependencies();
    while let Some(member) = pending.pop() {
        if !seen.insert(member.id().clone()) {
            continue;
        }
        if !AppliedOverrides::for_measure(&member, used).is_empty() {
            return Some(member);
        }
        pending.extend(member.get_dependencies());
    }
    None
}

// A dimension of the cube whose other dimensions a measure overrides would be
// read over the default join: on another date, say, than the overridden ones.
fn check_no_dimension_left_behind(
    measure: &Rc<MemberSymbol>,
    overrides: &AppliedOverrides,
    used: &[Rc<MemberSymbol>],
) -> Result<(), CubeError> {
    let referenced_cube = |symbol: &Rc<MemberSymbol>| {
        symbol
            .as_ref_symbol()
            .ok()
            .and_then(|reference| reference.target_member().cloned())
            .map(|target| target.cube_id())
    };
    // Once applied, an overridden dimension references its override and says
    // nothing about the cube it was taken from. The measure's own cube is left
    // out: its other columns have no role of their own.
    let own_cube = measure.peel_refs().cube_id();
    let overridden_cubes = used
        .iter()
        .filter(|symbol| overrides.is_unapplied_to(symbol))
        .filter_map(referenced_cube)
        .filter(|cube| *cube != own_cube)
        .collect::<Vec<_>>();
    for symbol in used.iter() {
        if overrides.has_dimension(symbol.id()) {
            continue;
        }
        if let Some(cube) = referenced_cube(symbol).filter(|cube| overridden_cubes.contains(cube)) {
            return Err(CubeError::user(format!(
                "Dimension {} is not overridden for measure {}, which overrides other dimensions of {}",
                symbol.full_name(),
                measure.full_name(),
                cube
            )));
        }
    }
    Ok(())
}
