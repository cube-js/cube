use crate::planner::collectors::{collect_join_hints, has_multi_stage_members};
use crate::planner::filter::FilterItem;
use crate::planner::join_hints::JoinHints;
use crate::planner::planners::JoinTreeBuilder;
use crate::planner::query_tools::JoinKey;
use crate::planner::query_tools::QueryTools;
use crate::planner::JoinTree;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

/// A single measure paired with the complete set of join hints it
/// needs — base query hints plus the measure's own incremental hints.
#[derive(Clone, Debug)]
pub struct MeasureJoinHints {
    pub measure: Rc<MemberSymbol>,
    pub hints: JoinHints,
}

/// Accumulates the join-hint context of a query (initial
/// `query_join_hints`, dimensions, filters) and produces
/// `MeasuresJoinHints` for a given set of measures.
pub struct MeasuresJoinHintsBuilder {
    initial_hints: JoinHints,
    dimensions: Vec<Rc<MemberSymbol>>,
    filters: Vec<FilterItem>,
}

impl MeasuresJoinHintsBuilder {
    pub fn add_dimensions(mut self, dims: &[Rc<MemberSymbol>]) -> Self {
        self.dimensions.extend(dims.iter().cloned());
        self
    }

    pub fn add_filters(mut self, filters: &[FilterItem]) -> Self {
        self.filters.extend(filters.iter().cloned());
        self
    }

    pub fn build(self, measures: &[Rc<MemberSymbol>]) -> Result<MeasuresJoinHints, CubeError> {
        let mut base_hints = self.initial_hints;

        for sym in self.dimensions.iter() {
            base_hints.extend(&collect_join_hints(sym)?);
        }

        let mut filter_symbols = Vec::new();
        for item in self.filters.iter() {
            item.find_all_member_evaluators(&mut filter_symbols);
        }
        for sym in filter_symbols.iter() {
            base_hints.extend(&collect_join_hints(sym)?);
        }

        MeasuresJoinHints::from_base_hints(base_hints, measures)
    }
}

/// Join-hint context of a query split into:
///
/// - `base_hints` — hints shared across all measures (query-level
///   hints plus those collected from dimensions and filters).
/// - `measure_hints` — per-measure incremental hints, one entry per
///   non-multi-stage measure. Multi-stage measures plan their joins
///   separately and are skipped here.
#[derive(Clone, Debug)]
pub struct MeasuresJoinHints {
    base_hints: JoinHints,
    measure_hints: Vec<MeasureJoinHints>,
}

impl MeasuresJoinHints {
    /// Start a builder seeded with the query-level join hints.
    pub fn builder(query_join_hints: &JoinHints) -> MeasuresJoinHintsBuilder {
        MeasuresJoinHintsBuilder {
            initial_hints: query_join_hints.clone(),
            dimensions: Vec::new(),
            filters: Vec::new(),
        }
    }

    /// Reuse the existing `base_hints` to produce a new
    /// `MeasuresJoinHints` over a different measure subset.
    pub fn for_measures(&self, measures: &[Rc<MemberSymbol>]) -> Result<Self, CubeError> {
        Self::from_base_hints(self.base_hints.clone(), measures)
    }

    fn from_base_hints(
        base_hints: JoinHints,
        measures: &[Rc<MemberSymbol>],
    ) -> Result<Self, CubeError> {
        let mut filtered_measures = Vec::new();
        for m in measures {
            if !has_multi_stage_members(m, true)? {
                filtered_measures.push(m.clone());
            }
        }

        let measure_hints: Vec<MeasureJoinHints> = filtered_measures
            .iter()
            .map(|m| -> Result<_, CubeError> {
                let mut hints = base_hints.clone();
                hints.extend(&collect_join_hints(m)?);
                Ok(MeasureJoinHints {
                    measure: m.clone(),
                    hints,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            base_hints,
            measure_hints,
        })
    }

    pub fn base_hints(&self) -> &JoinHints {
        &self.base_hints
    }

    pub fn measure_hints(&self) -> &[MeasureJoinHints] {
        &self.measure_hints
    }

    /// Combined hints for a specific measure (base plus the measure's
    /// own), or `None` when the measure has no entry — typically
    /// because it is multi-stage.
    pub fn hints_for_measure(&self, measure: &MemberSymbol) -> Option<JoinHints> {
        self.measure_hints
            .iter()
            .find(|mh| mh.measure.full_name() == measure.full_name())
            .map(|mh| mh.hints.clone())
    }
}

// --- MultiFactJoinGroups: builds actual join trees ---

/// Resolves a query's `MeasuresJoinHints` into concrete join trees
/// and groups measures by the `JoinKey` of the cube graph they share.
/// More than one group means the query is **multi-fact**: it touches
/// measures from cubes that cannot be combined under a single join.
///
/// Per-cube join paths from the root of each group are precomputed so
/// `resolve_join_path_for_dimension` / `resolve_join_path_for_measure`
/// are cheap lookups at render time.
#[derive(Clone)]
pub struct MultiFactJoinGroups {
    query_tools: Rc<QueryTools>,
    measures_join_hints: MeasuresJoinHints,
    groups: Vec<(Rc<JoinTree>, Vec<Rc<MemberSymbol>>)>,
    /// cube_name → join path from root, computed from the first group (shared for dimensions).
    dimension_paths: HashMap<String, Vec<String>>,
    /// measure full_name → join path from root, computed per group.
    measure_paths: HashMap<String, Vec<String>>,
}

impl MultiFactJoinGroups {
    /// Builds the join trees from `measures_join_hints` and groups
    /// measures by the resulting `JoinKey`.
    pub fn try_new(
        query_tools: Rc<QueryTools>,
        measures_join_hints: MeasuresJoinHints,
    ) -> Result<Self, CubeError> {
        let groups = Self::build_groups(&query_tools, &measures_join_hints)?;
        let (dimension_paths, measure_paths) = Self::precompute_paths(&groups);
        Ok(Self {
            query_tools,
            measures_join_hints,
            groups,
            dimension_paths,
            measure_paths,
        })
    }

    /// Rebuilds the groups for a different measure subset, reusing
    /// the shared `base_hints`.
    pub fn for_measures(&self, measures: &[Rc<MemberSymbol>]) -> Result<Self, CubeError> {
        let new_hints = self.measures_join_hints.for_measures(measures)?;
        Self::try_new(self.query_tools.clone(), new_hints)
    }

    fn build_groups(
        query_tools: &Rc<QueryTools>,
        hints: &MeasuresJoinHints,
    ) -> Result<Vec<(Rc<JoinTree>, Vec<Rc<MemberSymbol>>)>, CubeError> {
        let join_tree_builder = JoinTreeBuilder::new(query_tools.clone());
        let resolve = |join_hints: &JoinHints| -> Result<(JoinKey, Rc<JoinTree>), CubeError> {
            query_tools.join_tree_cache().get_or_build(join_hints, || {
                let (key, join) = query_tools.join_for_hints(join_hints)?;
                Ok((key, join_tree_builder.build(join)?))
            })
        };

        let measures_to_join = if hints.measure_hints.is_empty() {
            if hints.base_hints.is_empty() {
                vec![]
            } else {
                let (key, join_tree) = resolve(&hints.base_hints)?;
                vec![(Vec::new(), key, join_tree)]
            }
        } else {
            hints
                .measure_hints
                .iter()
                .map(|mh| -> Result<_, CubeError> {
                    let (key, join_tree) = resolve(&mh.hints)?;
                    Ok((vec![mh.measure.clone()], key, join_tree))
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        let mut key_order: Vec<JoinKey> = Vec::new();
        let mut grouped: HashMap<JoinKey, (Rc<JoinTree>, Vec<Rc<MemberSymbol>>)> = HashMap::new();
        for (measures, key, join_tree) in measures_to_join {
            if let Some(entry) = grouped.get_mut(&key) {
                entry.1.extend(measures);
            } else {
                key_order.push(key.clone());
                grouped.insert(key, (join_tree, measures));
            }
        }

        Ok(key_order
            .into_iter()
            .map(|key| grouped.remove(&key).unwrap())
            .collect())
    }

    pub fn measures_join_hints(&self) -> &MeasuresJoinHints {
        &self.measures_join_hints
    }

    /// True when the query's measures span more than one join tree.
    pub fn is_multi_fact(&self) -> bool {
        self.groups.len() > 1
    }

    /// True when any grouped measure sits on a cube that the join tree
    /// of its group multiplies (one-to-many join below the measure).
    pub fn has_multiplied_measures(&self) -> bool {
        self.groups
            .iter()
            .any(|(join, measures)| measures.iter().any(|m| join.is_multiplied(&m.cube_name())))
    }

    pub fn groups(&self) -> &[(Rc<JoinTree>, Vec<Rc<MemberSymbol>>)] {
        &self.groups
    }

    pub fn num_groups(&self) -> usize {
        self.groups.len()
    }

    /// Returns the join path from root to the dimension's cube.
    /// Precomputed from the first group (dimension paths are identical across all groups).
    pub fn resolve_join_path_for_dimension(
        &self,
        dimension: &Rc<MemberSymbol>,
    ) -> Option<&Vec<String>> {
        self.dimension_paths
            .get(&dimension.clone().resolve_reference_chain().cube_name())
    }

    /// Returns the join path from root to the measure's cube.
    /// Precomputed per measure from its group's JoinDefinition.
    pub fn resolve_join_path_for_measure(
        &self,
        measure: &Rc<MemberSymbol>,
    ) -> Option<&Vec<String>> {
        self.measure_paths
            .get(&measure.clone().resolve_reference_chain().full_name())
    }

    fn precompute_paths(
        groups: &[(Rc<JoinTree>, Vec<Rc<MemberSymbol>>)],
    ) -> (HashMap<String, Vec<String>>, HashMap<String, Vec<String>>) {
        let dimension_paths = if groups.is_empty() {
            HashMap::new()
        } else {
            Self::build_cube_paths(&groups[0].0)
        };

        let mut measure_paths = HashMap::new();
        for (join, measures) in groups {
            if measures.is_empty() {
                continue;
            }
            let cube_paths = Self::build_cube_paths(join);
            for m in measures {
                if let Some(path) = cube_paths.get(&m.cube_name()) {
                    measure_paths.insert(m.full_name(), path.clone());
                }
            }
        }

        (dimension_paths, measure_paths)
    }

    fn build_cube_paths(join: &JoinTree) -> HashMap<String, Vec<String>> {
        let root = join.root().name().clone();
        let mut paths: HashMap<String, Vec<String>> = HashMap::new();
        paths.insert(root.clone(), vec![root]);

        for join_item in join.joins() {
            let original_from = join_item.original_from().to_string();
            let original_to = join_item.cube().name().clone();
            let parent_path = paths
                .get(&original_from)
                .cloned()
                .unwrap_or_else(|| vec![original_from]);
            let mut path = parent_path;
            path.push(original_to.clone());
            paths.insert(original_to, path);
        }

        paths
    }

    /// The only join when the query has exactly one group; errors if
    /// the query is multi-fact, and `Ok(None)` if it has no groups
    /// at all.
    pub fn single_join(&self) -> Result<Option<Rc<JoinTree>>, CubeError> {
        if self.groups.is_empty() {
            return Ok(None);
        }
        if self.groups.len() > 1 {
            return Err(CubeError::internal(format!(
                "Expected just one multi-fact join group for simple query but got multiple: {}",
                self.groups
                    .iter()
                    .map(|(_, measures)| format!(
                        "({})",
                        measures.iter().map(|m| m.full_name()).join(", ")
                    ))
                    .join(", ")
            )));
        }
        Ok(Some(self.groups.first().unwrap().0.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::cube_bridge::MockSchema;
    use crate::test_fixtures::test_utils::TestContext;

    #[test]
    fn test_single_fact_one_group() {
        let schema = MockSchema::from_yaml_file("common/multi_fact.yaml");
        let ctx = TestContext::new(schema).unwrap();

        let orders_count = ctx.create_symbol("orders.count").unwrap();
        let customers_name = ctx.create_symbol("customers.name").unwrap();

        let hints = MeasuresJoinHints::builder(&JoinHints::new())
            .add_dimensions(&[customers_name])
            .build(&[orders_count])
            .unwrap();

        let groups = MultiFactJoinGroups::try_new(ctx.query_tools().clone(), hints).unwrap();

        assert!(!groups.is_multi_fact());
        assert_eq!(groups.num_groups(), 1);
        assert!(groups.single_join().unwrap().is_some());
    }

    #[test]
    fn test_multi_fact_two_groups() {
        let schema = MockSchema::from_yaml_file("common/multi_fact.yaml");
        let ctx = TestContext::new(schema).unwrap();

        let orders_count = ctx.create_symbol("orders.count").unwrap();
        let returns_count = ctx.create_symbol("returns.count").unwrap();
        let customers_name = ctx.create_symbol("customers.name").unwrap();

        let hints = MeasuresJoinHints::builder(&JoinHints::new())
            .add_dimensions(&[customers_name])
            .build(&[orders_count, returns_count])
            .unwrap();

        let groups = MultiFactJoinGroups::try_new(ctx.query_tools().clone(), hints).unwrap();

        assert!(groups.is_multi_fact());
        assert_eq!(groups.num_groups(), 2);
        assert!(groups.single_join().is_err());
    }

    #[test]
    fn test_resolve_join_path_for_measure() {
        let schema = MockSchema::from_yaml_file("common/multi_fact.yaml");
        let ctx = TestContext::new(schema).unwrap();

        let orders_count = ctx.create_symbol("orders.count").unwrap();
        let customers_name = ctx.create_symbol("customers.name").unwrap();

        let hints = MeasuresJoinHints::builder(&JoinHints::new())
            .add_dimensions(&[customers_name])
            .build(std::slice::from_ref(&orders_count))
            .unwrap();

        let groups = MultiFactJoinGroups::try_new(ctx.query_tools().clone(), hints).unwrap();

        assert_eq!(
            groups.resolve_join_path_for_measure(&orders_count),
            Some(&vec!["customers".to_string(), "orders".to_string()])
        );
    }

    #[test]
    fn test_resolve_join_path_for_dimension() {
        let schema = MockSchema::from_yaml_file("common/multi_fact.yaml");
        let ctx = TestContext::new(schema).unwrap();

        let orders_count = ctx.create_symbol("orders.count").unwrap();
        let customers_name = ctx.create_symbol("customers.name").unwrap();

        let hints = MeasuresJoinHints::builder(&JoinHints::new())
            .add_dimensions(std::slice::from_ref(&customers_name))
            .build(&[orders_count])
            .unwrap();

        let groups = MultiFactJoinGroups::try_new(ctx.query_tools().clone(), hints).unwrap();

        assert_eq!(
            groups.resolve_join_path_for_dimension(&customers_name),
            Some(&vec!["customers".to_string()])
        );
    }

    #[test]
    fn test_resolve_join_paths_multi_fact() {
        let schema = MockSchema::from_yaml_file("common/multi_fact.yaml");
        let ctx = TestContext::new(schema).unwrap();

        let orders_count = ctx.create_symbol("orders.count").unwrap();
        let returns_count = ctx.create_symbol("returns.count").unwrap();
        let customers_name = ctx.create_symbol("customers.name").unwrap();

        let hints = MeasuresJoinHints::builder(&JoinHints::new())
            .add_dimensions(std::slice::from_ref(&customers_name))
            .build(&[orders_count.clone(), returns_count.clone()])
            .unwrap();

        let groups = MultiFactJoinGroups::try_new(ctx.query_tools().clone(), hints).unwrap();

        assert_eq!(
            groups.resolve_join_path_for_measure(&orders_count),
            Some(&vec!["customers".to_string(), "orders".to_string()])
        );
        assert_eq!(
            groups.resolve_join_path_for_measure(&returns_count),
            Some(&vec!["customers".to_string(), "returns".to_string()])
        );
        assert_eq!(
            groups.resolve_join_path_for_dimension(&customers_name),
            Some(&vec!["customers".to_string()])
        );
        // Unknown measure
        let unknown = ctx.create_symbol("customers.count").unwrap();
        assert!(groups.resolve_join_path_for_measure(&unknown).is_none());
    }
}
