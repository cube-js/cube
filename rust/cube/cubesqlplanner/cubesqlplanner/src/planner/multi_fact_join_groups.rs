use crate::cube_bridge::join_hints::JoinHintItem;
use crate::planner::collectors::{
    collect_join_hints, collect_multiplied_measures, has_expression_or_calculated_members,
    has_multi_stage_members,
};
use crate::planner::filter::FilterItem;
use crate::planner::join_hints::JoinHints;
use crate::planner::planners::JoinTreeBuilder;
use crate::planner::query_tools::JoinKey;
use crate::planner::state::State;
use crate::planner::JoinTree;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
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

        MeasuresJoinHints::from_base_hints(base_hints, measures, None)
    }
}

/// Join-hint context of a query split into:
///
/// - `base_hints` — hints shared across all measures (query-level
///   hints plus those collected from dimensions and filters).
/// - `measure_hints` — per-measure incremental hints, one entry per
///   non-multi-stage measure. Multi-stage measures plan their joins
///   separately and are skipped here.
/// - `hints_by_cube` — the hints the measures of the whole query collected,
///   grouped by the cube the measure itself belongs to, which for a measure of a
///   view is that view. Multi-stage measures are included. Only used to resolve a
///   measure that carries no hints of its own and sits on a view (see
///   `MultiFactJoinGroups::fallback_hints_for_measure`), which is why the
///   grouping matters: such a measure may only borrow from members of its own
///   view. It is inherited as-is when regrouping over a measure subset, so the
///   measure stays in the join tree of the query it came from.
///
///   Dimensions, filters and query-level join hints are deliberately absent: they
///   land in `base_hints`, so a measure of a query that has any of them never
///   reaches the fallback in the first place. That also means the view grouping
///   only guards the case where `base_hints` is empty - a dimension of an
///   unrelated view still pulls a hint-less member expression into its join.
#[derive(Clone, Debug)]
pub struct MeasuresJoinHints {
    base_hints: JoinHints,
    measure_hints: Vec<MeasureJoinHints>,
    hints_by_cube: HashMap<String, JoinHints>,
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
    /// `MeasuresJoinHints` over a different measure subset. `hints_by_cube`
    /// keeps describing the whole query, not the subset.
    pub fn for_measures(&self, measures: &[Rc<MemberSymbol>]) -> Result<Self, CubeError> {
        Self::from_base_hints(
            self.base_hints.clone(),
            measures,
            Some(self.hints_by_cube.clone()),
        )
    }

    /// `inherited_hints_by_cube` describes the whole query these measures were
    /// taken from, so the measures add nothing to it; without it they are grouped
    /// from scratch.
    fn from_base_hints(
        base_hints: JoinHints,
        measures: &[Rc<MemberSymbol>],
        inherited_hints_by_cube: Option<HashMap<String, JoinHints>>,
    ) -> Result<Self, CubeError> {
        let inherited = inherited_hints_by_cube.is_some();
        let mut hints_by_cube = inherited_hints_by_cube.unwrap_or_default();

        let mut measure_hints: Vec<MeasureJoinHints> = Vec::new();
        for m in measures {
            // Multi-stage measures plan their joins separately, so they get no
            // entry of their own - but their hints still count towards their
            // cube's. With inherited hints there is nothing left to collect
            // them for.
            let is_multi_stage = has_multi_stage_members(m, true)?;
            if is_multi_stage && inherited {
                continue;
            }
            let own_hints = collect_join_hints(m)?;
            if !inherited {
                hints_by_cube
                    .entry(m.cube_name())
                    .or_insert_with(JoinHints::new)
                    .extend(&own_hints);
            }
            if is_multi_stage {
                continue;
            }
            let mut hints = base_hints.clone();
            hints.extend(&own_hints);
            measure_hints.push(MeasureJoinHints {
                measure: m.clone(),
                hints,
            });
        }

        Ok(Self {
            base_hints,
            measure_hints,
            hints_by_cube,
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

/// One group while it is still being assembled: the measures gathered so far
/// plus what it takes to rebuild their join tree - the `JoinKey` to compare
/// trees by and the hints to resolve a merged tree from.
struct GroupBuild {
    key: JoinKey,
    tree: Rc<JoinTree>,
    measures: Vec<Rc<MemberSymbol>>,
    hints: JoinHints,
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
    query_tools: Rc<State>,
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
        query_tools: Rc<State>,
        measures_join_hints: MeasuresJoinHints,
    ) -> Result<Self, CubeError> {
        Self::build(query_tools, measures_join_hints, false)
    }

    /// Like `try_new`, but additionally folds a group into another one whose
    /// join tree contains its own - see `merge_nested_groups` for when that is
    /// allowed. One group less is one scan of the shared join less.
    ///
    /// Only the query's own grouping is built this way. Regrouping a measure
    /// subset through `for_measures` never merges: a pre-aggregation is matched
    /// against one leg at a time, so collapsing the legs of an already planned
    /// query would cost it rollups that are worth far more than the scan saved.
    pub fn try_new_merging_nested(
        query_tools: Rc<State>,
        measures_join_hints: MeasuresJoinHints,
    ) -> Result<Self, CubeError> {
        Self::build(query_tools, measures_join_hints, true)
    }

    fn build(
        query_tools: Rc<State>,
        measures_join_hints: MeasuresJoinHints,
        merge_nested: bool,
    ) -> Result<Self, CubeError> {
        let groups = Self::build_groups(&query_tools, &measures_join_hints, merge_nested)?;
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
        Self::build(self.query_tools.clone(), new_hints, false)
    }

    fn build_groups(
        query_tools: &Rc<State>,
        hints: &MeasuresJoinHints,
        merge_nested: bool,
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
                vec![(Vec::new(), key, join_tree, hints.base_hints.clone())]
            }
        } else {
            hints
                .measure_hints
                .iter()
                .map(|mh| -> Result<_, CubeError> {
                    let measure_hints = if mh.hints.is_empty() {
                        Self::fallback_hints_for_measure(query_tools, &mh.measure, hints)?
                    } else {
                        mh.hints.clone()
                    };
                    if measure_hints.is_empty() {
                        return Err(CubeError::user(format!(
                            "Can't resolve the cube to query for '{}': the member references no \
                             members of '{}', and neither the rest of the query nor the join map \
                             of '{}' gives a cube to join from",
                            mh.measure.full_name(),
                            mh.measure.cube_name(),
                            mh.measure.cube_name()
                        )));
                    }
                    let (key, join_tree) = resolve(&measure_hints)?;
                    Ok((vec![mh.measure.clone()], key, join_tree, measure_hints))
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        let mut key_order: Vec<JoinKey> = Vec::new();
        let mut grouped: HashMap<JoinKey, GroupBuild> = HashMap::new();
        for (measures, key, join_tree, join_hints) in measures_to_join {
            if let Some(entry) = grouped.get_mut(&key) {
                entry.measures.extend(measures);
                entry.hints.extend(&join_hints);
            } else {
                key_order.push(key.clone());
                grouped.insert(
                    key.clone(),
                    GroupBuild {
                        key,
                        tree: join_tree,
                        measures,
                        hints: join_hints,
                    },
                );
            }
        }

        let mut groups = key_order
            .into_iter()
            .map(|key| grouped.remove(&key).unwrap())
            .collect::<Vec<_>>();

        // Cheapest question first: without a nested pair there is nothing to
        // merge, and the pre-aggregation scan crosses the bridge once per cube.
        if merge_nested
            && Self::has_nested_pair(&groups)
            && !Self::any_cube_has_pre_aggregations(query_tools, &groups)?
        {
            Self::merge_nested_groups(&mut groups, &resolve)?;
        }

        Ok(groups
            .into_iter()
            .map(|group| (group.tree, group.measures))
            .collect())
    }

    /// Whether any group's join tree is contained in another's - the only
    /// shape `merge_nested_groups` can do anything with.
    fn has_nested_pair(groups: &[GroupBuild]) -> bool {
        groups.iter().any(|group| {
            groups
                .iter()
                .any(|other| group.key.is_nested_in(&other.key))
        })
    }

    /// Whether any cube these groups read defines a pre-aggregation.
    ///
    /// A rollup is matched against one group at a time, so groups folded
    /// together can only be served by a rollup spanning all of them, which
    /// usually does not exist - the query would fall back to reading the raw
    /// tables, costing it far more than the scan the merge saves. Deciding this
    /// up front is coarse: it stands down whenever a rollup could exist, not
    /// only when one would actually have matched.
    fn any_cube_has_pre_aggregations(
        query_tools: &Rc<State>,
        groups: &[GroupBuild],
    ) -> Result<bool, CubeError> {
        let mut seen = HashSet::new();
        for group in groups.iter() {
            let cubes = std::iter::once(group.tree.root().name().clone()).chain(
                group
                    .tree
                    .joins()
                    .iter()
                    .map(|item| item.cube().name().clone()),
            );
            for cube_name in cubes {
                if !seen.insert(cube_name.clone()) {
                    continue;
                }
                let pre_aggregations = query_tools
                    .cube_evaluator()
                    .pre_aggregations_for_cube_as_array(cube_name)?;
                if !pre_aggregations.is_empty() {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Folds a group into another one that walks the same cube graph further,
    /// so both are answered by one scan of the shared part instead of two.
    ///
    /// The extra joins of the wider tree are `LEFT`, so every row of the
    /// narrower one survives in it, each replicated one or more times. A
    /// measure therefore reads the same rows and answers the same value in
    /// both, provided that replication either does not reach it or does not
    /// change what it computes - which is what `group_survives_join` decides.
    ///
    /// The wider tree is rebuilt from both groups' join hints before that
    /// question is asked. A tree only knows whether it multiplies the cubes its
    /// own hints named, and the cube a moving measure sits on may be in the
    /// wider tree only as a stop on the way to something else, which would
    /// otherwise answer "not multiplied" for a cube the tree does in fact
    /// multiply. Rebuilding fills that in; a rebuild that comes back with a
    /// different key built different joins than the group already has, and is
    /// skipped rather than merged.
    ///
    /// Merging is re-checked against the accumulated measures on every pass, so
    /// a chain of nested trees only collapses as far as every measure carried
    /// along stays safe.
    fn merge_nested_groups(
        groups: &mut Vec<GroupBuild>,
        resolve: &impl Fn(&JoinHints) -> Result<(JoinKey, Rc<JoinTree>), CubeError>,
    ) -> Result<(), CubeError> {
        loop {
            let mut merged = None;
            'outer: for (i, group) in groups.iter().enumerate() {
                for (j, other) in groups.iter().enumerate() {
                    if i == j || !group.key.is_nested_in(&other.key) {
                        continue;
                    }
                    let mut hints = other.hints.clone();
                    hints.extend(&group.hints);
                    // Resolving is a probe: a hint set the join graph refuses
                    // means there is no merge to make here, not that the query
                    // the groups came from is unplannable.
                    let Ok((key, tree)) = resolve(&hints) else {
                        continue;
                    };
                    if key != other.key {
                        continue;
                    }
                    if Self::group_survives_join(&group.measures, &tree)? {
                        merged = Some((i, j, tree));
                        break 'outer;
                    }
                }
            }
            let Some((from, into, tree)) = merged else {
                return Ok(());
            };
            let group = groups.remove(from);
            // Removing the earlier index shifts everything after it.
            let into = if into > from { into - 1 } else { into };
            let target = &mut groups[into];
            target.measures.extend(group.measures);
            target.hints.extend(&group.hints);
            target.tree = tree;
        }
    }

    /// Whether every measure of a group computes the same value, by the same
    /// SQL, when evaluated over `join` instead of its own tree.
    ///
    /// Only measures built out of plain aggregated measures qualify, leaf by
    /// leaf: a leaf whose cube `join` does not multiply reads its own rows
    /// anyway, a multiplied one has to be immune to replication. A key-based
    /// count is not accepted - staying correct would mean switching it to the
    /// distinct `MultipliedCount` form, decided from the measure's own tree
    /// elsewhere.
    ///
    /// Multi-stage measures plan through their own CTE pipeline, not as a leaf
    /// of this join. Member expressions and calculated measures write SQL around
    /// their references, so an aggregate written there is a member of nothing
    /// and no leaf accounts for it.
    fn group_survives_join(
        measures: &[Rc<MemberSymbol>],
        join: &Rc<JoinTree>,
    ) -> Result<bool, CubeError> {
        for measure in measures.iter() {
            if has_multi_stage_members(measure, false)?
                || has_expression_or_calculated_members(measure)?
            {
                return Ok(false);
            }
            // `join` is only a candidate here, not the tree the query will
            // render, so a shape it rejects means this merge is off - not that
            // the query is unplannable.
            let Ok(items) = collect_multiplied_measures(measure, join) else {
                return Ok(false);
            };
            for item in items {
                let Ok(leaf) = item.measure.as_measure() else {
                    return Ok(false);
                };
                if item.multiplied && !leaf.kind().survives_row_multiplication() {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// Hints to use for a measure whose own hint set resolved to empty.
    /// Seeds the measure's owning cube when it is a real, joinable cube.
    ///
    /// A view is not a joinable cube, so it can't seed anything. Such a measure
    /// borrows the hints of the other members **of that same view** instead, and
    /// lands in the same join group as the members it borrowed from. Members of
    /// another view or of a bare cube are not borrowed from: their cubes need not
    /// appear in this view at all, and counting rows of a join tree the view is
    /// not built on would answer a different question than the one asked.
    ///
    /// Borrowing at all is what the legacy planner does, but it borrows wider: it
    /// unions the join hints of every query member into one join tree, with no
    /// notion of which view a member came from. Narrowing that union to the
    /// measure's own view is the difference here.
    ///
    /// When there is nothing to borrow from either, the view's own join map is
    /// the last resort: its paths start at the cube the view is rooted at, so
    /// that cube is the one to query. This is what makes a query built only from
    /// such member expressions, like `COUNT(*)` over a view, resolvable. It only
    /// covers views that have a join map at all: a view over a single directly
    /// joinable cube records no path, and such a query is rejected - see
    /// `test_expr_measure_count_star_only_member_on_view`.
    ///
    /// Note that borrowing makes the meaning of such a measure depend on the rest
    /// of the query: `COUNT(*)` over a view with two facts counts the rows of the
    /// cube the view is rooted at when selected alone, and the rows of the fanned
    /// out join tree when selected together with measures from both facts. The
    /// legacy planner behaves the same way, since it pools the hints of all query
    /// members into one join tree.
    ///
    /// Known hole, kept for legacy parity: the same-view rule only reaches
    /// measures. Dimensions, filters and query-level hints land in `base_hints`,
    /// which is not view-scoped, and a measure whose `base_hints` are non-empty
    /// never gets here at all. So a dimension of an *unrelated* view still drags a
    /// hint-less member expression into that view's join and yields a number for a
    /// join tree its own view is not built on - see
    /// `test_expr_measure_count_star_no_hints_beside_other_view_dimension`, which
    /// pins that behaviour. Closing it means resolving from the view bucket
    /// whenever the measure's *own* hints are empty, which would also make the
    /// ordinary shape - a view dimension next to `COUNT(*)` on the same view -
    /// depend on that bucket carrying dimensions, so it is a larger change than
    /// this fix.
    fn fallback_hints_for_measure(
        query_tools: &Rc<State>,
        measure: &Rc<MemberSymbol>,
        all_hints: &MeasuresJoinHints,
    ) -> Result<JoinHints, CubeError> {
        let cube_name = measure.cube_name();
        let cube_definition = query_tools
            .cube_evaluator()
            .cube_from_path(cube_name.clone())
            .ok();
        let is_view = cube_definition
            .as_ref()
            .and_then(|cube| cube.static_data().is_view)
            .unwrap_or(false);
        if !is_view {
            return Ok(JoinHints::from_items(vec![JoinHintItem::Single(cube_name)]));
        }

        match all_hints.hints_by_cube.get(&cube_name) {
            Some(hints) if !hints.is_empty() => return Ok(hints.clone()),
            _ => {}
        }

        let join_map = cube_definition
            .and_then(|cube| cube.static_data().join_map.clone())
            .unwrap_or_default();
        if join_map.is_empty() {
            return Ok(JoinHints::new());
        }
        // A cube that heads one path but is reached from another one is not a
        // root of the view - the path it heads is just the tail of a longer walk.
        // Only the heads that nothing else reaches are candidates.
        let reached = join_map
            .iter()
            .flat_map(|path| path.iter().skip(1))
            .collect::<HashSet<_>>();
        let roots = join_map
            .iter()
            .filter_map(|path| path.first())
            .filter(|head| !reached.contains(*head))
            .unique()
            .collect_vec();

        let no_single_root = |detail: String| {
            CubeError::user(format!(
                "Can't resolve the cube to query for '{}': the member references no members of \
                 '{}', and {detail}",
                measure.full_name(),
                cube_name,
            ))
        };

        match roots.as_slice() {
            [root_cube] => Ok(JoinHints::from_items(vec![JoinHintItem::Single(
                (*root_cube).clone(),
            )])),
            // Every path of the join map is headed by a cube some other path
            // reaches, so the paths lead in a circle and none of them starts at
            // the view's root.
            [] => Err(no_single_root(format!(
                "the join paths of that view are cyclic: {}",
                join_map.iter().map(|path| path.join(".")).join(", ")
            ))),
            // The join map is ordered by the order the view lists its cubes, so
            // picking one root out of several would make the answer depend on
            // that order with nothing to hint at it.
            _ => Err(no_single_root(format!(
                "that view is built on cubes that don't share a single root: {}",
                roots.iter().join(", ")
            ))),
        }
    }

    pub fn measures_join_hints(&self) -> &MeasuresJoinHints {
        &self.measures_join_hints
    }

    /// True when the query's measures span more than one join tree.
    pub fn is_multi_fact(&self) -> bool {
        self.groups.len() > 1
    }

    /// True when any grouped measure — or a measure nested inside a
    /// composite one — sits on a cube that the join tree of its group
    /// multiplies (a one-to-many join below the measure). Descends the
    /// compiled measure tree so composite measures are covered.
    pub fn has_multiplied_measures(&self) -> Result<bool, CubeError> {
        for (join, measures) in self.groups.iter() {
            for measure in measures.iter() {
                if collect_multiplied_measures(measure, join)?
                    .iter()
                    .any(|item| item.multiplied)
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Full names of the (leaf) measures that are multiplied — i.e. sit below a
    /// one-to-many join — in these join groups. Like `has_multiplied_measures`,
    /// but returns the concrete measures so callers can compare the
    /// multiplicativity of a measure between two different groupings (e.g. the
    /// query vs a pre-aggregation).
    pub fn multiplied_measures(&self) -> Result<HashSet<String>, CubeError> {
        let mut result = HashSet::new();
        for (join, measures) in self.groups.iter() {
            for measure in measures.iter() {
                for item in collect_multiplied_measures(measure, join)? {
                    if item.multiplied {
                        result.insert(item.measure.full_name());
                    }
                }
            }
        }
        Ok(result)
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
    use crate::planner::{MemberExpressionExpression, MemberExpressionSymbol};
    use crate::test_fixtures::cube_bridge::{MockMemberSql, MockSchema};
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
    fn test_has_multiplied_measures_one_side_measure() {
        // `customers` is the `one` side of a one_to_many join to `orders`.
        // Selecting an `orders` dimension drags `orders` into the join, so
        // each customer row is multiplied and `customers.count` is multiplied.
        let schema = MockSchema::from_yaml_file("common/multi_fact.yaml");
        let ctx = TestContext::new(schema).unwrap();

        let customers_count = ctx.create_symbol("customers.count").unwrap();
        let orders_status = ctx.create_symbol("orders.status").unwrap();

        let hints = MeasuresJoinHints::builder(&JoinHints::new())
            .add_dimensions(&[orders_status])
            .build(&[customers_count])
            .unwrap();

        let groups = MultiFactJoinGroups::try_new(ctx.query_tools().clone(), hints).unwrap();

        assert!(groups.has_multiplied_measures().unwrap());
    }

    #[test]
    fn test_has_multiplied_measures_many_side_measure() {
        // `orders` is the `many` side, so joining the `customers` dimension
        // does not multiply order rows: `orders.count` is not multiplied.
        let schema = MockSchema::from_yaml_file("common/multi_fact.yaml");
        let ctx = TestContext::new(schema).unwrap();

        let orders_count = ctx.create_symbol("orders.count").unwrap();
        let customers_name = ctx.create_symbol("customers.name").unwrap();

        let hints = MeasuresJoinHints::builder(&JoinHints::new())
            .add_dimensions(&[customers_name])
            .build(&[orders_count])
            .unwrap();

        let groups = MultiFactJoinGroups::try_new(ctx.query_tools().clone(), hints).unwrap();

        assert!(!groups.has_multiplied_measures().unwrap());
    }

    #[test]
    fn test_multiplied_measures_one_side_measure() -> Result<(), CubeError> {
        // `customers` is the `one` side of a one_to_many join to `orders`.
        // Grouping `customers.count` by an `orders` dimension multiplies it,
        // so its full name is returned.
        let schema = MockSchema::from_yaml_file("common/multi_fact.yaml");
        let ctx = TestContext::new(schema)?;

        let customers_count = ctx.create_symbol("customers.count")?;
        let orders_status = ctx.create_symbol("orders.status")?;

        let hints = MeasuresJoinHints::builder(&JoinHints::new())
            .add_dimensions(&[orders_status])
            .build(&[customers_count])?;

        let groups = MultiFactJoinGroups::try_new(ctx.query_tools().clone(), hints)?;

        assert_eq!(
            groups.multiplied_measures()?,
            HashSet::from(["customers.count".to_string()])
        );
        Ok(())
    }

    #[test]
    fn test_multiplied_measures_many_side_measure() -> Result<(), CubeError> {
        // `orders` is the `many` side, so joining the `customers` dimension
        // does not multiply order rows: `orders.count` is not multiplied and
        // the set is empty.
        let schema = MockSchema::from_yaml_file("common/multi_fact.yaml");
        let ctx = TestContext::new(schema)?;

        let orders_count = ctx.create_symbol("orders.count")?;
        let customers_name = ctx.create_symbol("customers.name")?;

        let hints = MeasuresJoinHints::builder(&JoinHints::new())
            .add_dimensions(&[customers_name])
            .build(&[orders_count])?;

        let groups = MultiFactJoinGroups::try_new(ctx.query_tools().clone(), hints)?;

        assert!(groups.multiplied_measures()?.is_empty());
        Ok(())
    }

    #[test]
    fn test_multiplied_measures_multi_fact_mixed() -> Result<(), CubeError> {
        // Multi-fact query grouped by an `orders` dimension: `customers.count`
        // is multiplied (one side), `orders.count` is not (many side). Only the
        // multiplied measure is returned — discriminating per-measure where the
        // boolean `has_multiplied_measures()` cannot.
        let schema = MockSchema::from_yaml_file("common/multi_fact.yaml");
        let ctx = TestContext::new(schema)?;

        let customers_count = ctx.create_symbol("customers.count")?;
        let orders_count = ctx.create_symbol("orders.count")?;
        let orders_status = ctx.create_symbol("orders.status")?;

        let hints = MeasuresJoinHints::builder(&JoinHints::new())
            .add_dimensions(&[orders_status])
            .build(&[customers_count, orders_count])?;

        let groups = MultiFactJoinGroups::try_new(ctx.query_tools().clone(), hints)?;

        assert!(groups.has_multiplied_measures()?);
        assert_eq!(
            groups.multiplied_measures()?,
            HashSet::from(["customers.count".to_string()])
        );
        Ok(())
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

    /// Two measures of the same cube can still need different join trees, and
    /// then they are two groups like any other multi-fact pair - the owning cube
    /// says nothing about which joins to build.
    ///
    /// This is the precondition callers who slice by owning cube depend on, not
    /// a claim about what they do with it: what each planner emits per group is
    /// its own to cover.
    #[test]
    fn test_two_groups_for_measures_of_one_cube() {
        let schema = MockSchema::from_yaml_file("common/integration_calculated_multi_fact.yaml");
        let ctx = TestContext::new(schema).unwrap();

        let total_amount = ctx.create_symbol("payments.total_amount").unwrap();
        let converted_value = ctx.create_symbol("payments.converted_value").unwrap();
        let meta_value = ctx.create_symbol("payment_meta.value").unwrap();

        assert_eq!(total_amount.cube_name(), converted_value.cube_name());

        let hints = MeasuresJoinHints::builder(&JoinHints::new())
            .add_dimensions(&[meta_value])
            .build(&[total_amount.clone(), converted_value.clone()])
            .unwrap();

        let groups = MultiFactJoinGroups::try_new(ctx.query_tools().clone(), hints).unwrap();

        assert!(groups.is_multi_fact());
        assert_eq!(groups.num_groups(), 2);
        assert!(groups.single_join().is_err());

        let grouped = groups
            .groups()
            .iter()
            .map(|(_, measures)| measures.iter().map(|m| m.full_name()).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(
            grouped,
            vec![
                vec!["payments.total_amount"],
                vec!["payments.converted_value"]
            ]
        );
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

    fn nested_trees_context() -> TestContext {
        let schema = MockSchema::from_yaml_file("common/integration_nested_join_trees.yaml");
        TestContext::new(schema).unwrap()
    }

    fn nested_trees_groups(
        measure_paths: &[&str],
        merge_nested: bool,
    ) -> (usize, Vec<Vec<String>>) {
        let ctx = nested_trees_context();
        let measures = measure_paths
            .iter()
            .map(|path| ctx.create_symbol(path).unwrap())
            .collect_vec();

        nested_trees_groups_of(&ctx, measures, merge_nested)
    }

    fn nested_trees_groups_of(
        ctx: &TestContext,
        measures: Vec<Rc<MemberSymbol>>,
        merge_nested: bool,
    ) -> (usize, Vec<Vec<String>>) {
        let country = ctx.create_symbol("sites.country").unwrap();

        let hints = MeasuresJoinHints::builder(&JoinHints::new())
            .add_dimensions(&[country])
            .build(&measures)
            .unwrap();

        let groups = if merge_nested {
            MultiFactJoinGroups::try_new_merging_nested(ctx.query_tools().clone(), hints).unwrap()
        } else {
            MultiFactJoinGroups::try_new(ctx.query_tools().clone(), hints).unwrap()
        };

        let grouped_measures = groups
            .groups()
            .iter()
            .map(|(_, measures)| measures.iter().map(|m| m.full_name()).collect_vec())
            .collect_vec();
        (groups.num_groups(), grouped_measures)
    }

    #[test]
    fn test_nested_trees_distinct_measures_merge() {
        // `checkouts` is reached through `carts`, so the tree of a `carts`
        // measure is contained in the tree of a `checkouts` one. Both measures
        // are distinct counts, which the fan-out of the wider tree cannot
        // change, so one group answers both.
        let (num_groups, measures) =
            nested_trees_groups(&["carts.unique_msid", "checkouts.unique_msid"], true);

        assert_eq!(num_groups, 1);
        assert_eq!(
            measures,
            vec![vec![
                "checkouts.unique_msid".to_string(),
                "carts.unique_msid".to_string()
            ]]
        );
    }

    #[test]
    fn test_nested_trees_are_kept_apart_without_merging() {
        let (num_groups, _) =
            nested_trees_groups(&["carts.unique_msid", "checkouts.unique_msid"], false);

        assert_eq!(num_groups, 2);
    }

    #[test]
    fn test_nested_trees_plain_count_does_not_merge() {
        // The wider tree splits every `carts` row into one row per checkout, so
        // a plain count over it would answer the number of checkouts.
        let (num_groups, _) = nested_trees_groups(&["carts.count", "checkouts.unique_msid"], true);

        assert_eq!(num_groups, 2);
    }

    fn make_expression_measure(
        ctx: &TestContext,
        name: &str,
        cube_name: &str,
        sql: &str,
    ) -> Rc<MemberSymbol> {
        let member_sql = Rc::new(MockMemberSql::new(sql).unwrap());
        let mut compiler = ctx.query_tools().compiler().borrow_mut();
        let sql_call = compiler
            .compile_sql_call(&cube_name.to_string(), member_sql)
            .unwrap();
        let cube_symbol = compiler
            .add_cube_table_evaluator(cube_name.to_string(), vec![])
            .unwrap();
        drop(compiler);
        let symbol = MemberExpressionSymbol::try_new(
            cube_symbol,
            name.to_string(),
            MemberExpressionExpression::SqlCall(sql_call),
            None,
            None,
            vec![cube_name.to_string()],
        )
        .unwrap();
        MemberSymbol::new_member_expression(symbol)
    }

    #[test]
    fn test_nested_trees_expression_around_distinct_measure_does_not_merge() {
        // The `COUNT(*)` written in the expression is a member of nothing, so
        // the distinct count beside it is the only leaf there is to check - and
        // it says nothing about the count that would read the fanned-out rows.
        let ctx = nested_trees_context();
        let expression =
            make_expression_measure(&ctx, "net_carts", "carts", "COUNT(*) - {carts.unique_msid}");
        let checkouts_unique = ctx.create_symbol("checkouts.unique_msid").unwrap();

        let (num_groups, _) =
            nested_trees_groups_of(&ctx, vec![expression, checkouts_unique], true);

        assert_eq!(num_groups, 2);
    }

    #[test]
    fn test_nested_trees_sum_does_not_merge() {
        let (num_groups, _) =
            nested_trees_groups(&["carts.total_value", "checkouts.total_amount"], true);

        assert_eq!(num_groups, 2);
    }

    #[test]
    fn test_sibling_trees_do_not_merge() {
        // `orders` and `returns` hang off `customers` side by side, so neither
        // tree contains the other and there is no shared scan to fold into.
        let schema = MockSchema::from_yaml_file("common/multi_fact.yaml");
        let ctx = TestContext::new(schema).unwrap();

        let orders_count = ctx.create_symbol("orders.count").unwrap();
        let returns_count = ctx.create_symbol("returns.count").unwrap();
        let customers_name = ctx.create_symbol("customers.name").unwrap();

        let hints = MeasuresJoinHints::builder(&JoinHints::new())
            .add_dimensions(&[customers_name])
            .build(&[orders_count, returns_count])
            .unwrap();

        let groups =
            MultiFactJoinGroups::try_new_merging_nested(ctx.query_tools().clone(), hints).unwrap();

        assert_eq!(groups.num_groups(), 2);
    }
}
