use crate::planner::collectors::{collect_join_hints, LeafCubes, MeasureLeaf};
use crate::planner::join_hints::JoinHints;
use crate::planner::multi_fact_join_groups::{MeasuresJoinHints, MultiFactJoinGroups};
use crate::planner::symbol_classification::{MeasureForm, SymbolClassification};
use crate::planner::symbols::{deps, transforms};
use crate::planner::{CubeId, JoinTree, MemberId, MemberSymbol};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

/// Measures aggregated directly over one join.
pub struct RegularGroup {
    join: Rc<JoinTree>,
    measures: Vec<Rc<MemberSymbol>>,
}

impl RegularGroup {
    pub fn join(&self) -> &Rc<JoinTree> {
        &self.join
    }

    pub fn measures(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.measures
    }
}

/// Measures aggregated once per primary key of `key_cube`, whose rows `join`
/// multiplies or leaves out.
pub struct MultipliedGroup {
    key_cube: CubeId,
    join: Rc<JoinTree>,
    measures: Vec<Rc<MemberSymbol>>,
}

impl MultipliedGroup {
    pub fn key_cube(&self) -> &CubeId {
        &self.key_cube
    }

    pub fn join(&self) -> &Rc<JoinTree> {
        &self.join
    }

    pub fn measures(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.measures
    }
}

/// The groups a query aggregates its leaves in, each planned as a subquery of
/// its own, and how each measure reads them back.
#[derive(Default)]
pub struct MeasureGroups {
    regular: Vec<RegularGroup>,
    multiplied: Vec<MultipliedGroup>,
    // Per aggregated measure: its leaves as the groups compute them.
    render_forms: HashMap<MemberId, HashMap<MemberId, Rc<MemberSymbol>>>,
}

// One leaf as `measure` reaches it, joined by its own hints and `path_hints`.
struct LeafUse {
    measure: Rc<MemberSymbol>,
    leaf: MeasureLeaf,
    path_hints: JoinHints,
    per_row: bool,
}

/// How leaves are grouped by their joins.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Grouping {
    /// One group per distinct join.
    ByJoin,
    /// Nested groups folded together the way the query's own are.
    MergingNested,
}

impl MeasureGroups {
    pub(crate) fn try_new(
        classification: &SymbolClassification,
        hints: &MeasuresJoinHints,
        group: impl Fn(MeasuresJoinHints, Grouping) -> Result<MultiFactJoinGroups, CubeError>,
    ) -> Result<Self, CubeError> {
        let mut uses = Self::leaf_uses(classification)?;
        if uses.is_empty() {
            return Ok(Self::default());
        }
        // A composite measure stays whole, over its own join, when each of its
        // leaves aggregates there directly, as it is or in its distinct form.
        // Only otherwise is it split into leaves, each over a join of its own.
        let mut whole: HashMap<MemberId, JoinHints> = HashMap::new();
        for measure in uses
            .iter()
            .filter(|u| u.leaf.measure.id() != u.measure.id())
            .map(|u| u.measure.clone())
            .unique_by(|m| m.id().clone())
            .collect_vec()
        {
            let own = collect_join_hints(&measure)?;
            let fits = uses
                .iter()
                .filter(|u| u.measure.id() == measure.id())
                .all(|u| {
                    Self::leaf_join(u, &own, hints, &group).is_some_and(|join| {
                        Self::place_leaf(&u.leaf.measure, u, &join)
                            .is_ok_and(|(_, key_cube)| key_cube.is_none())
                    })
                });
            if fits {
                whole.insert(measure.id().clone(), own);
            }
        }
        for leaf_use in uses.iter_mut() {
            if let Some(own) = whole.get(leaf_use.measure.id()) {
                leaf_use.path_hints = own.clone();
            } else if !leaf_use.path_hints.is_empty()
                && !Self::keeps_path_hints(leaf_use, hints, &group)
            {
                leaf_use.path_hints = JoinHints::new();
            }
        }

        // A leaf reached by different hints may land in different joins, so each
        // distinct set of hints gets a symbol of its own. The leaf's own hints
        // count too: a view reaches its cube's measure along a path of its own.
        // A measure selected as it is keeps its own symbol, and so its column.
        let identities = uses
            .iter()
            .map(|u| {
                let mut hints = collect_join_hints(&u.leaf.measure)?;
                hints.extend(&u.path_hints);
                Ok(hints)
            })
            .collect::<Result<Vec<_>, CubeError>>()?;
        let mut variants: HashMap<MemberId, Vec<(JoinHints, Rc<MemberSymbol>)>> = HashMap::new();
        for (leaf_use, identity) in uses.iter().zip(identities.iter()) {
            if leaf_use.leaf.measure.id() == leaf_use.measure.id() {
                variants
                    .entry(leaf_use.measure.id().clone())
                    .or_default()
                    .push((identity.clone(), leaf_use.leaf.measure.clone()));
            }
        }
        let mut symbols = Vec::with_capacity(uses.len());
        for (leaf_use, identity) in uses.iter().zip(identities.iter()) {
            let base = &leaf_use.leaf.measure;
            let known = variants.entry(base.id().clone()).or_default();
            let symbol = match known.iter().find(|(hints, _)| hints == identity) {
                Some((_, symbol)) => symbol.clone(),
                None => {
                    let symbol = if known.is_empty() {
                        base.clone()
                    } else {
                        transforms::in_leg(base, known.len())
                    };
                    known.push((identity.clone(), symbol.clone()));
                    symbol
                }
            };
            symbols.push(symbol);
        }
        let leaves = symbols
            .iter()
            .zip(uses.iter())
            .map(|(symbol, leaf_use)| {
                (
                    symbol.clone(),
                    leaf_use.path_hints.clone(),
                    leaf_use.measure.clone(),
                )
            })
            .unique_by(|(symbol, _, _)| symbol.id().clone())
            .collect_vec();
        let leaf_hints = hints.for_leaves(&leaves)?;
        // A leaf is judged in its own join, then grouped only with leaves
        // judged alike: a wider join one of them needs is no reason for another.
        let own_groups = group(leaf_hints.clone(), Grouping::ByJoin)?;
        let own_join: HashMap<MemberId, Rc<JoinTree>> = own_groups
            .groups()
            .iter()
            .flat_map(|(join, measures)| {
                measures.iter().map(move |m| (m.id().clone(), join.clone()))
            })
            .collect();

        let mut forms: HashMap<MemberId, Rc<MemberSymbol>> = HashMap::new();
        let mut regular_forms = Vec::new();
        let mut multiplied_forms: HashMap<CubeId, Vec<Rc<MemberSymbol>>> = HashMap::new();
        for (symbol, leaf_use) in symbols.iter().zip(uses.iter()) {
            if forms.contains_key(symbol.id()) {
                continue;
            }
            let join = own_join.get(symbol.id()).ok_or_else(|| {
                CubeError::internal(format!(
                    "Leaf {} has no join of its own",
                    symbol.full_name()
                ))
            })?;
            let (form, key_cube) = Self::place_leaf(symbol, leaf_use, join)?;
            match key_cube {
                None => regular_forms.push(form.clone()),
                Some(cube) => multiplied_forms.entry(cube).or_default().push(form.clone()),
            }
            forms.insert(symbol.id().clone(), form);
        }

        // Columns keep the order their measures are selected in.
        let mut position: HashMap<MemberId, usize> = HashMap::new();
        for symbol in symbols.iter() {
            let next = position.len();
            position.entry(symbol.id().clone()).or_insert(next);
        }
        let mut canonical: HashMap<MemberId, Rc<MemberSymbol>> = HashMap::new();
        let mut regular = Vec::new();
        if !regular_forms.is_empty() {
            let grouping = if multiplied_forms.is_empty() {
                Grouping::MergingNested
            } else {
                Grouping::ByJoin
            };
            let groups = group(leaf_hints.for_forms(&regular_forms)?, grouping)?;
            for (join, measures) in groups.groups().iter() {
                regular.push(RegularGroup {
                    join: join.clone(),
                    measures: Self::collapse_variants(measures, &position, &mut canonical),
                });
            }
        }
        let mut multiplied = Vec::new();
        // Sorted so that subquery numbering stays deterministic across runs.
        for (key_cube, measures) in multiplied_forms
            .into_iter()
            .sorted_by(|(a, _), (b, _)| a.cmp(b))
        {
            let groups = group(leaf_hints.for_forms(&measures)?, Grouping::ByJoin)?;
            for (join, measures) in groups.groups().iter() {
                multiplied.push(MultipliedGroup {
                    key_cube: key_cube.clone(),
                    join: join.clone(),
                    measures: Self::collapse_variants(measures, &position, &mut canonical),
                });
            }
        }

        let mut render_forms: HashMap<MemberId, HashMap<MemberId, Rc<MemberSymbol>>> =
            HashMap::new();
        // Substitution is by id, so a measure can read each leaf in one form only.
        let mut read_as: HashMap<(MemberId, MemberId), MemberId> = HashMap::new();
        for (symbol, leaf_use) in symbols.iter().zip(uses.iter()) {
            let form = canonical.get(symbol.id()).cloned().ok_or_else(|| {
                CubeError::internal(format!(
                    "Leaf {} of {} is in no join group",
                    symbol.full_name(),
                    leaf_use.measure.full_name()
                ))
            })?;
            let leaf_id = leaf_use.leaf.measure.id().clone();
            let read = read_as
                .entry((leaf_use.measure.id().clone(), leaf_id.clone()))
                .or_insert_with(|| form.id().clone());
            if *read != *form.id() {
                return Err(CubeError::user(format!(
                    "Measure {} reads {} over two different joins, which is not supported",
                    leaf_use.measure.full_name(),
                    leaf_use.leaf.measure.full_name()
                )));
            }
            if !Rc::ptr_eq(&form, &leaf_use.leaf.measure) {
                render_forms
                    .entry(leaf_use.measure.id().clone())
                    .or_default()
                    .insert(leaf_id, form);
            }
        }

        Ok(Self {
            regular,
            multiplied,
            render_forms,
        })
    }

    // Variants of one leaf in one group are one column: the original symbol
    // where it is among them, else the first. Records the column of each.
    fn collapse_variants(
        measures: &[Rc<MemberSymbol>],
        position: &HashMap<MemberId, usize>,
        canonical: &mut HashMap<MemberId, Rc<MemberSymbol>>,
    ) -> Vec<Rc<MemberSymbol>> {
        let mut chosen: HashMap<MemberId, Rc<MemberSymbol>> = HashMap::new();
        for symbol in measures {
            let is_original = symbol.id().base() == symbol.id();
            chosen
                .entry(symbol.id().base().clone())
                .and_modify(|current| {
                    if is_original {
                        *current = symbol.clone();
                    }
                })
                .or_insert_with(|| symbol.clone());
        }
        for symbol in measures {
            canonical.insert(symbol.id().clone(), chosen[symbol.id().base()].clone());
        }
        measures
            .iter()
            .filter(|m| Rc::ptr_eq(m, &chosen[m.id().base()]))
            .sorted_by_key(|m| position.get(m.id()))
            .cloned()
            .collect()
    }

    // Every leaf of every aggregated measure, joined by the cubes of the
    // measures it is reached through.
    fn leaf_uses(classification: &SymbolClassification) -> Result<Vec<LeafUse>, CubeError> {
        let mut uses = Vec::new();
        for (measure, form) in classification.aggregated() {
            match form {
                MeasureForm::PerRow => uses.push(LeafUse {
                    measure: measure.clone(),
                    leaf: MeasureLeaf {
                        measure: measure.clone(),
                        cubes: LeafCubes::DimensionOnly(vec![]),
                        path: vec![],
                    },
                    path_hints: JoinHints::new(),
                    per_row: true,
                }),
                MeasureForm::Leaves(leaves) => {
                    for leaf in leaves {
                        let mut path_hints = JoinHints::new();
                        for step in leaf.path.iter() {
                            path_hints.extend(&Self::own_cube_hint(step)?);
                        }
                        uses.push(LeafUse {
                            measure: measure.clone(),
                            leaf: leaf.clone(),
                            path_hints,
                            per_row: false,
                        });
                    }
                }
            }
        }
        Ok(uses)
    }

    // Path hints matter where they move the rows the leaf reads: the root, or
    // the path to a cube of its own join. Elsewhere they are kept only while the
    // leaf stays regular in the wider join, where it can share its siblings' scan.
    fn keeps_path_hints(
        leaf_use: &LeafUse,
        hints: &MeasuresJoinHints,
        group: &impl Fn(MeasuresJoinHints, Grouping) -> Result<MultiFactJoinGroups, CubeError>,
    ) -> bool {
        let (Some(with), Some(without)) = (
            Self::leaf_join(leaf_use, &leaf_use.path_hints, hints, group),
            Self::leaf_join(leaf_use, &JoinHints::new(), hints, group),
        ) else {
            return true;
        };
        if with.root().cube_id() != without.root().cube_id() {
            return true;
        }
        let with_paths = with.cube_paths();
        let moves_rows = without
            .cube_paths()
            .iter()
            .any(|(cube, path)| with_paths.get(cube) != Some(path));
        moves_rows
            || Self::place_leaf(&leaf_use.leaf.measure, leaf_use, &with)
                .map_or(true, |(_, key_cube)| key_cube.is_none())
    }

    // The join of the leaf joined by `extra` as well, or none where it has none.
    fn leaf_join(
        leaf_use: &LeafUse,
        extra: &JoinHints,
        hints: &MeasuresJoinHints,
        group: &impl Fn(MeasuresJoinHints, Grouping) -> Result<MultiFactJoinGroups, CubeError>,
    ) -> Option<Rc<JoinTree>> {
        let leaf = (
            leaf_use.leaf.measure.clone(),
            extra.clone(),
            leaf_use.measure.clone(),
        );
        let groups = group(hints.for_leaves(&[leaf]).ok()?, Grouping::ByJoin).ok()?;
        groups.groups().first().map(|(join, _)| join.clone())
    }

    // The cube of a measure on a leaf's path: the first hint its own walk
    // yields. A view is joined through the paths its members carry.
    fn own_cube_hint(measure: &Rc<MemberSymbol>) -> Result<JoinHints, CubeError> {
        if measure.as_measure().map_or(true, |m| m.is_view()) {
            return Ok(JoinHints::new());
        }
        let own = collect_join_hints(measure)?;
        Ok(JoinHints::from_items(own.iter().take(1).cloned().collect()))
    }

    // The form the leaf's own `join` calls for, and the cube whose keys it is
    // aggregated per when that join multiplies it.
    fn place_leaf(
        symbol: &Rc<MemberSymbol>,
        leaf_use: &LeafUse,
        join: &Rc<JoinTree>,
    ) -> Result<(Rc<MemberSymbol>, Option<CubeId>), CubeError> {
        if leaf_use.per_row {
            return Ok((symbol.clone(), None));
        }
        let leaf = MeasureLeaf {
            measure: symbol.clone(),
            ..leaf_use.leaf.clone()
        };
        let item = leaf.resolve(join)?;
        let foreign = Self::reads_rows_its_cube_lacks(symbol, join)?;
        if !item.multiplied && !foreign {
            return Ok((symbol.clone(), None));
        }
        // An expression over dimensions has no other form: it moves to a
        // multiplied subquery as it is.
        let Ok(measure) = symbol.as_measure() else {
            return Ok((symbol.clone(), Some(item.cube_id)));
        };
        if let Some(regular_form) = transforms::regular_in_multiplied(&measure).filter(|_| !foreign)
        {
            return Ok((regular_form, None));
        }
        Ok((transforms::into_multiplied(&measure), Some(item.cube_id)))
    }

    // A measure aggregating another cube's columns, over a join not rooted at
    // its own cube, would also aggregate the rows its cube has none of.
    fn reads_rows_its_cube_lacks(
        symbol: &Rc<MemberSymbol>,
        join: &Rc<JoinTree>,
    ) -> Result<bool, CubeError> {
        // A view has no rows of its own to lack.
        if symbol.as_measure().map_or(true, |m| m.is_view()) {
            return Ok(false);
        }
        let own = symbol.cube_id();
        if *join.root().cube_id() == own {
            return Ok(false);
        }
        Ok(Self::aggregates_other_cube(symbol, &own))
    }

    // Filters are left out: a row its cube lacks fails them or adds nothing.
    fn aggregates_other_cube(node: &Rc<MemberSymbol>, own: &CubeId) -> bool {
        let deps = match node.as_ref() {
            MemberSymbol::Measure(measure) => deps::collect_deps(measure.kind()),
            MemberSymbol::Dimension(_) | MemberSymbol::TimeDimension(_) => {
                return node.clone().resolve_reference_chain().cube_id() != *own;
            }
            MemberSymbol::MemberExpression(_) | MemberSymbol::Ref(_) => node.get_dependencies(),
        };
        deps.iter().any(|dep| Self::aggregates_other_cube(dep, own))
    }

    pub fn regular(&self) -> &[RegularGroup] {
        &self.regular
    }

    pub fn multiplied(&self) -> &[MultipliedGroup] {
        &self.multiplied
    }

    /// Whether every leaf is aggregated directly over one and the same join.
    pub fn is_single_regular_join(&self) -> bool {
        self.multiplied.is_empty() && self.regular.len() <= 1
    }

    /// `measure` reading its leaves in the forms and columns the groups
    /// compute them in.
    pub fn render(&self, measure: &Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError> {
        match self.render_forms.get(measure.id()) {
            Some(forms) => transforms::substitute_by_name(measure, forms),
            None => Ok(measure.clone()),
        }
    }
}
