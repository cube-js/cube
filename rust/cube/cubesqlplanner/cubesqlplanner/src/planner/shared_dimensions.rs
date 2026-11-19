//! View dimensions shared through `join_paths`.
//!
//! A view can include one dimension cube through several join paths, one per
//! fact cube (`join_paths: [orders.event_day, refunds.event_day]`). Each of its
//! dimensions is a reference that may stand for the member along any of those
//! paths. Before a query is planned, every measure picks the path its own fact
//! joins through, and the query is rewritten so that no shared dimension is
//! left: with one choice for the whole query its dimensions are bound in place,
//! otherwise it is split into one leg per choice, merged on the dimensions like
//! any multi-fact query.

use crate::planner::collectors::collect_join_hints;
use crate::planner::join_hints::{JoinHint, JoinHints};
use crate::planner::planners::JoinTreeBuilder;
use crate::planner::symbols::transforms::substitute_by_name;
use crate::planner::{
    CubeId, MemberId, MemberSymbol, QueryProperties, RefSymbol, TraversalVisitor,
};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

/// How a query reads the shared view dimensions it uses.
pub enum SharedDimensionBinding {
    /// The query uses no shared dimension.
    None,
    /// Every measure reads each shared dimension through the same path: the
    /// query with those paths bound.
    Single(Rc<QueryProperties>),
    /// Measures read a shared dimension through different paths: one leg per
    /// choice of paths, each with the measures making it and without the
    /// query's measure filters, order and limit, which apply over the merge.
    Split(Vec<Rc<QueryProperties>>),
}

/// Binds the shared view dimensions `query` uses to the paths its measures
/// join through.
pub fn bind_shared_dimensions(
    query: &Rc<QueryProperties>,
) -> Result<SharedDimensionBinding, CubeError> {
    let shared = collect_shared_dimensions(&query.all_used_symbols()?)?;
    if shared.is_empty() {
        return Ok(SharedDimensionBinding::None);
    }

    let base_hints = base_hints(query)?;
    let base_roots = hint_roots(&base_hints);

    // Measures choosing the same paths share a leg, in order of appearance.
    let mut legs: Vec<(Vec<usize>, Vec<Rc<MemberSymbol>>)> = Vec::new();
    for measure in query.all_used_measures()? {
        let hints = collect_join_hints(&measure)?;
        let choice = shared
            .iter()
            .map(|dimension| choose_path(query, dimension, &measure, &hints, &base_hints))
            .collect::<Result<Vec<_>, _>>()?;
        match legs
            .iter_mut()
            .find(|(leg_choice, _)| *leg_choice == choice)
        {
            Some((_, measures)) => measures.push(measure),
            None => legs.push((choice, vec![measure])),
        }
    }

    match legs.len() {
        0 => {
            let choice = shared
                .iter()
                .map(|dimension| default_path(dimension, &base_roots))
                .collect_vec();
            Ok(SharedDimensionBinding::Single(bind(
                query, &shared, &choice,
            )?))
        }
        1 => Ok(SharedDimensionBinding::Single(bind(
            query, &shared, &legs[0].0,
        )?)),
        _ => {
            if query.ungrouped() {
                return Err(CubeError::user(format!(
                    "An ungrouped query can't read {} through different join paths for different measures",
                    shared.iter().map(|d| format!("'{}'", d.full_name())).join(", ")
                )));
            }
            let legs = legs
                .iter()
                .map(|(choice, measures)| {
                    let substitutions = substitutions(&shared, choice)?;
                    let bound = bind_with(query, &substitutions)?;
                    let measures = measures
                        .iter()
                        .map(|m| substitute_by_name(m, &substitutions))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(bound.as_merge_leg(measures))
                })
                .collect::<Result<Vec<_>, CubeError>>()?;
            Ok(SharedDimensionBinding::Split(legs))
        }
    }
}

/// Whether any member `symbols` reach is a shared view dimension.
pub fn has_shared_dimensions(symbols: &[Rc<MemberSymbol>]) -> Result<bool, CubeError> {
    Ok(!collect_shared_dimensions(symbols)?.is_empty())
}

struct SharedDimensionsCollector {
    found: Vec<Rc<RefSymbol>>,
}

impl TraversalVisitor for SharedDimensionsCollector {
    type State = ();
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        _: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        // A time dimension's dependencies look through its base symbol, so the
        // reference it wraps is only seen here.
        if let MemberSymbol::TimeDimension(time_dimension) = node.as_ref() {
            return self.on_node_traverse(time_dimension.base_symbol(), &());
        }
        if let MemberSymbol::Ref(reference) = node.as_ref() {
            if reference.is_shared() {
                if !self.found.iter().any(|f| f.id() == reference.id()) {
                    self.found.push(reference.clone());
                }
                return Ok(None);
            }
        }
        Ok(Some(()))
    }
}

fn collect_shared_dimensions(
    symbols: &[Rc<MemberSymbol>],
) -> Result<Vec<Rc<RefSymbol>>, CubeError> {
    let mut collector = SharedDimensionsCollector { found: Vec::new() };
    for symbol in symbols {
        collector.apply(symbol, &())?;
    }
    Ok(collector.found)
}

/// The hints every measure of the query shares: the query's own and those of
/// its dimensions and filters. Shared dimensions add none (see
/// `JoinHintsCollector`).
fn base_hints(query: &QueryProperties) -> Result<JoinHints, CubeError> {
    let mut hints = (**query.query_join_hints()).clone();
    let mut members = query.dimensions().clone();
    members.extend(query.time_dimensions().iter().cloned());
    members.extend(query.extract_dimensions_from_order());
    for item in query
        .dimensions_filters()
        .iter()
        .chain(query.time_dimensions_filters().iter())
        .chain(query.segments().iter())
    {
        item.find_all_member_evaluators(&mut members);
    }
    for member in members.iter() {
        hints.extend(&collect_join_hints(member)?);
    }
    Ok(hints)
}

/// The cubes the joins of `hints` start from.
fn hint_roots(hints: &JoinHints) -> Vec<String> {
    hints
        .iter()
        .filter_map(|hint| hint.cubes().first())
        .map(|cube| cube.root().target().to_string())
        .unique()
        .collect()
}

/// The path of `dimension` a query without measures reads: the first one
/// starting at a cube the query already joins, else the first one.
fn default_path(dimension: &RefSymbol, base_roots: &[String]) -> usize {
    dimension
        .shared_targets()
        .unwrap_or_default()
        .iter()
        .position(|target| base_roots.contains(&target.root))
        .unwrap_or(0)
}

/// The path of `dimension` that `measure` reads: the first one starting at a
/// cube the measure's own joins start from, else the first one starting at a
/// cube the measure's fact joins to.
fn choose_path(
    query: &QueryProperties,
    dimension: &RefSymbol,
    measure: &Rc<MemberSymbol>,
    measure_hints: &JoinHints,
    base_hints: &JoinHints,
) -> Result<usize, CubeError> {
    let targets = dimension.shared_targets().unwrap_or_default();
    let measure_roots = hint_roots(measure_hints);
    if measure_roots.is_empty() {
        return Ok(default_path(dimension, &hint_roots(base_hints)));
    }
    if let Some(index) = targets
        .iter()
        .position(|target| measure_roots.contains(&target.root))
    {
        return Ok(index);
    }

    // The measure's fact may reach a path's start through its own joins
    // (`order_items` to `orders`): the path is read over the measure's tree
    // when joining its start keeps that tree rooted at the measure's fact.
    let tree_builder = JoinTreeBuilder::new(query.query_tools().clone());
    for (index, target) in targets.iter().enumerate() {
        let mut hints = base_hints.clone();
        hints.extend(measure_hints);
        hints.push(JoinHint::Single(CubeId::cube(target.root.clone())));
        let tree = tree_builder.try_build_for_hints(&hints).ok().flatten();
        if let Some((_, tree)) = tree {
            if measure_roots.contains(&tree.root().cube_id().target().to_string()) {
                return Ok(index);
            }
        }
    }

    Err(CubeError::user(format!(
        "Can't read '{}' for '{}': none of its join_paths ({}) starts at a cube '{}' is read over. Add a path starting at '{}' to join_paths",
        dimension.full_name(),
        measure.full_name(),
        targets.iter().map(|t| t.path.clone()).join(", "),
        measure.full_name(),
        measure_roots[0],
    )))
}

fn substitutions(
    shared: &[Rc<RefSymbol>],
    choice: &[usize],
) -> Result<HashMap<MemberId, Rc<MemberSymbol>>, CubeError> {
    shared
        .iter()
        .zip(choice.iter())
        .map(|(dimension, index)| {
            Ok((
                dimension.id().clone(),
                MemberSymbol::new_ref(dimension.bind_shared(*index)?),
            ))
        })
        .collect()
}

fn bind(
    query: &Rc<QueryProperties>,
    shared: &[Rc<RefSymbol>],
    choice: &[usize],
) -> Result<Rc<QueryProperties>, CubeError> {
    bind_with(query, &substitutions(shared, choice)?)
}

fn bind_with(
    query: &Rc<QueryProperties>,
    substitutions: &HashMap<MemberId, Rc<MemberSymbol>>,
) -> Result<Rc<QueryProperties>, CubeError> {
    query.map_symbols(&|symbol| substitute_by_name(symbol, substitutions))
}
