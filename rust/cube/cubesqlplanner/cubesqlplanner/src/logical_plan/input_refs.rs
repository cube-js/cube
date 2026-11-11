use super::*;
use crate::planner::collectors::rendered_dependencies;
use crate::planner::query_properties::OrderByItem;
use crate::planner::{InputId, MemberId, MemberSymbol, RefTarget};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

// Where the keys of a reading node come from.
enum KeySource {
    // Merged from these subqueries: the same dimension of each.
    Merge(Vec<InputId>),
    // Read from this subquery, the grid every row comes from.
    Grid(InputId),
}

/// Where a node reading subqueries reads its members from, as planned: its keys
/// from where the rows come from, every other member a subquery outputs from
/// that subquery, and anything else computed over those.
pub struct InputPlacement {
    keys: HashMap<MemberId, KeySource>,
    members: HashMap<MemberId, InputId>,
}

impl InputPlacement {
    /// Reading `legs` merged by `keys`.
    pub fn over_merge(legs: &[Rc<SubqueryRef>], keys: &LogicalSchema) -> Self {
        let inputs = legs.iter().map(|leg| leg.id()).unique().collect_vec();
        let keys = keys
            .all_dimensions()
            .map(|key| (chain_id(key), KeySource::Merge(inputs.clone())))
            .collect();
        Self::with_members(keys, legs)
    }

    /// Reading the rows of `grid` with `legs` looked up onto them.
    pub fn over_lookup(lookup: &GridLookup) -> Self {
        let grid = lookup.grid();
        let keys = grid
            .schema()
            .all_dimensions()
            .map(|key| (chain_id(key), KeySource::Grid(grid.id())))
            .collect();
        let legs = lookup
            .legs()
            .iter()
            .map(|leg| leg.subquery().clone())
            .collect_vec();
        Self::with_members(keys, &legs)
    }

    fn with_members(keys: HashMap<MemberId, KeySource>, legs: &[Rc<SubqueryRef>]) -> Self {
        let mut members = HashMap::new();
        for leg in legs.iter() {
            for member in leg.schema().all_members() {
                let id = chain_id(member);
                if !keys.contains_key(&id) {
                    members.entry(id).or_insert(leg.id());
                }
            }
        }
        Self { keys, members }
    }

    pub fn place(&self, symbol: &Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError> {
        symbol.apply_recursive(&|node: &Rc<MemberSymbol>| {
            if node.is_terminal_ref() {
                return Ok(node.clone());
            }
            let id = chain_id(node);
            if let Some(input) = self.members.get(&id) {
                return Ok(MemberSymbol::new_input_ref(node, *input));
            }
            Ok(match self.keys.get(&id) {
                Some(KeySource::Merge(inputs)) => MemberSymbol::new_key_ref(node, inputs.clone()),
                Some(KeySource::Grid(grid)) => MemberSymbol::new_input_ref(node, *grid),
                None => node.clone(),
            })
        })
    }
}

fn chain_id(member: &Rc<MemberSymbol>) -> MemberId {
    member.clone().resolve_reference_chain().id().clone()
}

pub(super) fn map_order_by(
    items: &[OrderByItem],
    f: &impl Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>,
) -> Result<Vec<OrderByItem>, CubeError> {
    items
        .iter()
        .map(|item| Ok(OrderByItem::new(f(&item.member_symbol())?, item.desc())))
        .collect()
}

// `symbol` reading `to` wherever it read the input `from`. Fails if `to` does
// not carry a member read from `from`.
fn forward_symbol_input_refs(
    symbol: &Rc<MemberSymbol>,
    from: InputId,
    to: &Rc<SubqueryRef>,
) -> Result<Rc<MemberSymbol>, CubeError> {
    symbol.apply_recursive(&|node: &Rc<MemberSymbol>| {
        let MemberSymbol::Ref(reference) = node.as_ref() else {
            return Ok(node.clone());
        };
        let inputs = reference.inputs();
        let Some(origin) = node.input_origin().filter(|_| inputs.contains(&from)) else {
            return Ok(node.clone());
        };
        if !to.schema().has_member(origin) {
            return Err(CubeError::internal(format!(
                "Input {} forwarded to {} ({}) has no {}",
                from,
                to.id(),
                to.name(),
                origin.full_name()
            )));
        }
        Ok(match reference.target() {
            RefTarget::Key { .. } => {
                let inputs = inputs
                    .iter()
                    .map(|input| if *input == from { to.id() } else { *input })
                    .unique()
                    .collect();
                MemberSymbol::new_key_ref(origin, inputs)
            }
            _ => MemberSymbol::new_input_ref(origin, to.id()),
        })
    })
}

/// A node reading members from subqueries through references to them.
pub trait InputReader: LogicalNode + Sized {
    /// The subqueries its references may read; empty when it reads none.
    fn read_inputs(&self) -> Vec<InputId>;

    fn map_symbols(
        &self,
        f: &dyn Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>,
    ) -> Result<Self, CubeError>;

    /// The node reading its members where `placement` puts them.
    fn with_input_refs(&self, placement: &InputPlacement) -> Result<Self, CubeError> {
        self.map_symbols(&|s| placement.place(s))
    }

    /// Visits the symbols its SQL renders.
    fn visit_rendered_symbols(
        &self,
        f: &mut dyn FnMut(&Rc<MemberSymbol>) -> Result<(), CubeError>,
    ) -> Result<(), CubeError>;

    /// The node reading `to` wherever it read the input `from`.
    fn forward_input_refs(
        self: Rc<Self>,
        from: InputId,
        to: &Rc<SubqueryRef>,
    ) -> Result<Rc<Self>, CubeError> {
        if !self.read_inputs().contains(&from) {
            return Ok(self);
        }
        Ok(Rc::new(self.map_symbols(&|s| {
            forward_symbol_input_refs(s, from, to)
        })?))
    }

    /// Fails unless every rendered symbol reads its members through references
    /// to the node's inputs. `is_masked` tells which members render their mask.
    fn check_input_refs(
        &self,
        is_masked: &dyn Fn(&Rc<MemberSymbol>) -> bool,
    ) -> Result<(), CubeError> {
        let inputs = self.read_inputs();
        if inputs.is_empty() {
            return Ok(());
        }
        self.visit_rendered_symbols(&mut |s| {
            check_symbol_input_refs(s, &inputs, self.node_name(), is_masked)
        })
    }
}

// The ordering items rendered by their own symbol: a projected item is sorted by
// the projected member, and one neither projected nor a measure is not sorted by.
pub(super) fn visit_rendered_order_by(
    items: &[OrderByItem],
    schema: &LogicalSchema,
    f: &mut dyn FnMut(&Rc<MemberSymbol>) -> Result<(), CubeError>,
) -> Result<(), CubeError> {
    items
        .iter()
        .map(|item| item.member_symbol())
        .filter(|member| {
            schema.find_member_positions(member.id()).is_empty()
                && member.input_origin().unwrap_or(member).is_measure()
        })
        .try_for_each(|member| f(&member))
}

// Every input reference of `symbol` reads one of `inputs`, and no member its SQL
// renders is read from a cube directly.
fn check_symbol_input_refs(
    symbol: &Rc<MemberSymbol>,
    inputs: &[InputId],
    node: &str,
    is_masked: &dyn Fn(&Rc<MemberSymbol>) -> bool,
) -> Result<(), CubeError> {
    if let MemberSymbol::Ref(reference) = symbol.as_ref() {
        let read = reference.inputs();
        if let Some(input) = read.iter().find(|input| !inputs.contains(input)) {
            return Err(CubeError::internal(format!(
                "{} reads {} from input {} it does not read",
                node,
                symbol.full_name(),
                input
            )));
        }
        if !read.is_empty() {
            return Ok(());
        }
        if reference.is_terminal() {
            return Ok(());
        }
    }
    if symbol.owned_by_cube() {
        return Err(CubeError::internal(format!(
            "{} reads {} from a cube, but reads only its inputs",
            node,
            symbol.full_name()
        )));
    }
    rendered_dependencies(symbol, is_masked)
        .iter()
        .try_for_each(|dep| check_symbol_input_refs(dep, inputs, node, is_masked))
}
