use super::*;
use crate::planner::{InputId, MemberSymbol};
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// The distinct key rows of `sources`: the grid a `GridLookup` hands values onto.
#[derive(Clone, TypedBuilder)]
pub struct KeysGrid {
    schema: Rc<LogicalSchema>,
    sources: Vec<Rc<SubqueryRef>>,
}

impl KeysGrid {
    pub fn schema(&self) -> &Rc<LogicalSchema> {
        &self.schema
    }

    pub fn sources(&self) -> &Vec<Rc<SubqueryRef>> {
        &self.sources
    }
}

impl LogicalNode for KeysGrid {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::KeysGrid(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 0, self.node_name())?;
        Ok(self)
    }

    fn referenced_inputs(&self) -> Vec<InputId> {
        self.sources.iter().map(|r| r.id()).collect()
    }

    fn with_forwarded_input(
        self: Rc<Self>,
        from: InputId,
        to: &Rc<SubqueryRef>,
    ) -> Result<Rc<Self>, CubeError> {
        if !self.referenced_inputs().contains(&from) {
            return Ok(self);
        }
        Ok(Rc::new(Self {
            schema: self.schema.clone(),
            sources: forward_refs(&self.sources, from, to),
        }))
    }

    fn node_name(&self) -> &'static str {
        "KeysGrid"
    }

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::KeysGrid(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "KeysGrid"))
        }
    }
}

impl PrettyPrint for KeysGrid {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("KeysGrid: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        result.println("sources:", &state);
        for source in self.sources.iter() {
            source.pretty_print(result, &details_state);
        }
    }
}

/// A subquery joined onto the grid by the grid keys it carries.
#[derive(Clone)]
pub struct LookupLeg {
    subquery: Rc<SubqueryRef>,
    on: Vec<Rc<MemberSymbol>>,
}

impl LookupLeg {
    /// `subquery` joined by every key of `grid` its schema carries.
    pub fn new(subquery: Rc<SubqueryRef>, grid: &LogicalSchema) -> Self {
        let on = grid
            .all_dimensions()
            .filter(|dim| subquery.schema().has_dimension(dim))
            .cloned()
            .collect();
        Self { subquery, on }
    }

    pub fn subquery(&self) -> &Rc<SubqueryRef> {
        &self.subquery
    }

    pub fn on(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.on
    }
}

// The legs reading `to` instead of `from`, keeping their keys. Legs landing on
// one subquery become one leg joined by the keys of all of them.
fn forward_legs(legs: &[LookupLeg], from: InputId, to: &Rc<SubqueryRef>) -> Vec<LookupLeg> {
    let mut result: Vec<LookupLeg> = Vec::new();
    for leg in legs.iter() {
        let subquery = if leg.subquery.id() == from || leg.subquery.id() == to.id() {
            debug_assert!(leg.on.iter().all(|key| to.schema().has_dimension(key)));
            to.clone()
        } else {
            leg.subquery.clone()
        };
        match result.iter_mut().find(|r| r.subquery.id() == subquery.id()) {
            Some(existing) => {
                for key in leg.on.iter() {
                    if !existing.on.iter().any(|k| k.id() == key.id()) {
                        existing.on.push(key.clone());
                    }
                }
            }
            None => result.push(LookupLeg {
                subquery,
                on: leg.on.clone(),
            }),
        }
    }
    result
}

/// Values of `legs`, computed at coarser grains, handed onto every row of the
/// `grid`: the grid rows are kept and each leg is left-joined by its `on` keys.
#[derive(Clone, TypedBuilder)]
pub struct GridLookup {
    grid: Rc<SubqueryRef>,
    legs: Vec<LookupLeg>,
}

impl GridLookup {
    pub fn grid(&self) -> &Rc<SubqueryRef> {
        &self.grid
    }

    pub fn legs(&self) -> &Vec<LookupLeg> {
        &self.legs
    }
}

impl LogicalNode for GridLookup {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::GridLookup(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 0, self.node_name())?;
        Ok(self)
    }

    fn referenced_inputs(&self) -> Vec<InputId> {
        std::iter::once(self.grid.id())
            .chain(self.legs.iter().map(|leg| leg.subquery.id()))
            .collect()
    }

    fn with_forwarded_input(
        self: Rc<Self>,
        from: InputId,
        to: &Rc<SubqueryRef>,
    ) -> Result<Rc<Self>, CubeError> {
        if !self.referenced_inputs().contains(&from) {
            return Ok(self);
        }
        Ok(Rc::new(Self {
            grid: self.grid.forwarded(from, to),
            legs: forward_legs(&self.legs, from, to),
        }))
    }

    fn node_name(&self) -> &'static str {
        "GridLookup"
    }

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::GridLookup(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "GridLookup"))
        }
    }
}

impl PrettyPrint for GridLookup {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("GridLookup: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println("grid:", &state);
        self.grid.pretty_print(result, &details_state);
        result.println("legs:", &state);
        for leg in self.legs.iter() {
            leg.subquery.pretty_print(result, &details_state);
            result.println(
                &format!("on: {}", print_symbols(&leg.on)),
                &details_state.new_level(),
            );
        }
    }
}
