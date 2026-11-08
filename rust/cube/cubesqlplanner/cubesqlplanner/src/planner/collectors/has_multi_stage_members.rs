use crate::planner::{MemberSymbol, TraversalVisitor};
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

/// Where a symbol stops being computed by the query that selects it.
#[derive(Clone, Debug, PartialEq)]
pub enum BoundaryKind {
    MultiStage,
    /// A rolling window outside `multi_stage`.
    Cumulative,
    SubQueryDimension,
}

#[derive(Clone, Debug)]
pub struct Boundary {
    pub kind: BoundaryKind,
    pub symbol: Rc<MemberSymbol>,
}

/// Whether any of the boundaries puts part of the symbol into a multi-stage stage.
pub fn crosses_multi_stage(boundaries: &[Boundary], ignore_cumulative: bool) -> bool {
    boundaries.iter().any(|boundary| match boundary.kind {
        BoundaryKind::MultiStage => true,
        BoundaryKind::Cumulative => !ignore_cumulative,
        BoundaryKind::SubQueryDimension => false,
    })
}

// A multi-stage member is computed by its own stages, so its body is not
// walked. A rolling window and a subquery dimension are, since what they read
// can still be multi-stage.
#[derive(Default)]
struct BoundaryCollector {
    boundaries: Vec<Boundary>,
    visited: HashSet<String>,
    // Set by a caller that only asks whether a multi-stage boundary is crossed.
    stop_at_multi_stage: Option<bool>,
    done: bool,
}

impl BoundaryCollector {
    fn push(&mut self, kind: BoundaryKind, symbol: &Rc<MemberSymbol>) {
        self.boundaries.push(Boundary {
            kind,
            symbol: symbol.clone(),
        });
        if let Some(ignore_cumulative) = self.stop_at_multi_stage {
            self.done = crosses_multi_stage(&self.boundaries, ignore_cumulative);
        }
    }
}

impl TraversalVisitor for BoundaryCollector {
    type State = ();
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        _: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        if self.done || !self.visited.insert(node.full_name()) {
            return Ok(None);
        }
        match node.as_ref() {
            MemberSymbol::Measure(m) if m.is_multi_stage() => {
                self.push(BoundaryKind::MultiStage, node);
                return Ok(None);
            }
            MemberSymbol::Dimension(d) if d.is_multi_stage() => {
                self.push(BoundaryKind::MultiStage, node);
                return Ok(None);
            }
            MemberSymbol::Measure(m) if m.is_cumulative() => {
                self.push(BoundaryKind::Cumulative, node)
            }
            MemberSymbol::Dimension(d) if d.is_sub_query() => {
                self.push(BoundaryKind::SubQueryDimension, node)
            }
            // Its dependencies look through the base member, so the base itself
            // is checked here, and a multi-stage base keeps its body to itself.
            MemberSymbol::TimeDimension(td) => {
                self.apply(td.base_symbol(), &())?;
                if td.base_symbol().is_multi_stage() {
                    return Ok(None);
                }
            }
            _ => {}
        }
        Ok(Some(()))
    }
}

pub fn collect_boundaries(node: &Rc<MemberSymbol>) -> Result<Vec<Boundary>, CubeError> {
    let mut collector = BoundaryCollector::default();
    collector.apply(node, &())?;
    Ok(collector.boundaries)
}

pub fn has_multi_stage_members(
    node: &Rc<MemberSymbol>,
    ignore_cumulative: bool,
) -> Result<bool, CubeError> {
    let mut collector = BoundaryCollector {
        stop_at_multi_stage: Some(ignore_cumulative),
        ..Default::default()
    };
    collector.apply(node, &())?;
    Ok(collector.done)
}
