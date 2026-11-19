use crate::planner::{MemberId, MemberSymbol, TraversalVisitor};
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

impl BoundaryKind {
    fn crosses_multi_stage(&self, ignore_cumulative: bool) -> bool {
        match self {
            Self::MultiStage => true,
            Self::Cumulative => !ignore_cumulative,
            Self::SubQueryDimension => false,
        }
    }
}

/// Whether any of the boundaries puts part of the symbol into a multi-stage stage.
pub fn crosses_multi_stage(boundaries: &[Boundary], ignore_cumulative: bool) -> bool {
    boundaries
        .iter()
        .any(|boundary| boundary.kind.crosses_multi_stage(ignore_cumulative))
}

#[derive(Default)]
enum Mode {
    #[default]
    CollectAll,
    StopAtMultiStage {
        ignore_cumulative: bool,
    },
}

// A multi-stage member and a subquery dimension are computed by queries of their
// own, so their bodies are not walked. A rolling window is, since what it reads
// can still be multi-stage.
#[derive(Default)]
struct BoundaryCollector {
    boundaries: Vec<Boundary>,
    visited: HashSet<MemberId>,
    mode: Mode,
    done: bool,
}

impl BoundaryCollector {
    fn push(&mut self, kind: BoundaryKind, symbol: &Rc<MemberSymbol>) {
        if let Mode::StopAtMultiStage { ignore_cumulative } = self.mode {
            self.done = kind.crosses_multi_stage(ignore_cumulative);
        }
        self.boundaries.push(Boundary {
            kind,
            symbol: symbol.clone(),
        });
    }
}

impl TraversalVisitor for BoundaryCollector {
    type State = ();
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        _: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        if self.done || !self.visited.insert(node.id().clone()) {
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
                self.push(BoundaryKind::SubQueryDimension, node);
                return Ok(None);
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
        mode: Mode::StopAtMultiStage { ignore_cumulative },
        ..Default::default()
    };
    collector.apply(node, &())?;
    Ok(collector.done)
}
