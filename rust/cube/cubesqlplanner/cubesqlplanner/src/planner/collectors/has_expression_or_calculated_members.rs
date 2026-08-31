use crate::planner::{MemberSymbol, TraversalVisitor};
use cubenativeutils::CubeError;
use std::rc::Rc;

struct HasExpressionOrCalculatedMembersCollector {
    found: bool,
}

impl HasExpressionOrCalculatedMembersCollector {
    fn new() -> Self {
        Self { found: false }
    }

    fn extract_result(self) -> bool {
        self.found
    }
}

impl TraversalVisitor for HasExpressionOrCalculatedMembersCollector {
    type State = ();
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        _: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        match node.as_ref() {
            MemberSymbol::MemberExpression(s) => {
                if !s.is_reference() {
                    self.found = true;
                }
            }
            MemberSymbol::Measure(s) => {
                if s.is_calculated() && !s.is_reference() {
                    self.found = true;
                }
            }
            _ => {}
        };
        if self.found {
            Ok(None)
        } else {
            Ok(Some(()))
        }
    }
}

/// Whether the dependency tree of `node` contains a member that writes SQL of
/// its own around the members it references - a member expression or a measure
/// of a calculated kind. What such a body computes is not a function of the
/// members it references: an aggregate written in it is not a member of
/// anything and appears nowhere in the tree.
///
/// A bare reference writes nothing of its own - every measure of a view is one
/// - so it is followed into the member it references instead.
pub fn has_expression_or_calculated_members(node: &Rc<MemberSymbol>) -> Result<bool, CubeError> {
    let mut visitor = HasExpressionOrCalculatedMembersCollector::new();
    visitor.apply(node, &())?;
    Ok(visitor.extract_result())
}
