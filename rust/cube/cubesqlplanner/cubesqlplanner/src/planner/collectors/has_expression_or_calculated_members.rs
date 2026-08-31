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
            MemberSymbol::MemberExpression(_) => self.found = true,
            MemberSymbol::Measure(s) => {
                if s.is_calculated() {
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

/// Whether the dependency tree of `node` contains a member whose own body
/// writes SQL around the members it references - a member expression or a
/// measure of a calculated kind. What such a body computes is not a function
/// of the members it references: an aggregate written in it is not a member of
/// anything and appears nowhere in the tree.
pub fn has_expression_or_calculated_members(node: &Rc<MemberSymbol>) -> Result<bool, CubeError> {
    let mut visitor = HasExpressionOrCalculatedMembersCollector::new();
    visitor.apply(node, &())?;
    Ok(visitor.extract_result())
}
