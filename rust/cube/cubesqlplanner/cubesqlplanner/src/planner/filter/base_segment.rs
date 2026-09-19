use crate::planner::{
    CubeTableSymbol, MemberExpressionExpression, MemberExpressionSymbol, MemberSymbol, SqlCall,
};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// One `segments:` entry from the data model — a boolean expression
/// attached to a cube under a name, materialised as a synthetic
/// `MemberExpression` so it plugs into the same member machinery as
/// dimensions and measures.
#[derive(Clone)]
pub struct BaseSegment {
    full_name: String,
    member_evaluator: Rc<MemberSymbol>,
    cube_name: String,
    name: String,
    /// True when this segment is an ad-hoc query-level member expression (no
    /// registered `segments:` path), as opposed to a named cube segment.
    is_member_expression: bool,
}

impl PartialEq for BaseSegment {
    fn eq(&self, other: &Self) -> bool {
        self.full_name == other.full_name
    }
}

impl BaseSegment {
    pub fn try_new(
        expression: Rc<SqlCall>,
        cube_symbol: Rc<CubeTableSymbol>,
        name: String,
        full_name: Option<String>,
    ) -> Result<Rc<Self>, CubeError> {
        let cube_name = cube_symbol.cube_name().clone();
        let member_expression_symbol = MemberExpressionSymbol::try_new(
            cube_symbol,
            name.clone(),
            MemberExpressionExpression::SqlCall(expression),
            None,
            None,
            vec![cube_name.clone()],
        )?;
        let is_member_expression = full_name.is_none();
        let full_name = full_name.unwrap_or(member_expression_symbol.full_name());
        let member_evaluator = MemberSymbol::new_member_expression(member_expression_symbol);

        Ok(Rc::new(Self {
            full_name,
            member_evaluator,
            cube_name,
            name,
            is_member_expression,
        }))
    }

    pub fn is_member_expression(&self) -> bool {
        self.is_member_expression
    }

    /// Whether `member` names this segment, as a `FILTER_PARAMS` binding or a
    /// filter-tree target does. A view exposes a segment under its own path
    /// while a binding in the underlying cube's sql names the cube's, so every
    /// segment in the reference chain counts, not only the name the query asked
    /// for. The chain stops at the first non-segment: a segment whose sql is a
    /// bare reference resolves on to that dimension, whose own binding states a
    /// column to compare a value against rather than a predicate.
    pub fn matches_member_name(&self, member: &str) -> bool {
        if self.is_member_expression {
            return false;
        }
        if self.full_name == member {
            return true;
        }
        let mut current = Some(self.member_evaluator.clone());
        while let Some(symbol) = current {
            if symbol.as_member_expression().is_err() {
                return false;
            }
            // A segment symbol lives in the `expr:` namespace, so the path is
            // reassembled from the cube and member names it was compiled under.
            if format!("{}.{}", symbol.cube_name(), symbol.name()) == member {
                return true;
            }
            current = symbol.reference_member();
        }
        false
    }

    pub fn full_name(&self) -> String {
        self.full_name.clone()
    }

    pub fn member_evaluator(&self) -> Rc<MemberSymbol> {
        self.member_evaluator.clone()
    }

    pub fn with_member_evaluator(&self, member_evaluator: Rc<MemberSymbol>) -> Rc<Self> {
        let mut result = self.clone();
        result.member_evaluator = member_evaluator;
        Rc::new(result)
    }

    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }

    pub fn name(&self) -> &String {
        &self.name
    }
}
