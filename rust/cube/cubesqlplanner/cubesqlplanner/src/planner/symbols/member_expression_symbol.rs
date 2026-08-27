use super::common::CompiledMemberPath;
use super::deps::{self, symbol_deps, DepVisitor, DepVisitorMut, SymbolDeps};
use super::MemberSymbol;
use crate::planner::collectors::member_childs;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{CubeTableSymbol, SqlCall};
use crate::utils::debug::DebugSql;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

/// Body of a member expression.
///
/// - `SqlCall` — an arbitrary SQL expression provided directly by the
///   query input.
/// - `PatchedSymbol` — an existing member with query-time
///   modifications applied on top.
#[derive(Clone)]
pub enum MemberExpressionExpression {
    SqlCall(Rc<SqlCall>),
    PatchedSymbol(Rc<MemberSymbol>),
}

impl SymbolDeps for MemberExpressionExpression {
    fn visit_deps(&self, visitor: &mut dyn DepVisitor) -> std::ops::ControlFlow<()> {
        match self {
            Self::SqlCall(sql_call) => sql_call.visit_deps(visitor),
            Self::PatchedSymbol(symbol) => visitor.symbol(symbol),
        }
    }

    fn visit_deps_mut(&mut self, visitor: &mut dyn DepVisitorMut) -> Result<(), CubeError> {
        match self {
            Self::SqlCall(sql_call) => sql_call.visit_deps_mut(visitor),
            Self::PatchedSymbol(symbol) => visitor.symbol(symbol),
        }
    }
}

/// `MemberSymbol::MemberExpression` body: a synthetic member built
/// at query time from a SQL expression or from another member with
/// query-time modifications. Not declared in the data model. Its
/// full name lives in the `expr:` namespace.
#[derive(Clone)]
pub struct MemberExpressionSymbol {
    pub(super) compiled_path: CompiledMemberPath,
    pub(super) expression: MemberExpressionExpression,
    #[allow(dead_code)]
    pub(super) definition: Option<String>,
    pub(super) is_reference: bool,
    pub(super) parenthesized: bool,
    /// True when this expression materialises a `segments:` entry used as a
    /// selected dimension (in a pre-aggregation). Such a boolean must be
    /// wrapped per dialect when projected/grouped (e.g. MSSQL `BIT`).
    pub(super) is_segment: bool,
}

symbol_deps! {
    MemberExpressionSymbol {
        expression: dep,
        compiled_path: skip,
        definition: skip,
        is_reference: skip,
        parenthesized: skip,
        is_segment: skip,
    }
}

impl MemberExpressionSymbol {
    pub fn try_new(
        cube: Rc<CubeTableSymbol>,
        name: String,
        expression: MemberExpressionExpression,
        definition: Option<String>,
        alias: Option<String>,
        path: Vec<String>,
    ) -> Result<Rc<Self>, CubeError> {
        let full_name = format!("expr:{}.{}", cube.cube_name(), name);
        let alias = alias.unwrap_or_else(|| PlanSqlTemplates::alias_name(&name));
        let is_reference = match &expression {
            MemberExpressionExpression::SqlCall(sql_call) => sql_call.is_direct_reference(),
            MemberExpressionExpression::PatchedSymbol(_symbol) => false,
        };
        let compiled_path = CompiledMemberPath::new(cube, full_name, name, alias, path);
        Ok(Rc::new(Self {
            compiled_path,
            expression,
            definition,
            is_reference,
            parenthesized: false,
            is_segment: false,
        }))
    }

    /// Returns a copy of the symbol marked as parenthesized when
    /// rendered.
    pub fn with_parenthesized(self: &Rc<Self>) -> Rc<Self> {
        let mut result = self.as_ref().clone();
        result.parenthesized = true;
        Rc::new(result)
    }

    /// Returns a copy marked as a segment-as-dimension, so rendering wraps it
    /// per dialect (e.g. MSSQL `CAST(... AS BIT)`).
    pub fn with_is_segment(self: &Rc<Self>) -> Rc<Self> {
        let mut result = self.as_ref().clone();
        result.is_segment = true;
        Rc::new(result)
    }

    pub fn is_segment(&self) -> bool {
        self.is_segment
    }

    pub fn expression(&self) -> &MemberExpressionExpression {
        &self.expression
    }

    pub fn is_parenthesized(&self) -> bool {
        self.parenthesized
    }

    pub fn compiled_path(&self) -> &CompiledMemberPath {
        &self.compiled_path
    }

    /// Full unique identifier of the symbol; lives in the `expr:`
    /// namespace to keep it disjoint from data-model member names.
    pub fn full_name(&self) -> String {
        self.compiled_path.full_name().clone()
    }

    /// Default alias of the expression, derived from the compiled
    /// member path.
    pub fn alias(&self) -> String {
        self.compiled_path.alias().clone()
    }

    pub fn is_reference(&self) -> bool {
        self.is_reference
    }

    /// The member this expression references, or `None` if it is not
    /// a reference. An expression is a reference only when its body
    /// is a `SqlCall` that is itself a direct member reference.
    pub fn reference_member(&self) -> Option<Rc<MemberSymbol>> {
        if !self.is_reference() {
            return None;
        }
        self.get_dependencies().first().cloned()
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        deps::collect_deps(self)
    }

    /// If every leaf member referenced by the expression is a
    /// dimension, returns the list of cube names those dimensions
    /// belong to. Returns `None` if any leaf is a measure or other
    /// non-dimension member.
    pub fn cube_names_if_dimension_only_expression(
        self: Rc<Self>,
    ) -> Result<Option<Vec<String>>, CubeError> {
        let childs = member_childs(&MemberSymbol::new_member_expression(self), true)?;
        if childs.iter().any(|s| !s.is_dimension()) {
            Ok(None)
        } else {
            // Single member expression can reference multiple dimensions from
            // the same cube
            let cube_names = childs
                .into_iter()
                .map(|child| child.cube_name())
                .unique()
                .collect_vec();
            Ok(Some(cube_names))
        }
    }

    pub fn cube_name(&self) -> String {
        self.compiled_path.cube_name().clone()
    }

    pub fn name(&self) -> String {
        self.compiled_path.name().clone()
    }

    pub fn path(&self) -> &Vec<String> {
        self.compiled_path.path()
    }

    pub fn definition(&self) -> &Option<String> {
        &self.definition
    }
}

impl DebugSql for MemberExpressionSymbol {
    fn debug_sql(&self, expand_deps: bool) -> String {
        match &self.expression {
            MemberExpressionExpression::SqlCall(sql) => sql.debug_sql(expand_deps),
            MemberExpressionExpression::PatchedSymbol(symbol) => {
                if expand_deps {
                    symbol.debug_sql(true)
                } else {
                    format!("{{EXPRESSION:{}}}", self.full_name())
                }
            }
        }
    }
}
