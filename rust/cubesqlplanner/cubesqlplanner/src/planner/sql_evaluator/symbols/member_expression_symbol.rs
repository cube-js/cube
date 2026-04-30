use super::common::CompiledMemberPath;
use super::MemberSymbol;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::member_childs;
use crate::planner::sql_evaluator::{
    sql_nodes::SqlNode, CubeRef, CubeTableSymbol, SqlCall, SqlEvaluatorVisitor,
};
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::utils::debug::DebugSql;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

#[derive(Clone)]
pub enum MemberExpressionExpression {
    SqlCall(Rc<SqlCall>),
    PatchedSymbol(Rc<MemberSymbol>),
}

#[derive(Clone)]
pub struct MemberExpressionSymbol {
    compiled_path: CompiledMemberPath,
    expression: MemberExpressionExpression,
    #[allow(dead_code)]
    definition: Option<String>,
    is_reference: bool,
    parenthesized: bool,
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
        }))
    }

    pub fn evaluate_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let sql = match &self.expression {
            MemberExpressionExpression::SqlCall(sql_call) => {
                sql_call.eval(visitor, node_processor, query_tools, templates)?
            }
            MemberExpressionExpression::PatchedSymbol(symbol) => {
                visitor.apply(symbol, node_processor, templates)?
            }
        };
        if self.parenthesized {
            Ok(format!("({})", sql))
        } else {
            Ok(sql)
        }
    }

    pub fn with_parenthesized(self: &Rc<Self>) -> Rc<Self> {
        let mut result = self.as_ref().clone();
        result.parenthesized = true;
        Rc::new(result)
    }

    pub fn compiled_path(&self) -> &CompiledMemberPath {
        &self.compiled_path
    }

    pub fn strip_join_prefix(&mut self) {
        self.compiled_path = self.compiled_path.strip_join_prefix();
    }

    pub fn full_name(&self) -> String {
        self.compiled_path.full_name().clone()
    }

    pub fn alias(&self) -> String {
        self.compiled_path.alias().clone()
    }

    pub fn is_reference(&self) -> bool {
        self.is_reference
    }

    pub fn reference_member(&self) -> Option<Rc<MemberSymbol>> {
        if !self.is_reference() {
            return None;
        }
        let deps = self.get_dependencies();
        if deps.is_empty() {
            return None;
        }
        deps.first().cloned()
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let mut result = self.clone();
        match &mut result.expression {
            MemberExpressionExpression::SqlCall(sql_call) => {
                *sql_call = sql_call.apply_recursive(f)?
            }
            MemberExpressionExpression::PatchedSymbol(member_symbol) => {
                *member_symbol = f(member_symbol)?
            }
        }

        Ok(MemberSymbol::new_member_expression(Rc::new(result)))
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = vec![];
        match &self.expression {
            MemberExpressionExpression::SqlCall(sql_call) => {
                sql_call.extract_symbol_deps(&mut deps)
            }
            MemberExpressionExpression::PatchedSymbol(member_symbol) => {
                deps.push(member_symbol.clone())
            }
        }
        deps
    }

    pub fn get_cube_refs(&self) -> Vec<CubeRef> {
        let mut refs = vec![];
        match &self.expression {
            MemberExpressionExpression::SqlCall(sql_call) => sql_call.extract_cube_refs(&mut refs),
            MemberExpressionExpression::PatchedSymbol(_) => {}
        }
        refs
    }

    pub fn cube_names_if_dimension_only_expression(
        self: Rc<Self>,
    ) -> Result<Option<Vec<String>>, CubeError> {
        let childs = member_childs(&MemberSymbol::new_member_expression(self), true)?;
        if childs.iter().any(|s| !s.is_dimension()) {
            Ok(None)
        } else {
            let cube_names = childs
                .into_iter()
                .map(|child| child.cube_name())
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
