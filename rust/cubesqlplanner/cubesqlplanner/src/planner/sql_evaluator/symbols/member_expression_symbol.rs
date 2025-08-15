use super::MemberSymbol;
use crate::cube_bridge::base_tools::BaseTools;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::member_childs;
use crate::planner::sql_evaluator::{sql_nodes::SqlNode, SqlCall, SqlEvaluatorVisitor};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub enum MemberExpressionExpression {
    SqlCall(Rc<SqlCall>),
    PatchedSymbol(Rc<MemberSymbol>),
}

pub struct MemberExpressionSymbol {
    cube_name: String,
    name: String,
    expression: MemberExpressionExpression,
    #[allow(dead_code)]
    definition: Option<String>,
    is_reference: bool,
}

impl MemberExpressionSymbol {
    pub fn try_new(
        cube_name: String,
        name: String,
        expression: MemberExpressionExpression,
        definition: Option<String>,
        base_tools: Rc<dyn BaseTools>,
    ) -> Result<Rc<Self>, CubeError> {
        let is_reference = match &expression {
            MemberExpressionExpression::SqlCall(sql_call) => {
                sql_call.is_direct_reference(base_tools.clone())?
            }
            MemberExpressionExpression::PatchedSymbol(_symbol) => false,
        };
        Ok(Rc::new(Self {
            cube_name,
            name,
            expression,
            definition,
            is_reference,
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
        Ok(sql)
    }

    pub fn full_name(&self) -> String {
        format!("expr:{}.{}", self.cube_name, self.name)
    }

    pub fn alias(&self) -> String {
        PlanSqlTemplates::alias_name(&self.name)
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

    pub fn get_dependencies_with_path(&self) -> Vec<(Rc<MemberSymbol>, Vec<String>)> {
        let mut deps = vec![];
        match &self.expression {
            MemberExpressionExpression::SqlCall(sql_call) => {
                sql_call.extract_symbol_deps_with_path(&mut deps)
            }
            MemberExpressionExpression::PatchedSymbol(member_symbol) => {
                deps.push((member_symbol.clone(), vec![]))
            }
        }
        deps
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

    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn definition(&self) -> &Option<String> {
        &self.definition
    }
}
