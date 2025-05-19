use super::MemberSymbol;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{sql_nodes::SqlNode, SqlCall, SqlEvaluatorVisitor};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct MemberExpressionSymbol {
    cube_name: String,
    name: String,
    expression: Rc<SqlCall>,
    #[allow(dead_code)]
    definition: Option<String>,
    is_reference: bool,
}

impl MemberExpressionSymbol {
    pub fn try_new(
        cube_name: String,
        name: String,
        expression: Rc<SqlCall>,
        definition: Option<String>,
    ) -> Result<Self, CubeError> {
        let is_reference = expression.is_direct_reference()?;
        Ok(Self {
            cube_name,
            name,
            expression,
            definition,
            is_reference,
        })
    }

    pub fn evaluate_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let sql = self
            .expression
            .eval(visitor, node_processor, query_tools, templates)?;
        Ok(sql)
    }

    pub fn expression(&self) -> &Rc<SqlCall> {
        &self.expression
    }

    pub fn full_name(&self) -> String {
        format!("expr:{}.{}", self.cube_name, self.name)
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
        self.expression.extract_symbol_deps(&mut deps);
        deps
    }

    pub fn get_dependencies_with_path(&self) -> Vec<(Rc<MemberSymbol>, Vec<String>)> {
        let mut deps = vec![];
        self.expression.extract_symbol_deps_with_path(&mut deps);
        deps
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
