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
}

impl MemberExpressionSymbol {
    pub fn new(
        cube_name: String,
        name: String,
        expression: Rc<SqlCall>,
        definition: Option<String>,
    ) -> Self {
        Self {
            cube_name,
            name,
            expression,
            definition,
        }
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
}
