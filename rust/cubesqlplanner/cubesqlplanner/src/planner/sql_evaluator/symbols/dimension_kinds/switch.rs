use super::super::MemberSymbol;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{sql_nodes::SqlNode, SqlCall, SqlEvaluatorVisitor};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub struct SwitchDimension {
    values: Vec<String>,
    member_sql: Option<Rc<SqlCall>>,
}

impl SwitchDimension {
    pub fn new(values: Vec<String>, member_sql: Option<Rc<SqlCall>>) -> Self {
        Self { values, member_sql }
    }

    pub fn values(&self) -> &[String] {
        &self.values
    }

    pub fn member_sql(&self) -> Option<&Rc<SqlCall>> {
        self.member_sql.as_ref()
    }

    pub fn is_calc_group(&self) -> bool {
        self.member_sql.is_none()
    }

    pub fn evaluate_sql(
        &self,
        name: &str,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        if let Some(member_sql) = &self.member_sql {
            member_sql.eval(visitor, node_processor, query_tools, templates)
        } else {
            Ok(templates.quote_identifier(name)?)
        }
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = vec![];
        if let Some(member_sql) = &self.member_sql {
            member_sql.extract_symbol_deps(&mut deps);
        }
        deps
    }

    pub fn get_dependencies_with_path(&self) -> Vec<(Rc<MemberSymbol>, Vec<String>)> {
        let mut deps = vec![];
        if let Some(member_sql) = &self.member_sql {
            member_sql.extract_symbol_deps_with_path(&mut deps);
        }
        deps
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        let member_sql = if let Some(sql) = &self.member_sql {
            Some(sql.apply_recursive(f)?)
        } else {
            None
        };
        Ok(Self {
            values: self.values.clone(),
            member_sql,
        })
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        Box::new(self.member_sql.iter())
    }

    pub fn is_owned_by_cube(&self) -> bool {
        false
    }
}
