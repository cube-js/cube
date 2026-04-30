use super::super::super::MemberSymbol;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{sql_nodes::SqlNode, CubeRef, SqlCall, SqlEvaluatorVisitor};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CalculatedMeasureType {
    Number,
    String,
    Time,
    Boolean,
}

impl CalculatedMeasureType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Number => "number",
            Self::String => "string",
            Self::Time => "time",
            Self::Boolean => "boolean",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "number" => Some(Self::Number),
            "string" => Some(Self::String),
            "time" => Some(Self::Time),
            "boolean" => Some(Self::Boolean),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct CalculatedMeasure {
    calc_type: CalculatedMeasureType,
    member_sql: Option<Rc<SqlCall>>,
}

impl CalculatedMeasure {
    pub fn new(calc_type: CalculatedMeasureType, member_sql: Rc<SqlCall>) -> Self {
        Self {
            calc_type,
            member_sql: Some(member_sql),
        }
    }

    pub fn new_without_sql(calc_type: CalculatedMeasureType) -> Self {
        Self {
            calc_type,
            member_sql: None,
        }
    }

    pub fn calc_type(&self) -> CalculatedMeasureType {
        self.calc_type
    }

    pub fn member_sql(&self) -> Option<&Rc<SqlCall>> {
        self.member_sql.as_ref()
    }

    pub fn evaluate_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        match &self.member_sql {
            Some(sql) => sql.eval(visitor, node_processor, query_tools, templates),
            None => Err(CubeError::internal(
                "Calculated measure without sql cannot be evaluated directly".to_string(),
            )),
        }
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = vec![];
        if let Some(sql) = &self.member_sql {
            sql.extract_symbol_deps(&mut deps);
        }
        deps
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        Ok(Self {
            calc_type: self.calc_type,
            member_sql: self
                .member_sql
                .as_ref()
                .map(|sql| sql.apply_recursive(f))
                .transpose()?,
        })
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        Box::new(self.member_sql.iter())
    }

    pub fn get_cube_refs(&self) -> Vec<CubeRef> {
        let mut refs = vec![];
        if let Some(sql) = &self.member_sql {
            sql.extract_cube_refs(&mut refs);
        }
        refs
    }

    pub fn is_owned_by_cube(&self) -> bool {
        self.member_sql
            .as_ref()
            .is_some_and(|sql| sql.is_owned_by_cube())
    }
}
