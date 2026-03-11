use super::super::common::{Case, DimensionType};
use super::super::MemberSymbol;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{sql_nodes::SqlNode, CubeRef, SqlCall, SqlEvaluatorVisitor};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub struct CaseDimension {
    dimension_type: DimensionType,
    case: Case,
    member_sql: Option<Rc<SqlCall>>,
}

impl CaseDimension {
    pub fn new(dimension_type: DimensionType, case: Case, member_sql: Option<Rc<SqlCall>>) -> Self {
        Self {
            dimension_type,
            case,
            member_sql,
        }
    }

    pub fn dimension_type(&self) -> &DimensionType {
        &self.dimension_type
    }

    pub fn case(&self) -> &Case {
        &self.case
    }

    pub fn member_sql(&self) -> Option<&Rc<SqlCall>> {
        self.member_sql.as_ref()
    }

    pub fn replace_case(&self, new_case: Case) -> Self {
        Self {
            dimension_type: self.dimension_type,
            case: new_case,
            member_sql: self.member_sql.clone(),
        }
    }

    pub fn evaluate_sql(
        &self,
        full_name: &str,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        if let Some(member_sql) = &self.member_sql {
            member_sql.eval(visitor, node_processor, query_tools, templates)
        } else {
            Err(CubeError::internal(format!(
                "Dimension {} hasn't sql evaluator",
                full_name
            )))
        }
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = vec![];
        if let Some(member_sql) = &self.member_sql {
            member_sql.extract_symbol_deps(&mut deps);
        }
        self.case.extract_symbol_deps(&mut deps);
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
            dimension_type: self.dimension_type,
            case: self.case.apply_to_deps(f)?,
            member_sql,
        })
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        Box::new(self.member_sql.iter().chain(self.case.iter_sql_calls()))
    }

    pub fn get_cube_refs(&self) -> Vec<CubeRef> {
        let mut refs = vec![];
        if let Some(member_sql) = &self.member_sql {
            member_sql.extract_cube_refs(&mut refs);
        }
        self.case.extract_cube_refs(&mut refs);
        refs
    }

    pub fn is_owned_by_cube(&self) -> bool {
        let mut owned = false;
        if let Some(sql) = &self.member_sql {
            owned |= sql.is_owned_by_cube();
        }
        owned |= self.case.is_owned_by_cube();
        owned
    }
}
