use super::SqlCall;
use crate::physical_plan::{
    evaluate_sql_call_with_context, ReferenceSubstitutions, VisitorContext,
};
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::symbols::transforms;
use cubenativeutils::CubeError;
use std::rc::Rc;
pub trait BaseJoinCondition {
    fn to_sql(
        &self,
        context: Rc<VisitorContext>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError>;

    /// The same condition reading the members in `substitutions` the way
    /// the enclosing select reads them.
    fn substitute(
        &self,
        substitutions: &ReferenceSubstitutions,
    ) -> Result<Rc<dyn BaseJoinCondition>, CubeError>;
}
pub struct SqlJoinCondition {
    sql_call: Rc<SqlCall>,
}
impl SqlJoinCondition {
    pub fn try_new(sql_call: Rc<SqlCall>) -> Result<Rc<Self>, CubeError> {
        Ok(Rc::new(Self { sql_call }))
    }
}

impl BaseJoinCondition for SqlJoinCondition {
    fn to_sql(
        &self,
        context: Rc<VisitorContext>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        evaluate_sql_call_with_context(&self.sql_call, context, templates)
    }

    fn substitute(
        &self,
        substitutions: &ReferenceSubstitutions,
    ) -> Result<Rc<dyn BaseJoinCondition>, CubeError> {
        Ok(Rc::new(Self {
            sql_call: transforms::substitute_in_sql_call(&self.sql_call, substitutions)?,
        }))
    }
}
