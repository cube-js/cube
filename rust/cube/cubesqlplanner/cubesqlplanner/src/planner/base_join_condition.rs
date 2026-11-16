use super::{CubeId, SqlCall};
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

    /// The cube this condition joins in, when it is a cube join.
    fn joined_cube(&self) -> Option<&CubeId> {
        None
    }
}
pub struct SqlJoinCondition {
    sql_call: Rc<SqlCall>,
    joined_cube: Option<CubeId>,
}
impl SqlJoinCondition {
    pub fn try_new(sql_call: Rc<SqlCall>) -> Result<Rc<Self>, CubeError> {
        Ok(Rc::new(Self {
            sql_call,
            joined_cube: None,
        }))
    }

    pub fn try_new_cube_join(
        sql_call: Rc<SqlCall>,
        joined_cube: CubeId,
    ) -> Result<Rc<Self>, CubeError> {
        Ok(Rc::new(Self {
            sql_call,
            joined_cube: Some(joined_cube),
        }))
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
            joined_cube: self.joined_cube.clone(),
        }))
    }

    fn joined_cube(&self) -> Option<&CubeId> {
        self.joined_cube.as_ref()
    }
}
