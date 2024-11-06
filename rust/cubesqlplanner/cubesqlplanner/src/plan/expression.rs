use super::Schema;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{BaseMember, VisitorContext};
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub struct MemberExpression {
    pub member: Rc<dyn BaseMember>,
    pub source: Option<String>,
}

impl MemberExpression {
    pub fn new(member: Rc<dyn BaseMember>, source: Option<String>) -> Self {
        Self { member, source }
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
        schema: Rc<Schema>,
    ) -> Result<String, CubeError> {
        if let Some(reference_column) = context
            .source_schema()
            .find_column_for_member(&self.member.full_name(), &self.source)
        {
            templates.column_reference(&reference_column.table_name, &reference_column.alias)
        } else {
            self.member.to_sql(context, schema)
        }
    }
}

#[derive(Clone)]
pub enum Expr {
    Member(MemberExpression),
}

impl Expr {
    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
        schema: Rc<Schema>,
    ) -> Result<String, CubeError> {
        match self {
            Expr::Member(member) => member.to_sql(templates, context, schema),
        }
    }
}
