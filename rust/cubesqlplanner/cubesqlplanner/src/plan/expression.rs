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
        if let Some(reference_column) =
            schema.find_column_for_member(&self.member.full_name(), &self.source)
        {
            templates.column_reference(&reference_column.table_name, &reference_column.alias)
        } else {
            self.member.to_sql(context, schema)
        }
    }
}

#[derive(Clone)]
pub struct ReferenceExpression {
    pub reference: String,
    pub source: Option<String>,
}

impl ReferenceExpression {
    pub fn new(reference: String, source: Option<String>) -> Self {
        Self { reference, source }
    }

    pub fn to_sql(&self, templates: &PlanSqlTemplates) -> Result<String, CubeError> {
        templates.column_reference(&self.source, &self.reference)
    }
}

#[derive(Clone)]
pub enum Expr {
    Member(MemberExpression),
    Reference(ReferenceExpression),
}

impl Expr {
    pub fn new_member(member: Rc<dyn BaseMember>, source: Option<String>) -> Self {
        Self::Member(MemberExpression::new(member, source))
    }
    pub fn new_reference(reference: String, source: Option<String>) -> Self {
        Self::Reference(ReferenceExpression::new(reference, source))
    }
    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
        schema: Rc<Schema>,
    ) -> Result<String, CubeError> {
        match self {
            Self::Member(member) => member.to_sql(templates, context, schema),
            Self::Reference(reference) => reference.to_sql(templates),
        }
    }
}
