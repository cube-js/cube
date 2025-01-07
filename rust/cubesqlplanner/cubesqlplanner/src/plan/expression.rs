use super::QualifiedColumnName;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{BaseMember, VisitorContext};
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub struct MemberExpression {
    pub member: Rc<dyn BaseMember>,
}

impl MemberExpression {
    pub fn new(member: Rc<dyn BaseMember>) -> Self {
        Self { member }
    }

    pub fn to_sql(
        &self,
        _templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        self.member.to_sql(context)
    }
}

#[derive(Clone)]
pub struct FunctionExpression {
    pub function: String,
    pub arguments: Vec<Expr>,
}

#[derive(Clone)]
pub enum Expr {
    Member(MemberExpression),
    Reference(QualifiedColumnName),
    Function(FunctionExpression),
}

impl Expr {
    pub fn new_member(member: Rc<dyn BaseMember>) -> Self {
        Self::Member(MemberExpression::new(member))
    }
    pub fn new_reference(source: Option<String>, reference: String) -> Self {
        Self::Reference(QualifiedColumnName::new(source, reference))
    }
    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        match self {
            Self::Member(member) => member.to_sql(templates, context),
            Self::Reference(reference) => {
                templates.column_reference(reference.source(), &reference.name())
            }
            Expr::Function(FunctionExpression {
                function,
                arguments,
            }) => templates.scalar_function(
                function.to_string(),
                arguments
                    .iter()
                    .map(|e| e.to_sql(&templates, context.clone()))
                    .collect::<Result<Vec<_>, _>>()?,
                None,
                None,
            ),
        }
    }
}
