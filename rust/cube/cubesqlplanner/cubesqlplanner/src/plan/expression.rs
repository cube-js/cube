use super::QualifiedColumnName;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{evaluate_with_context, VisitorContext};
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub struct MemberExpression {
    pub member: Rc<MemberSymbol>,
    pub context: Option<Rc<VisitorContext>>,
}

impl MemberExpression {
    pub fn new(member: Rc<MemberSymbol>) -> Self {
        Self {
            member,
            context: None,
        }
    }

    pub fn new_with_context(member: Rc<MemberSymbol>, context: Rc<VisitorContext>) -> Self {
        Self {
            member,
            context: Some(context),
        }
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        evaluate_with_context(&self.member, context, templates)
    }
}

#[derive(Clone)]
pub struct FunctionExpression {
    pub function: String,
    pub arguments: Vec<Expr>,
}

#[derive(Clone)]
pub enum Expr {
    Null,
    Member(MemberExpression),
    Reference(QualifiedColumnName),
    GroupAny(QualifiedColumnName),
    Function(FunctionExpression),
    Asterisk,
}

impl Expr {
    pub fn new_member(member: Rc<MemberSymbol>) -> Self {
        Self::Member(MemberExpression::new(member))
    }
    pub fn new_member_with_context(member: Rc<MemberSymbol>, context: Rc<VisitorContext>) -> Self {
        Self::Member(MemberExpression::new_with_context(member, context))
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
            Self::Null => Ok(format!("CAST(NULL as integer)")),
            Self::Member(member) => {
                let context = if let Some(self_context) = &member.context {
                    self_context.clone()
                } else {
                    context
                };
                member.to_sql(templates, context)
            }
            Self::Reference(reference) => {
                templates.column_reference(reference.source(), &reference.name())
            }
            Self::GroupAny(reference) => {
                let reference =
                    templates.column_reference(reference.source(), &reference.name())?;
                templates.group_any(&reference)
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
            Self::Asterisk => Ok("*".to_string()),
        }
    }
}
