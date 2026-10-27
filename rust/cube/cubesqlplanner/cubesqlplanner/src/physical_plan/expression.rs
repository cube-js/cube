use super::filter::render_filter_item;
use super::{evaluate_with_context, QualifiedColumnName, ReferenceSubstitutions, VisitorContext};
use crate::planner::filter::FilterItem;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::symbols::transforms;
use crate::planner::MemberSymbol;
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
    ConditionalMeasure(MemberExpression, FilterItem),
    UnionPaddingNull,
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

    /// A member bound to a context of its own renders in that context, not
    /// the select's, so it is not substituted.
    pub fn substitute(&self, substitutions: &ReferenceSubstitutions) -> Result<Self, CubeError> {
        Ok(match self {
            Self::Member(member) if member.context.is_none() => {
                Self::Member(MemberExpression::new(transforms::substitute_by_name(
                    &member.member,
                    substitutions,
                )?))
            }
            Self::Function(FunctionExpression {
                function,
                arguments,
            }) => Self::Function(FunctionExpression {
                function: function.clone(),
                arguments: arguments
                    .iter()
                    .map(|a| a.substitute(substitutions))
                    .collect::<Result<Vec<_>, _>>()?,
            }),
            _ => self.clone(),
        })
    }
    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        match self {
            Self::ConditionalMeasure(measure, condition) => {
                let condition = render_filter_item(&context, condition, templates)?;
                let visitor = context
                    .make_visitor(context.query_tools())
                    .with_measure_row_condition(Some(condition));
                visitor.apply(&measure.member, context.node_processor(), templates)
            }
            Self::UnionPaddingNull => templates.full_key_aggregate_union_null(),
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
