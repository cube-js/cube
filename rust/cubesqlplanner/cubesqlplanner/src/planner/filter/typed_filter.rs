use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{evaluate_with_context, VisitorContext};
use cubenativeutils::CubeError;
use std::rc::Rc;

use super::base_filter::FilterType;
use super::operators::equality::EqualityOp;
use super::operators::in_list::InListOp;
use super::operators::nullability::NullabilityOp;
use super::operators::{FilterOperationSql, FilterSqlContext};
use super::FilterOperator;

#[derive(Clone, Debug)]
pub enum FilterOp {
    Equality(EqualityOp),
    InList(InListOp),
    Nullability(NullabilityOp),
}

#[derive(Clone)]
pub struct TypedFilter {
    #[allow(dead_code)]
    query_tools: Rc<QueryTools>,
    member_evaluator: Rc<MemberSymbol>,
    #[allow(dead_code)]
    filter_type: FilterType,
    op: FilterOp,
}

impl TypedFilter {
    pub fn builder() -> TypedFilterBuilder {
        TypedFilterBuilder::default()
    }

    pub fn to_sql(
        &self,
        context: Rc<VisitorContext>,
        plan_templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let member_sql =
            evaluate_with_context(&self.member_evaluator, context.clone(), plan_templates)?;

        let ctx = FilterSqlContext {
            member_sql: &member_sql,
            query_tools: &self.query_tools,
            plan_templates,
        };

        match &self.op {
            FilterOp::Equality(op) => op.to_sql(&ctx),
            FilterOp::InList(op) => op.to_sql(&ctx),
            FilterOp::Nullability(op) => op.to_sql(&ctx),
        }
    }

}

#[derive(Default)]
pub struct TypedFilterBuilder {
    query_tools: Option<Rc<QueryTools>>,
    member_evaluator: Option<Rc<MemberSymbol>>,
    filter_type: Option<FilterType>,
    operator: Option<FilterOperator>,
    values: Option<Vec<Option<String>>>,
}

impl TypedFilterBuilder {
    pub fn query_tools(mut self, v: Rc<QueryTools>) -> Self {
        self.query_tools = Some(v);
        self
    }

    pub fn member_evaluator(mut self, v: Rc<MemberSymbol>) -> Self {
        self.member_evaluator = Some(v);
        self
    }

    pub fn filter_type(mut self, v: FilterType) -> Self {
        self.filter_type = Some(v);
        self
    }

    pub fn operator(mut self, v: FilterOperator) -> Self {
        self.operator = Some(v);
        self
    }

    pub fn values(mut self, v: Option<Vec<Option<String>>>) -> Self {
        self.values = v;
        self
    }

    fn resolve_member_type(member_evaluator: &Rc<MemberSymbol>) -> Option<String> {
        let symbol = if let Ok(td) = member_evaluator.as_time_dimension() {
            td.base_symbol().clone()
        } else {
            member_evaluator.clone()
        };
        match symbol.as_ref() {
            MemberSymbol::Dimension(d) => Some(d.dimension_type().to_string()),
            MemberSymbol::Measure(m) => Some(m.measure_type().to_string()),
            _ => None,
        }
    }

    // FIXME: return TypedFilter directly once all operators are migrated from BaseFilter
    pub fn build(self) -> Result<Option<TypedFilter>, CubeError> {
        let query_tools = self
            .query_tools
            .ok_or_else(|| CubeError::internal("query_tools is required".to_string()))?;
        let member_evaluator = self
            .member_evaluator
            .ok_or_else(|| CubeError::internal("member_evaluator is required".to_string()))?;
        let filter_type = self
            .filter_type
            .ok_or_else(|| CubeError::internal("filter_type is required".to_string()))?;
        let operator = self
            .operator
            .ok_or_else(|| CubeError::internal("operator is required".to_string()))?;
        let values = self.values.unwrap_or_default();

        let member_type = Self::resolve_member_type(&member_evaluator);

        let op = match operator {
            FilterOperator::Equal => {
                FilterOp::Equality(EqualityOp::new(false, values, member_type))
            }
            FilterOperator::NotEqual => {
                FilterOp::Equality(EqualityOp::new(true, values, member_type))
            }
            FilterOperator::In => FilterOp::InList(InListOp::new(false, values, member_type)),
            FilterOperator::NotIn => FilterOp::InList(InListOp::new(true, values, member_type)),
            FilterOperator::Set => FilterOp::Nullability(NullabilityOp::new(false)),
            FilterOperator::NotSet => FilterOp::Nullability(NullabilityOp::new(true)),
            _ => return Ok(None),
        };

        Ok(Some(TypedFilter {
            query_tools,
            member_evaluator,
            filter_type,
            op,
        }))
    }
}
