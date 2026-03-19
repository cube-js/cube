use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::VisitorContext;
use cubenativeutils::CubeError;
use std::rc::Rc;

use super::base_filter::FilterType;
use super::FilterOperator;

#[derive(Clone, Debug)]
pub enum FilterOp {}

#[derive(Clone)]
pub struct TypedFilter {
    #[allow(dead_code)]
    query_tools: Rc<QueryTools>,
    #[allow(dead_code)]
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
        _context: Rc<VisitorContext>,
        _plan_templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        match self.op {}
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

    // FIXME: return TypedFilter directly once all operators are migrated from BaseFilter
    pub fn build(self) -> Result<Option<TypedFilter>, CubeError> {
        let _query_tools = self
            .query_tools
            .ok_or_else(|| CubeError::internal("query_tools is required".to_string()))?;
        let _member_evaluator = self
            .member_evaluator
            .ok_or_else(|| CubeError::internal("member_evaluator is required".to_string()))?;
        let _filter_type = self
            .filter_type
            .ok_or_else(|| CubeError::internal("filter_type is required".to_string()))?;
        let _operator = self
            .operator
            .ok_or_else(|| CubeError::internal("operator is required".to_string()))?;
        let _values = self.values.unwrap_or_default();

        Ok(None)
    }
}
