use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{
    MemberExpressionExpression, MemberExpressionSymbol, MemberSymbol, SqlCall,
};
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{evaluate_with_context, VisitorContext};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct BaseSegment {
    full_name: String,
    member_evaluator: Rc<MemberSymbol>,
    cube_name: String,
    name: String,
}

impl PartialEq for BaseSegment {
    fn eq(&self, other: &Self) -> bool {
        self.full_name == other.full_name
    }
}

impl BaseSegment {
    pub fn try_new(
        expression: Rc<SqlCall>,
        cube_name: String,
        name: String,
        full_name: Option<String>,
        query_tools: Rc<QueryTools>,
    ) -> Result<Rc<Self>, CubeError> {
        let member_expression_symbol = MemberExpressionSymbol::try_new(
            cube_name.clone(),
            name.clone(),
            MemberExpressionExpression::SqlCall(expression),
            None,
            query_tools.base_tools().clone(),
        )?;
        let full_name = full_name.unwrap_or(member_expression_symbol.full_name());
        let member_evaluator = MemberSymbol::new_member_expression(member_expression_symbol);

        Ok(Rc::new(Self {
            full_name,
            member_evaluator,
            cube_name,
            name,
        }))
    }
    pub fn to_sql(
        &self,
        context: Rc<VisitorContext>,
        plan_templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        evaluate_with_context(&self.member_evaluator, context, plan_templates)
    }

    pub fn full_name(&self) -> String {
        self.full_name.clone()
    }

    pub fn member_evaluator(&self) -> Rc<MemberSymbol> {
        self.member_evaluator.clone()
    }

    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }

    pub fn name(&self) -> &String {
        &self.name
    }
}
