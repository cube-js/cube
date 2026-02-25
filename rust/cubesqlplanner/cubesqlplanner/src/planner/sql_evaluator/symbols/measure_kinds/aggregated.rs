use super::super::super::MemberSymbol;
use super::super::common::AggregationType;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{sql_nodes::SqlNode, SqlCall, SqlEvaluatorVisitor};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub struct AggregatedMeasure {
    agg_type: AggregationType,
    member_sql: Rc<SqlCall>,
}

impl AggregatedMeasure {
    pub fn new(agg_type: AggregationType, member_sql: Rc<SqlCall>) -> Self {
        Self {
            agg_type,
            member_sql,
        }
    }

    pub fn agg_type(&self) -> AggregationType {
        self.agg_type
    }

    pub fn member_sql(&self) -> &Rc<SqlCall> {
        &self.member_sql
    }

    pub fn evaluate_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        self.member_sql
            .eval(visitor, node_processor, query_tools, templates)
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = vec![];
        self.member_sql.extract_symbol_deps(&mut deps);
        deps
    }

    pub fn get_dependencies_with_path(&self) -> Vec<(Rc<MemberSymbol>, Vec<String>)> {
        let mut deps = vec![];
        self.member_sql.extract_symbol_deps_with_path(&mut deps);
        deps
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        Ok(Self {
            agg_type: self.agg_type,
            member_sql: self.member_sql.apply_recursive(f)?,
        })
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        Box::new(std::iter::once(&self.member_sql))
    }

    pub fn is_owned_by_cube(&self) -> bool {
        self.member_sql.is_owned_by_cube()
    }
}
