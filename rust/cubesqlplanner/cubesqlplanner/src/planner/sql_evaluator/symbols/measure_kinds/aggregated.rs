use super::super::super::MemberSymbol;
use super::super::common::AggregationType;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{sql_nodes::SqlNode, CubeRef, SqlCall, SqlEvaluatorVisitor};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub struct AggregatedMeasure {
    agg_type: AggregationType,
    member_sql: Option<Rc<SqlCall>>,
}

impl AggregatedMeasure {
    pub fn new(agg_type: AggregationType, member_sql: Rc<SqlCall>) -> Self {
        Self {
            agg_type,
            member_sql: Some(member_sql),
        }
    }

    pub fn new_without_sql(agg_type: AggregationType) -> Self {
        Self {
            agg_type,
            member_sql: None,
        }
    }

    pub fn agg_type(&self) -> AggregationType {
        self.agg_type
    }

    pub fn member_sql(&self) -> Option<&Rc<SqlCall>> {
        self.member_sql.as_ref()
    }

    pub fn evaluate_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        match &self.member_sql {
            Some(sql) => sql.eval(visitor, node_processor, query_tools, templates),
            None => Err(CubeError::internal(
                "Aggregated measure without sql cannot be evaluated directly".to_string(),
            )),
        }
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = vec![];
        if let Some(sql) = &self.member_sql {
            sql.extract_symbol_deps(&mut deps);
        }
        deps
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        Ok(Self {
            agg_type: self.agg_type,
            member_sql: self
                .member_sql
                .as_ref()
                .map(|sql| sql.apply_recursive(f))
                .transpose()?,
        })
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        Box::new(self.member_sql.iter())
    }

    pub fn get_cube_refs(&self) -> Vec<CubeRef> {
        let mut refs = vec![];
        if let Some(sql) = &self.member_sql {
            sql.extract_cube_refs(&mut refs);
        }
        refs
    }

    pub fn is_owned_by_cube(&self) -> bool {
        self.member_sql
            .as_ref()
            .is_some_and(|sql| sql.is_owned_by_cube())
    }
}
