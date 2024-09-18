use super::dependecy::Dependency;
use super::{Compiler, MemberEvaluator, MemberEvaluatorFactory};
use super::{EvaluationNode, MemberEvaluatorType};
use crate::cube_bridge::dimension_definition::DimensionDefinition;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::memeber_sql::{self, MemberSql, MemberSqlArg};
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct JoinConditionEvaluator {
    cube_name: String,
    member_sql: Rc<dyn MemberSql>,
}

impl JoinConditionEvaluator {
    pub fn new(cube_name: String, member_sql: Rc<dyn MemberSql>) -> Self {
        Self {
            cube_name,
            member_sql,
        }
    }
    pub fn evaluate_sql(&self, args: Vec<MemberSqlArg>) -> Result<String, CubeError> {
        self.member_sql.call(args)
    }
    pub fn default_evaluate_sql(
        &self,
        args: Vec<MemberSqlArg>,
        tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        self.member_sql.call(args)
    }
}

impl MemberEvaluator for JoinConditionEvaluator {
    fn cube_name(&self) -> &String {
        &self.cube_name
    }
}

pub struct JoinConditionEvaluatorFactory {
    cube_name: String,
    sql: Rc<dyn MemberSql>,
}

impl JoinConditionEvaluatorFactory {
    pub fn try_new(
        cube_name: &String,
        sql: Rc<dyn MemberSql>,
        cube_evaluator: Rc<dyn CubeEvaluator>,
    ) -> Result<Self, CubeError> {
        Ok(Self {
            cube_name: cube_name.clone(),
            sql,
        })
    }
}

impl MemberEvaluatorFactory for JoinConditionEvaluatorFactory {
    fn evaluator_name() -> String {
        "join".to_string()
    }

    fn is_cachable() -> bool {
        false
    }

    fn cube_name(&self) -> &String {
        &self.cube_name
    }

    fn deps_names(&self) -> Result<Vec<String>, CubeError> {
        Ok(self.sql.args_names().clone())
    }

    fn member_sql(&self) -> Option<Rc<dyn MemberSql>> {
        Some(self.sql.clone())
    }

    fn build(
        self,
        deps: Vec<Dependency>,
        _compiler: &mut Compiler,
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        let Self { cube_name, sql } = self;
        Ok(EvaluationNode::new_join_condition(
            JoinConditionEvaluator::new(cube_name, sql),
            deps,
        ))
    }
}
