use super::dependecy::Dependency;
use super::{EvaluationNode, MemberEvaluatorType};
use super::{MemberEvaluator, MemberEvaluatorFactory};
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::measure_definition::MeasureDefinition;
use crate::cube_bridge::memeber_sql::{MemberSql, MemberSqlArg};
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct MeasureEvaluator {
    cube_name: String,
    name: String,
    definition: Rc<dyn MeasureDefinition>,
    member_sql: Rc<dyn MemberSql>,
}

impl MeasureEvaluator {
    pub fn new(
        cube_name: String,
        name: String,
        member_sql: Rc<dyn MemberSql>,
        definition: Rc<dyn MeasureDefinition>,
    ) -> Self {
        Self {
            cube_name,
            name,
            member_sql,
            definition,
        }
    }

    fn is_calculated(&self) -> bool {
        match self.definition.static_data().measure_type.as_str() {
            "number" | "string" | "time" | "boolean" => true,
            _ => false,
        }
    }

    pub fn default_evaluate_sql(
        &self,
        args: Vec<MemberSqlArg>,
        tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        let sql = tools.auto_prefix_with_cube_name(&self.cube_name, &self.member_sql.call(args)?);
        if self.is_calculated() {
            Ok(sql)
        } else {
            let measure_type = &self.definition.static_data().measure_type;
            Ok(format!("{}({})", measure_type, sql))
        }
    }
}

impl MemberEvaluator for MeasureEvaluator {
    fn cube_name(&self) -> &String {
        &self.cube_name
    }
}

pub struct MeasureEvaluatorFactory {
    cube_name: String,
    name: String,
    sql: Rc<dyn MemberSql>,
    definition: Rc<dyn MeasureDefinition>,
}

impl MeasureEvaluatorFactory {
    pub fn try_new(
        full_name: &String,
        cube_evaluator: Rc<dyn CubeEvaluator>,
    ) -> Result<Self, CubeError> {
        let mut iter = cube_evaluator
            .parse_path("measures".to_string(), full_name.clone())?
            .into_iter();
        let cube_name = iter.next().unwrap();
        let name = iter.next().unwrap();
        let definition = cube_evaluator.measure_by_path(full_name.clone())?;
        let sql = if let Some(sql) = definition.sql()? {
            sql
        } else {
            let primary_keys = cube_evaluator
                .static_data()
                .primary_keys
                .get(&cube_name)
                .unwrap();
            let primary_key = primary_keys.first().unwrap();
            let key_dimension =
                cube_evaluator.dimension_by_path(format!("{}.{}", cube_name, primary_key))?;
            key_dimension.sql()?
        };
        Ok(Self {
            cube_name,
            name,
            sql,
            definition,
        })
    }
}

impl MemberEvaluatorFactory for MeasureEvaluatorFactory {
    fn cube_name(&self) -> &String {
        &self.cube_name
    }

    fn member_sql(&self) -> Option<Rc<dyn MemberSql>> {
        Some(self.sql.clone())
    }

    fn deps_names(&self) -> Result<Vec<String>, CubeError> {
        if let Some(member_sql) = self.definition.sql()? {
            Ok(member_sql.args_names().clone())
        } else {
            Ok(vec![])
        }
    }

    fn build(self, deps: Vec<Dependency>) -> Result<Rc<EvaluationNode>, CubeError> {
        let Self {
            cube_name,
            name,
            sql,
            definition,
        } = self;
        Ok(EvaluationNode::new_measure(
            MeasureEvaluator::new(cube_name, name, sql, definition),
            deps,
        ))
    }
}
