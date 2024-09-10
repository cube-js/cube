use super::dependecy::Dependency;
use super::{evaluate_sql, MemberEvaluator, MemberEvaluatorFactory};
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::measure_definition::MeasureDefinition;
use crate::cube_bridge::memeber_sql::MemberSql;
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct MeasureEvaluator {
    cube_name: String,
    name: String,
    definition: Rc<dyn MeasureDefinition>,
    member_sql: Rc<dyn MemberSql>,

    deps: Vec<Dependency>,
}

impl MeasureEvaluator {
    pub fn new(
        cube_name: String,
        name: String,
        member_sql: Rc<dyn MemberSql>,
        definition: Rc<dyn MeasureDefinition>,
        deps: Vec<Dependency>,
    ) -> Rc<Self> {
        Rc::new(Self {
            cube_name,
            name,
            member_sql,
            definition,
            deps,
        })
    }

    fn is_calculated(&self) -> bool {
        match self.definition.static_data().measure_type.as_str() {
            "number" | "string" | "time" | "boolean" => true,
            _ => false,
        }
    }
}

impl MemberEvaluator for MeasureEvaluator {
    fn evaluate(&self, tools: Rc<QueryTools>) -> Result<String, CubeError> {
        let primary_keys = tools
            .cube_evaluator()
            .static_data()
            .primary_keys
            .get(&self.cube_name)
            .unwrap();

        let sql = tools.auto_prefix_with_cube_name(
            &self.cube_name,
            &evaluate_sql(tools.clone(), self.member_sql.clone(), &self.deps)?,
        );

        if self.is_calculated() {
            Ok(sql)
        } else {
            let measure_type = &self.definition.static_data().measure_type;
            Ok(format!("{}({})", measure_type, sql))
        }
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }
}

pub struct MeasureEvaluatorFactory {
    cube_name: String,
    name: String,
    sql: Rc<dyn MemberSql>,
    definition: Rc<dyn MeasureDefinition>,
}

impl MemberEvaluatorFactory for MeasureEvaluatorFactory {
    type Result = MeasureEvaluator;

    fn try_new(
        full_name: String,
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

    fn build(self, deps: Vec<Dependency>) -> Result<Rc<Self::Result>, CubeError> {
        let Self {
            cube_name,
            name,
            sql,
            definition,
        } = self;
        Ok(MeasureEvaluator::new(
            cube_name, name, sql, definition, deps,
        ))
    }
}
