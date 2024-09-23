use super::dependecy::Dependency;
use super::{default_visitor::DefaultEvaluatorVisitor, EvaluationNode};
use super::{Compiler, MemberSymbol, MemberSymbolFactory};
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::measure_definition::MeasureDefinition;
use crate::cube_bridge::memeber_sql::{MemberSql, MemberSqlArg};
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct MeasureSymbol {
    cube_name: String,
    name: String,
    definition: Rc<dyn MeasureDefinition>,
    measure_filters: Vec<Rc<EvaluationNode>>,
    member_sql: Rc<dyn MemberSql>,
}

impl MeasureSymbol {
    pub fn new(
        cube_name: String,
        name: String,
        member_sql: Rc<dyn MemberSql>,
        definition: Rc<dyn MeasureDefinition>,
        measure_filters: Vec<Rc<EvaluationNode>>,
    ) -> Self {
        Self {
            cube_name,
            name,
            member_sql,
            definition,
            measure_filters,
        }
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.cube_name, self.name)
    }

    pub fn is_calculated(&self) -> bool {
        match self.definition.static_data().measure_type.as_str() {
            "number" | "string" | "time" | "boolean" => true,
            _ => false,
        }
    }

    pub fn evaluate_sql(&self, args: Vec<MemberSqlArg>) -> Result<String, CubeError> {
        let sql = self.member_sql.call(args)?;
        Ok(sql)
    }

    pub fn measure_type(&self) -> &String {
        &self.definition.static_data().measure_type
    }

    pub fn measure_filters(&self) -> &Vec<Rc<EvaluationNode>> {
        &self.measure_filters
    }

    pub fn default_evaluate_sql(
        &self,
        visitor: &DefaultEvaluatorVisitor,
        args: Vec<MemberSqlArg>,
        tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        let sql = tools.auto_prefix_with_cube_name(
            &self.cube_name,
            &self.member_sql.call(args)?,
            visitor.cube_alias_prefix(),
        );
        if self.is_calculated() {
            Ok(sql)
        } else {
            let measure_type = &self.definition.static_data().measure_type;
            Ok(format!("{}({})", measure_type, sql))
        }
    }
}

impl MemberSymbol for MeasureSymbol {
    fn cube_name(&self) -> &String {
        &self.cube_name
    }
}

pub struct MeasureFilterSymbol {
    cube_name: String,
    member_sql: Rc<dyn MemberSql>,
}

impl MeasureFilterSymbol {
    pub fn new(cube_name: String, member_sql: Rc<dyn MemberSql>) -> Self {
        Self {
            cube_name,
            member_sql,
        }
    }

    pub fn full_name(&self) -> String {
        format!("{}.measure_filter", self.cube_name)
    }

    pub fn evaluate_sql(&self, args: Vec<MemberSqlArg>) -> Result<String, CubeError> {
        let sql = self.member_sql.call(args)?;
        Ok(sql)
    }
}

impl MemberSymbol for MeasureFilterSymbol {
    fn cube_name(&self) -> &String {
        &self.cube_name
    }
}

pub struct MeasureSymbolFactory {
    cube_name: String,
    name: String,
    sql: Rc<dyn MemberSql>,
    definition: Rc<dyn MeasureDefinition>,
}

impl MeasureSymbolFactory {
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

impl MemberSymbolFactory for MeasureSymbolFactory {
    fn symbol_name() -> String {
        "measure".to_string()
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

    fn build(
        self,
        deps: Vec<Dependency>,
        compiler: &mut Compiler,
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        let Self {
            cube_name,
            name,
            sql,
            definition,
        } = self;
        let mut measure_filters = vec![];
        if let Some(filters) = definition.filters()? {
            for filter in filters.items().iter() {
                let node =
                    compiler.add_measure_filter_evaluator(cube_name.clone(), filter.sql()?)?;
                measure_filters.push(node);
            }
        }

        Ok(EvaluationNode::new_measure(
            MeasureSymbol::new(cube_name, name, sql, definition, measure_filters),
            deps,
        ))
    }
}

pub struct MeasureFilterSymbolFactory {
    cube_name: String,
    sql: Rc<dyn MemberSql>,
}

impl MeasureFilterSymbolFactory {
    pub fn try_new(cube_name: &String, sql: Rc<dyn MemberSql>) -> Result<Self, CubeError> {
        Ok(Self {
            cube_name: cube_name.clone(),
            sql,
        })
    }
}

impl MemberSymbolFactory for MeasureFilterSymbolFactory {
    fn is_cachable() -> bool {
        false
    }
    fn symbol_name() -> String {
        "measure_filter".to_string()
    }
    fn cube_name(&self) -> &String {
        &self.cube_name
    }

    fn member_sql(&self) -> Option<Rc<dyn MemberSql>> {
        Some(self.sql.clone())
    }

    fn deps_names(&self) -> Result<Vec<String>, CubeError> {
        Ok(self.sql.args_names().clone())
    }

    fn build(
        self,
        deps: Vec<Dependency>,
        _compiler: &mut Compiler,
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        let Self { cube_name, sql } = self;
        Ok(EvaluationNode::new_measure_filter(
            MeasureFilterSymbol::new(cube_name, sql),
            deps,
        ))
    }
}
