use super::{MemberSymbol, SymbolFactory};
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::measure_definition::{MeasureDefinition, TimeShiftReference};
use crate::cube_bridge::memeber_sql::{MemberSql, MemberSqlArg};
use crate::planner::sql_evaluator::{Compiler, Dependency, EvaluationNode};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct MeasureOrderBy {
    evaluation_node: Rc<EvaluationNode>,
    direction: String,
}

impl MeasureOrderBy {
    pub fn new(evaluation_node: Rc<EvaluationNode>, direction: String) -> Self {
        Self {
            evaluation_node,
            direction,
        }
    }

    pub fn evaluation_node(&self) -> &Rc<EvaluationNode> {
        &self.evaluation_node
    }

    pub fn direction(&self) -> &String {
        &self.direction
    }
}

pub struct MeasureSymbol {
    cube_name: String,
    name: String,
    definition: Rc<dyn MeasureDefinition>,
    measure_filters: Vec<Rc<EvaluationNode>>,
    measure_order_by: Vec<MeasureOrderBy>,
    member_sql: Rc<dyn MemberSql>,
}

impl MeasureSymbol {
    pub fn new(
        cube_name: String,
        name: String,
        member_sql: Rc<dyn MemberSql>,
        definition: Rc<dyn MeasureDefinition>,
        measure_filters: Vec<Rc<EvaluationNode>>,
        measure_order_by: Vec<MeasureOrderBy>,
    ) -> Self {
        Self {
            cube_name,
            name,
            member_sql,
            definition,
            measure_filters,
            measure_order_by,
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

    pub fn owned_by_cube(&self) -> bool {
        self.definition()
            .static_data()
            .owned_by_cube
            .unwrap_or(true)
    }

    pub fn measure_type(&self) -> &String {
        &self.definition.static_data().measure_type
    }

    pub fn measure_filters(&self) -> &Vec<Rc<EvaluationNode>> {
        &self.measure_filters
    }

    pub fn measure_order_by(&self) -> &Vec<MeasureOrderBy> {
        &self.measure_order_by
    }

    pub fn definition(&self) -> Rc<dyn MeasureDefinition> {
        self.definition.clone()
    }

    pub fn time_shift_references(&self) -> &Option<Vec<TimeShiftReference>> {
        &self.definition.static_data().time_shift_references
    }

    pub fn is_multi_stage(&self) -> bool {
        self.definition.static_data().multi_stage.unwrap_or(false)
    }
}

impl MemberSymbol for MeasureSymbol {
    fn cube_name(&self) -> &String {
        &self.cube_name
    }
    fn name(&self) -> &String {
        &self.name
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

impl SymbolFactory for MeasureSymbolFactory {
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
                let node = compiler.add_simple_sql_evaluator(cube_name.clone(), filter.sql()?)?;
                measure_filters.push(node);
            }
        }

        let mut measure_order_by = vec![];
        if let Some(group_by) = definition.order_by()? {
            for item in group_by.items().iter() {
                let node = compiler.add_simple_sql_evaluator(cube_name.clone(), item.sql()?)?;
                measure_order_by.push(MeasureOrderBy::new(node, item.dir()?));
            }
        }

        Ok(EvaluationNode::new_measure(
            MeasureSymbol::new(
                cube_name,
                name,
                sql,
                definition,
                measure_filters,
                measure_order_by,
            ),
            deps,
        ))
    }
}
