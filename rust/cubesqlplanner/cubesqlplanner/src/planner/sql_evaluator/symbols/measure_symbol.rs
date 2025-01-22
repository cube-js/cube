use super::{MemberSymbol, SymbolFactory};
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::measure_definition::{
    MeasureDefinition, RollingWindow, TimeShiftReference,
};
use crate::cube_bridge::member_sql::MemberSql;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{sql_nodes::SqlNode, Compiler, SqlCall, SqlEvaluatorVisitor};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub struct MeasureOrderBy {
    sql_call: Rc<SqlCall>,
    direction: String,
}

impl MeasureOrderBy {
    pub fn new(sql_call: Rc<SqlCall>, direction: String) -> Self {
        Self {
            sql_call,
            direction,
        }
    }

    pub fn sql_call(&self) -> &Rc<SqlCall> {
        &self.sql_call
    }

    pub fn direction(&self) -> &String {
        &self.direction
    }
}

#[derive(Clone)]
pub struct MeasureSymbol {
    cube_name: String,
    name: String,
    definition: Rc<dyn MeasureDefinition>,
    measure_filters: Vec<Rc<SqlCall>>,
    measure_drill_filters: Vec<Rc<SqlCall>>,
    measure_order_by: Vec<MeasureOrderBy>,
    member_sql: Option<Rc<SqlCall>>,
    pk_sqls: Vec<Rc<SqlCall>>,
    is_splitted_source: bool,
}

impl MeasureSymbol {
    pub fn new(
        cube_name: String,
        name: String,
        member_sql: Option<Rc<SqlCall>>,
        pk_sqls: Vec<Rc<SqlCall>>,
        definition: Rc<dyn MeasureDefinition>,
        measure_filters: Vec<Rc<SqlCall>>,
        measure_drill_filters: Vec<Rc<SqlCall>>,
        measure_order_by: Vec<MeasureOrderBy>,
    ) -> Self {
        Self {
            cube_name,
            name,
            member_sql,
            pk_sqls,
            definition,
            measure_filters,
            measure_drill_filters,
            measure_order_by,
            is_splitted_source: false,
        }
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.cube_name, self.name)
    }

    pub fn is_splitted_source(&self) -> bool {
        self.is_splitted_source
    }

    pub fn pk_sqls(&self) -> &Vec<Rc<SqlCall>> {
        &self.pk_sqls
    }

    pub fn is_calculated(&self) -> bool {
        match self.definition.static_data().measure_type.as_str() {
            "number" | "string" | "time" | "boolean" => true,
            _ => false,
        }
    }

    pub fn evaluate_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        if let Some(member_sql) = &self.member_sql {
            let sql = member_sql.eval(visitor, node_processor, query_tools, templates)?;
            Ok(sql)
        } else {
            Err(CubeError::internal(format!(
                "Measure {} hasn't sql evaluator",
                self.full_name()
            )))
        }
    }

    pub fn has_sql(&self) -> bool {
        self.member_sql.is_some()
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = vec![];
        if let Some(member_sql) = &self.member_sql {
            member_sql.extract_symbol_deps(&mut deps);
        }
        for pk in self.pk_sqls.iter() {
            pk.extract_symbol_deps(&mut deps);
        }
        for filter in self.measure_filters.iter() {
            filter.extract_symbol_deps(&mut deps);
        }
        for filter in self.measure_drill_filters.iter() {
            filter.extract_symbol_deps(&mut deps);
        }
        for order in self.measure_order_by.iter() {
            order.sql_call().extract_symbol_deps(&mut deps);
        }
        deps
    }

    pub fn get_dependencies_with_path(&self) -> Vec<(Rc<MemberSymbol>, Vec<String>)> {
        let mut deps = vec![];
        if let Some(member_sql) = &self.member_sql {
            member_sql.extract_symbol_deps_with_path(&mut deps);
        }
        for pk in self.pk_sqls.iter() {
            pk.extract_symbol_deps_with_path(&mut deps);
        }
        for filter in self.measure_filters.iter() {
            filter.extract_symbol_deps_with_path(&mut deps);
        }
        for filter in self.measure_drill_filters.iter() {
            filter.extract_symbol_deps_with_path(&mut deps);
        }
        for order in self.measure_order_by.iter() {
            order.sql_call().extract_symbol_deps_with_path(&mut deps);
        }
        deps
    }

    pub fn get_dependent_cubes(&self) -> Vec<String> {
        let mut cubes = vec![];
        if let Some(member_sql) = &self.member_sql {
            member_sql.extract_cube_deps(&mut cubes);
        }
        for pk in self.pk_sqls.iter() {
            pk.extract_cube_deps(&mut cubes);
        }
        for filter in self.measure_filters.iter() {
            filter.extract_cube_deps(&mut cubes);
        }
        for filter in self.measure_drill_filters.iter() {
            filter.extract_cube_deps(&mut cubes);
        }
        for order in self.measure_order_by.iter() {
            order.sql_call().extract_cube_deps(&mut cubes);
        }
        cubes
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

    pub fn rolling_window(&self) -> &Option<RollingWindow> {
        &self.definition.static_data().rolling_window
    }

    pub fn is_rolling_window(&self) -> bool {
        self.rolling_window().is_some()
    }

    pub fn is_running_total(&self) -> bool {
        self.measure_type() == "runningTotal"
    }

    pub fn is_cumulative(&self) -> bool {
        self.is_rolling_window() || self.is_running_total()
    }

    pub fn measure_filters(&self) -> &Vec<Rc<SqlCall>> {
        &self.measure_filters
    }

    pub fn measure_drill_filters(&self) -> &Vec<Rc<SqlCall>> {
        &self.measure_drill_filters
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

    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }
    pub fn name(&self) -> &String {
        &self.name
    }
}

pub struct MeasureSymbolFactory {
    cube_name: String,
    name: String,
    sql: Option<Rc<dyn MemberSql>>,
    definition: Rc<dyn MeasureDefinition>,
    cube_evaluator: Rc<dyn CubeEvaluator>,
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
        let sql = definition.sql()?;
        Ok(Self {
            cube_name,
            name,
            sql,
            definition,
            cube_evaluator,
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
        self.sql.clone()
    }

    fn deps_names(&self) -> Result<Vec<String>, CubeError> {
        if let Some(member_sql) = &self.sql {
            Ok(member_sql.args_names().clone())
        } else {
            Ok(vec![])
        }
    }

    fn build(self, compiler: &mut Compiler) -> Result<Rc<MemberSymbol>, CubeError> {
        let Self {
            cube_name,
            name,
            sql,
            definition,
            cube_evaluator,
        } = self;
        let pk_sqls = if sql.is_none() {
            cube_evaluator
                .static_data()
                .primary_keys
                .get(&cube_name)
                .cloned()
                .unwrap_or_else(|| vec![])
                .into_iter()
                .map(|primary_key| -> Result<_, CubeError> {
                    let key_dimension_name = format!("{}.{}", cube_name, primary_key);
                    let key_dimension =
                        cube_evaluator.dimension_by_path(key_dimension_name.clone())?;
                    let key_dimension_sql = if let Some(key_dimension_sql) = key_dimension.sql()? {
                        Ok(key_dimension_sql)
                    } else {
                        Err(CubeError::internal(format!(
                            "Key dimension {} hasn't sql evaluator",
                            key_dimension_name
                        )))
                    }?;
                    compiler.compile_sql_call(&cube_name, key_dimension_sql)
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            vec![]
        };
        let mut measure_filters = vec![];
        if let Some(filters) = definition.filters()? {
            for filter in filters.iter() {
                let node = compiler.compile_sql_call(&cube_name, filter.sql()?)?;
                measure_filters.push(node);
            }
        }

        let mut measure_drill_filters = vec![];
        if let Some(filters) = definition.drill_filters()? {
            for filter in filters.iter() {
                let node = compiler.compile_sql_call(&cube_name, filter.sql()?)?;
                measure_drill_filters.push(node);
            }
        }

        let mut measure_order_by = vec![];
        if let Some(group_by) = definition.order_by()? {
            for item in group_by.iter() {
                let node = compiler.compile_sql_call(&cube_name, item.sql()?)?;
                measure_order_by.push(MeasureOrderBy::new(node, item.dir()?));
            }
        }
        let sql = if let Some(sql) = sql {
            Some(compiler.compile_sql_call(&cube_name, sql)?)
        } else {
            None
        };

        Ok(MemberSymbol::new_measure(MeasureSymbol::new(
            cube_name,
            name,
            sql,
            pk_sqls,
            definition,
            measure_filters,
            measure_drill_filters,
            measure_order_by,
        )))
    }
}
