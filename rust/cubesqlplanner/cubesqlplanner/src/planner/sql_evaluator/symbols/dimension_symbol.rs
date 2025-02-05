use super::{MemberSymbol, SymbolFactory};
use crate::cube_bridge::dimension_definition::DimensionDefinition;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::member_sql::MemberSql;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{sql_nodes::SqlNode, Compiler, SqlCall, SqlEvaluatorVisitor};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct DimensionSymbol {
    cube_name: String,
    name: String,
    member_sql: Option<Rc<SqlCall>>,
    latitude: Option<Rc<SqlCall>>,
    longitude: Option<Rc<SqlCall>>,
    #[allow(dead_code)]
    definition: Rc<dyn DimensionDefinition>,
}

impl DimensionSymbol {
    pub fn new(
        cube_name: String,
        name: String,
        member_sql: Option<Rc<SqlCall>>,
        latitude: Option<Rc<SqlCall>>,
        longitude: Option<Rc<SqlCall>>,
        definition: Rc<dyn DimensionDefinition>,
    ) -> Self {
        Self {
            cube_name,
            name,
            member_sql,
            latitude,
            longitude,
            definition,
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
                "Dimension {} hasn't sql evaluator",
                self.full_name()
            )))
        }
    }

    pub fn latitude(&self) -> Option<Rc<SqlCall>> {
        self.latitude.clone()
    }

    pub fn longitude(&self) -> Option<Rc<SqlCall>> {
        self.longitude.clone()
    }

    pub fn member_sql(&self) -> &Option<Rc<SqlCall>> {
        &self.member_sql
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.cube_name, self.name)
    }

    pub fn owned_by_cube(&self) -> bool {
        self.definition.static_data().owned_by_cube.unwrap_or(true)
    }

    pub fn is_multi_stage(&self) -> bool {
        self.definition.static_data().multi_stage.unwrap_or(false)
    }

    pub fn is_sub_query(&self) -> bool {
        self.definition.static_data().sub_query.unwrap_or(false)
    }

    pub fn dimension_type(&self) -> &String {
        &self.definition.static_data().dimension_type
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = vec![];
        if let Some(member_sql) = &self.member_sql {
            member_sql.extract_symbol_deps(&mut deps);
        }
        if let Some(member_sql) = &self.latitude {
            member_sql.extract_symbol_deps(&mut deps);
        }
        if let Some(member_sql) = &self.longitude {
            member_sql.extract_symbol_deps(&mut deps);
        }
        deps
    }

    pub fn get_dependencies_with_path(&self) -> Vec<(Rc<MemberSymbol>, Vec<String>)> {
        let mut deps = vec![];
        if let Some(member_sql) = &self.member_sql {
            member_sql.extract_symbol_deps_with_path(&mut deps);
        }
        if let Some(member_sql) = &self.latitude {
            member_sql.extract_symbol_deps_with_path(&mut deps);
        }
        if let Some(member_sql) = &self.longitude {
            member_sql.extract_symbol_deps_with_path(&mut deps);
        }
        deps
    }

    pub fn get_dependent_cubes(&self) -> Vec<String> {
        let mut cubes = vec![];
        if let Some(member_sql) = &self.member_sql {
            member_sql.extract_cube_deps(&mut cubes);
        }
        cubes
    }

    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }

    pub fn definition(&self) -> &Rc<dyn DimensionDefinition> {
        &self.definition
    }

    pub fn name(&self) -> &String {
        &self.name
    }
}

pub struct DimensionSymbolFactory {
    cube_name: String,
    name: String,
    sql: Option<Rc<dyn MemberSql>>,
    definition: Rc<dyn DimensionDefinition>,
}

impl DimensionSymbolFactory {
    pub fn try_new(
        full_name: &String,
        cube_evaluator: Rc<dyn CubeEvaluator>,
    ) -> Result<Self, CubeError> {
        let mut iter = cube_evaluator
            .parse_path("dimensions".to_string(), full_name.clone())?
            .into_iter();
        let cube_name = iter.next().unwrap();
        let name = iter.next().unwrap();
        let definition = cube_evaluator.dimension_by_path(full_name.clone())?;
        Ok(Self {
            cube_name,
            name,
            sql: definition.sql()?,
            definition,
        })
    }
}

impl SymbolFactory for DimensionSymbolFactory {
    fn symbol_name() -> String {
        "dimension".to_string()
    }

    fn cube_name(&self) -> &String {
        &self.cube_name
    }

    fn deps_names(&self) -> Result<Vec<String>, CubeError> {
        if let Some(member_sql) = &self.sql {
            Ok(member_sql.args_names().clone())
        } else {
            Ok(vec![])
        }
    }

    fn member_sql(&self) -> Option<Rc<dyn MemberSql>> {
        self.sql.clone()
    }

    fn build(self, compiler: &mut Compiler) -> Result<Rc<MemberSymbol>, CubeError> {
        let Self {
            cube_name,
            name,
            sql,
            definition,
        } = self;
        let sql = if let Some(sql) = sql {
            Some(compiler.compile_sql_call(&cube_name, sql)?)
        } else {
            None
        };
        let (latitude, longitude) = if definition.static_data().dimension_type == "geo" {
            if let (Some(latitude_item), Some(longitude_item)) =
                (definition.latitude()?, definition.longitude()?)
            {
                let latitude = compiler.compile_sql_call(&cube_name, latitude_item.sql()?)?;
                let longitude = compiler.compile_sql_call(&cube_name, longitude_item.sql()?)?;
                (Some(latitude), Some(longitude))
            } else {
                return Err(CubeError::user(format!(
                    "Geo dimension '{}.{}'must have latitude and longitude",
                    cube_name, name
                )));
            }
        } else {
            (None, None)
        };
        Ok(MemberSymbol::new_dimension(DimensionSymbol::new(
            cube_name, name, sql, latitude, longitude, definition,
        )))
    }
}
