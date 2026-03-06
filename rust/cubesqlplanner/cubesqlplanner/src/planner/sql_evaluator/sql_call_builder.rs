use super::symbols::MemberSymbol;
use super::Compiler;
use super::{
    CubeRef, SqlCall, SqlCallDependency, SqlCallFilterGroupItem, SqlCallFilterParamsItem,
    SqlDependency, SymbolPath, SymbolPathType,
};
use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::member_sql::*;
use crate::cube_bridge::security_context::SecurityContext;
use crate::planner::sql_evaluator::TimeDimensionSymbol;
use crate::planner::GranularityHelper;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct SqlCallBuilder<'a> {
    compiler: &'a mut Compiler,
    cube_evaluator: Rc<dyn CubeEvaluator>,
    base_tools: Rc<dyn BaseTools>,
    security_context: Rc<dyn SecurityContext>,
}

impl<'a> SqlCallBuilder<'a> {
    pub fn new(
        compiler: &'a mut Compiler,
        cube_evaluator: Rc<dyn CubeEvaluator>,
        base_tools: Rc<dyn BaseTools>,
        security_context: Rc<dyn SecurityContext>,
    ) -> Self {
        Self {
            compiler,
            cube_evaluator,
            base_tools,
            security_context,
        }
    }

    pub fn build(
        mut self,
        cube_name: &String,
        member_sql: Rc<dyn MemberSql>,
    ) -> Result<SqlCall, CubeError> {
        let (template, template_args) = member_sql
            .compile_template_sql(self.base_tools.clone(), self.security_context.clone())?;

        let deps = template_args
            .symbol_paths
            .iter()
            .map(|path| self.build_dependency(cube_name, path))
            .collect::<Result<Vec<_>, _>>()?;

        let filter_params = template_args
            .filter_params
            .iter()
            .map(|itm| self.build_filter_params_item(itm))
            .collect::<Result<Vec<_>, _>>()?;

        let filter_groups = template_args
            .filter_groups
            .iter()
            .map(|itm| self.build_filter_group_item(itm))
            .collect::<Result<Vec<_>, _>>()?;

        let result = SqlCall::new(
            template.clone(),
            deps,
            filter_params,
            filter_groups,
            template_args.security_context.clone(),
        );
        Ok(result)
    }

    fn build_filter_params_item(
        &mut self,
        item: &FilterParamsItem,
    ) -> Result<SqlCallFilterParamsItem, CubeError> {
        Ok(SqlCallFilterParamsItem {
            filter_symbol_name: format!("{}.{}", item.cube_name, item.name),
            column: item.column.clone(),
        })
    }

    fn build_filter_group_item(
        &mut self,
        item: &FilterGroupItem,
    ) -> Result<SqlCallFilterGroupItem, CubeError> {
        let filter_params = item
            .filter_params
            .iter()
            .map(|itm| self.build_filter_params_item(itm))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SqlCallFilterGroupItem { filter_params })
    }

    fn build_dependency(
        &mut self,
        current_cube_name: &String,
        dep_path: &Vec<String>,
    ) -> Result<SqlCallDependency, CubeError> {
        assert!(!dep_path.is_empty());

        let symbol_path = SymbolPath::parse_parts(
            self.cube_evaluator.clone(),
            Some(current_cube_name),
            dep_path,
        )
        .map_err(|e| CubeError::user(format!("Error in `{}`: {}", dep_path.join("."), e)))?;

        let path = symbol_path.path().clone();

        match symbol_path.path_type() {
            SymbolPathType::Dimension => {
                let member = self
                    .compiler
                    .add_dimension_evaluator_by_path(symbol_path.clone())?;
                Ok(SqlCallDependency {
                    path,
                    symbol: SqlDependency::Symbol(member),
                })
            }
            SymbolPathType::Measure => {
                let member = self
                    .compiler
                    .add_measure_evaluator_by_path(symbol_path.clone())?;
                Ok(SqlCallDependency {
                    path,
                    symbol: SqlDependency::Symbol(member),
                })
            }
            SymbolPathType::Segment => {
                let member = self
                    .compiler
                    .add_segment_evaluator_by_path(symbol_path.clone())?;
                Ok(SqlCallDependency {
                    path,
                    symbol: SqlDependency::Symbol(member),
                })
            }
            SymbolPathType::CubeName => {
                let symbol = self
                    .compiler
                    .add_cube_name_evaluator(symbol_path.cube_name().clone())?;
                Ok(SqlCallDependency {
                    path: path.clone(),
                    symbol: SqlDependency::CubeRef(CubeRef::Name { symbol, path }),
                })
            }
            SymbolPathType::CubeTable => {
                let symbol = self
                    .compiler
                    .add_cube_table_evaluator(symbol_path.cube_name().clone())?;
                Ok(SqlCallDependency {
                    path: path.clone(),
                    symbol: SqlDependency::CubeRef(CubeRef::Table { symbol, path }),
                })
            }
        }
    }
}
