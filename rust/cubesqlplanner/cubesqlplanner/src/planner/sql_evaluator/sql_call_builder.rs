use super::symbols::MemberSymbol;
use super::Compiler;
use super::{SqlCall, SqlCallDependency, SqlCallFilterGroupItem, SqlCallFilterParamsItem};
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

        let result = SqlCall::builder()
            .template(template.clone())
            .deps(deps)
            .filter_params(filter_params)
            .filter_groups(filter_groups)
            .security_context(template_args.security_context.clone())
            .build();
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

        self.process_dependency_item(current_cube_name, dep_path, vec![])
            .map_err(|e| CubeError::user(format!("Error in `{}`: {}", dep_path.join("."), e)))
    }

    fn process_dependency_item(
        &mut self,
        current_cube_name: &String,
        path_tail: &[String],
        processed_path: Vec<String>,
    ) -> Result<SqlCallDependency, CubeError> {
        assert!(!path_tail.is_empty());
        if let Some(member) = self.try_process_member_dependency_item(
            current_cube_name,
            path_tail,
            processed_path.clone(),
        ) {
            Ok(member)
        } else if let Some(cube_name) = self.get_cube_name(&current_cube_name, &path_tail[0])? {
            self.process_cube_dependency_item(&cube_name, path_tail, processed_path)
        } else {
            Err(CubeError::user(format!(
                "Undefined property {}",
                path_tail[0]
            )))
        }
    }

    fn try_process_member_dependency_item(
        &mut self,
        current_cube_name: &String,
        path_tail: &[String],
        processed_path: Vec<String>,
    ) -> Option<SqlCallDependency> {
        if let Ok(member_symbol) = self.build_evaluator(&current_cube_name, &path_tail[0]) {
            if let Ok(dimension) = member_symbol.as_dimension() {
                if dimension.dimension_type() == "time" && path_tail.len() == 2 {
                    let granularity = &path_tail[1];
                    if let Ok(Some(granularity_obj)) = GranularityHelper::make_granularity_obj(
                        self.cube_evaluator.clone(),
                        self.compiler,
                        &current_cube_name,
                        &path_tail[0],
                        Some(granularity.clone()),
                    ) {
                        let time_dim_symbol =
                            MemberSymbol::new_time_dimension(TimeDimensionSymbol::new(
                                member_symbol,
                                Some(granularity.clone()),
                                Some(granularity_obj),
                                None,
                            ));
                        let result = SqlCallDependency {
                            path: processed_path,
                            symbol: time_dim_symbol,
                        };
                        return Some(result);
                    } else {
                        return None;
                    }
                }
            }
            if path_tail.len() > 1 {
                return None;
            }
            let result = SqlCallDependency {
                path: processed_path,
                symbol: member_symbol,
            };
            Some(result)
        } else {
            None
        }
    }

    fn process_cube_dependency_item(
        &mut self,
        cube_name: &String,
        path_tail: &[String],
        mut processed_path: Vec<String>,
    ) -> Result<SqlCallDependency, CubeError> {
        processed_path.push(cube_name.clone());
        if path_tail.len() == 1 {
            let result = SqlCallDependency {
                path: processed_path,
                symbol: self.compiler.add_cube_name_evaluator(cube_name.clone())?,
            };
            return Ok(result);
        }
        if path_tail.len() == 2 && path_tail[1] == "__sql_fn" {
            let result = SqlCallDependency {
                path: processed_path,
                symbol: self.compiler.add_cube_table_evaluator(cube_name.clone())?,
            };
            return Ok(result);
        }
        self.process_dependency_item(&cube_name, &path_tail[1..], processed_path)
    }

    fn get_cube_name(
        &mut self,
        current_cube: &String,
        cube_name: &String,
    ) -> Result<Option<String>, CubeError> {
        if self.is_current_cube(cube_name) {
            Ok(Some(current_cube.clone()))
        } else if self.cube_evaluator.cube_exists(cube_name.clone())? {
            Ok(Some(cube_name.clone()))
        } else {
            Ok(None)
        }
    }

    fn is_current_cube(&self, name: &str) -> bool {
        match name {
            "CUBE" | "TABLE" => true,
            _ => false,
        }
    }

    fn build_evaluator(
        &mut self,
        current_cube_name: &String,
        name: &String,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let dep_full_name = format!("{}.{}", current_cube_name, name);
        let res = self
            .compiler
            .add_auto_resolved_member_evaluator(dep_full_name.clone())?;
        Ok(res)
    }
}
