use super::Compiler;
use super::{
    CubeRef, SqlCall, SqlCallFilterGroupItem, SqlCallFilterParamsItem, SqlDependency, SymbolPath,
    SymbolPathType,
};
use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::member_sql::*;
use crate::cube_bridge::security_context::SecurityContext;
use crate::planner::collectors::collect_cube_names;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Builds a `SqlCall` from a `MemberSql` declaration: compiles the
/// template, resolves each path placeholder to a `SqlDependency`
/// (member symbol or cube reference), and attaches filter params,
/// filter groups and security-context bindings.
pub struct SqlCallBuilder<'a> {
    compiler: &'a mut Compiler,
    cube_evaluator: Rc<dyn CubeEvaluator>,
    base_tools: Rc<dyn BaseTools>,
    security_context: Rc<dyn SecurityContext>,
    /// Set while compiling a cube's own `sql`. That sql builds the table the
    /// query reads from, so no member is in scope inside it — a member
    /// reference there is rejected instead of resolved, wherever in the sql or
    /// in one of its `FILTER_PARAMS` columns it appears.
    is_cube_sql: bool,
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
            is_cube_sql: false,
        }
    }

    pub fn for_cube_sql(mut self) -> Self {
        self.is_cube_sql = true;
        self
    }

    pub fn build(
        mut self,
        cube_name: &String,
        member_sql: Rc<dyn MemberSql>,
    ) -> Result<SqlCall, CubeError> {
        let compiled = self.base_tools.compile_member_sql(
            member_sql.clone(),
            self.security_context.clone(),
            member_sql.args_names().clone(),
        )?;
        self.build_from_template(cube_name, compiled.template, &compiled.args)
    }

    // Assembles a `SqlCall` from an already-compiled template and the
    // dependencies it recorded. Recurses for a `FILTER_PARAMS` column that came
    // back compiled, since such a column is a call of its own with its own
    // dependency list.
    fn build_from_template(
        &mut self,
        cube_name: &String,
        template: SqlTemplate,
        args: &SqlTemplateArgs,
    ) -> Result<SqlCall, CubeError> {
        let deps = args
            .symbol_paths
            .iter()
            .map(|path| self.build_dependency(cube_name, path))
            .collect::<Result<Vec<_>, _>>()?;

        let filter_params = args
            .filter_params
            .iter()
            .map(|itm| self.build_filter_params_item(cube_name, itm))
            .collect::<Result<Vec<_>, _>>()?;

        let filter_groups = args
            .filter_groups
            .iter()
            .map(|itm| self.build_filter_group_item(cube_name, itm))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(SqlCall::new(
            template,
            deps,
            filter_params,
            filter_groups,
            args.security_context.clone(),
        ))
    }

    fn build_filter_params_item(
        &mut self,
        cube_name: &String,
        item: &FilterParamsItem,
    ) -> Result<SqlCallFilterParamsItem, CubeError> {
        let (compiled_call, foreign_cube) = match &item.column {
            FilterParamsColumn::Compiled(compiled) => {
                let call =
                    self.build_from_template(cube_name, compiled.template.clone(), &compiled.args)?;
                let foreign_cube = Self::foreign_cube_reference(cube_name, &call)?;
                (Some(Rc::new(call)), foreign_cube)
            }
            _ => (None, None),
        };

        Ok(SqlCallFilterParamsItem {
            filter_symbol_name: format!("{}.{}", item.cube_name, item.name),
            column: item.column.clone(),
            compiled_call,
            foreign_cube,
        })
    }

    // A cube a compiled column reads outside the one owning it. Such a column
    // renders only when its filter reaches the query, so the members it reads are
    // not dependencies of the enclosing member and cannot bring a cube into the
    // join — the qualifier it emits would have no table behind it. A cube's table
    // expression is exempt: it inlines the whole expression and needs no join.
    fn foreign_cube_reference(
        cube_name: &String,
        call: &SqlCall,
    ) -> Result<Option<(String, String)>, CubeError> {
        let mut foreign = call
            .get_cube_refs()
            .iter()
            .filter(|cube_ref| matches!(cube_ref, CubeRef::Name(_)))
            .map(|cube_ref| cube_ref.cube_name().clone())
            .collect::<Vec<_>>();

        for dep in call.get_dependencies() {
            foreign.extend(collect_cube_names(&dep)?);
        }

        foreign.retain(|referenced| referenced != cube_name);
        // Reported deterministically: the cube names arrive unordered.
        foreign.sort();
        Ok(foreign
            .into_iter()
            .next()
            .map(|referenced| (cube_name.clone(), referenced)))
    }

    fn build_filter_group_item(
        &mut self,
        cube_name: &String,
        item: &FilterGroupItem,
    ) -> Result<SqlCallFilterGroupItem, CubeError> {
        let filter_params = item
            .filter_params
            .iter()
            .map(|itm| self.build_filter_params_item(cube_name, itm))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SqlCallFilterGroupItem { filter_params })
    }

    fn build_dependency(
        &mut self,
        current_cube_name: &String,
        dep_path: &Vec<String>,
    ) -> Result<SqlDependency, CubeError> {
        assert!(!dep_path.is_empty());

        let symbol_path = SymbolPath::parse_parts(
            self.cube_evaluator.clone(),
            Some(current_cube_name),
            dep_path,
        )
        .map_err(|e| CubeError::user(format!("Error in `{}`: {}", dep_path.join("."), e)))?;

        if self.is_cube_sql {
            if let SymbolPathType::Dimension | SymbolPathType::Measure | SymbolPathType::Segment =
                symbol_path.path_type()
            {
                return Err(CubeError::user(format!(
                    "`sql` of cube `{}` references member `{}`. A cube's sql builds the table the \
                     query reads from, so no member is in scope there — reference the underlying \
                     column instead",
                    current_cube_name,
                    symbol_path.full_name()
                )));
            }
        }

        let path = symbol_path.path().clone();

        match symbol_path.path_type() {
            SymbolPathType::Dimension => {
                let member = self
                    .compiler
                    .add_dimension_evaluator_by_path(symbol_path.clone())?;
                Ok(SqlDependency::Symbol(member))
            }
            SymbolPathType::Measure => {
                let member = self
                    .compiler
                    .add_measure_evaluator_by_path(symbol_path.clone())?;
                Ok(SqlDependency::Symbol(member))
            }
            SymbolPathType::Segment => {
                let member = self
                    .compiler
                    .add_segment_evaluator_by_path(symbol_path.clone())?;
                Ok(SqlDependency::Symbol(member))
            }
            SymbolPathType::CubeName => {
                let symbol = self
                    .compiler
                    .add_cube_name_evaluator(symbol_path.cube_name().clone(), path)?;
                Ok(SqlDependency::CubeRef(CubeRef::Name(symbol)))
            }
            SymbolPathType::CubeTable => {
                let symbol = self
                    .compiler
                    .add_cube_table_evaluator(symbol_path.cube_name().clone(), path)?;
                Ok(SqlDependency::CubeRef(CubeRef::Table(symbol)))
            }
        }
    }
}
