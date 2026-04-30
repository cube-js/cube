use super::sql_evaluator::Compiler;
use super::ParamsAllocator;
use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::cube_bridge::join_graph::JoinGraph;
use crate::cube_bridge::join_item::JoinItemStatic;
use crate::cube_bridge::security_context::SecurityContext;
use crate::cube_bridge::sql_templates_render::SqlTemplatesRender;
use crate::planner::join_hints::JoinHints;
use crate::planner::sql_templates::PlanSqlTemplates;
use chrono_tz::Tz;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct JoinKey {
    root: String,
    joins: Vec<JoinItemStatic>,
}

pub struct QueryTools {
    cube_evaluator: Rc<dyn CubeEvaluator>,
    base_tools: Rc<dyn BaseTools>,
    join_graph: Rc<dyn JoinGraph>,
    templates_render: Rc<dyn SqlTemplatesRender>,
    params_allocator: Rc<RefCell<ParamsAllocator>>,
    evaluator_compiler: Rc<RefCell<Compiler>>,
    timezone: Tz,
    convert_tz_for_raw_time_dimension: bool,
    masked_members: HashSet<String>,
}

impl QueryTools {
    pub fn try_new(
        cube_evaluator: Rc<dyn CubeEvaluator>,
        security_context: Rc<dyn SecurityContext>,
        base_tools: Rc<dyn BaseTools>,
        join_graph: Rc<dyn JoinGraph>,
        timezone_name: Option<String>,
        export_annotated_sql: bool,
        convert_tz_for_raw_time_dimension: bool,
        masked_members: Option<Vec<String>>,
        member_to_alias: Option<HashMap<String, String>>,
    ) -> Result<Rc<Self>, CubeError> {
        let templates_render = base_tools.sql_templates()?;
        let timezone = if let Some(timezone) = timezone_name {
            timezone
                .parse::<Tz>()
                .map_err(|_| CubeError::user(format!("Incorrect timezone {}", timezone)))?
        } else {
            Tz::UTC
        };
        let evaluator_compiler = Rc::new(RefCell::new(Compiler::new(
            cube_evaluator.clone(),
            base_tools.clone(),
            security_context.clone(),
            timezone.clone(),
            member_to_alias,
        )));
        Ok(Rc::new(Self {
            cube_evaluator,
            base_tools,
            join_graph,
            templates_render,
            params_allocator: Rc::new(RefCell::new(ParamsAllocator::new(export_annotated_sql))),
            evaluator_compiler,
            timezone,
            convert_tz_for_raw_time_dimension,
            masked_members: masked_members.unwrap_or_default().into_iter().collect(),
        }))
    }

    pub fn is_member_masked(&self, member_path: &str) -> bool {
        self.masked_members.contains(member_path)
    }

    pub fn cube_evaluator(&self) -> &Rc<dyn CubeEvaluator> {
        &self.cube_evaluator
    }

    pub fn plan_sql_templates(&self, external: bool) -> Result<PlanSqlTemplates, CubeError> {
        let driver_tools = self.base_tools.driver_tools(external)?;
        PlanSqlTemplates::try_new(driver_tools, external)
    }

    pub fn base_tools(&self) -> &Rc<dyn BaseTools> {
        &self.base_tools
    }

    pub fn join_graph(&self) -> &Rc<dyn JoinGraph> {
        &self.join_graph
    }

    pub fn timezone(&self) -> Tz {
        self.timezone
    }

    pub fn convert_tz_for_raw_time_dimension(&self) -> bool {
        self.convert_tz_for_raw_time_dimension
    }

    pub fn join_for_hints(
        &self,
        hints: &JoinHints,
    ) -> Result<(JoinKey, Rc<dyn JoinDefinition>), CubeError> {
        let join = self
            .base_tools
            .join_tree_for_hints(hints.items().to_vec())?;
        let join_key = JoinKey {
            root: join.static_data().root.to_string(),
            joins: join
                .joins()?
                .iter()
                .map(|i| i.static_data().clone())
                .collect(),
        };
        Ok((join_key, join))
    }

    pub fn evaluator_compiler(&self) -> &Rc<RefCell<Compiler>> {
        &self.evaluator_compiler
    }

    pub fn alias_name(&self, name: &str) -> String {
        PlanSqlTemplates::alias_name(name)
    }

    pub fn cube_alias_name(&self, name: &str, prefix: &Option<String>) -> String {
        if let Some(prefix) = prefix {
            self.alias_name(&format!("{}__{}", prefix, self.alias_name(name)))
        } else {
            self.alias_name(name)
        }
    }

    pub fn parse_member_path(&self, name: &str) -> Result<(String, String), CubeError> {
        let path = name.split('.').collect_vec();
        if path.len() == 2 {
            Ok((path[0].to_string(), path[1].to_string()))
        } else {
            Err(CubeError::internal(format!(
                "Invalid member name: '{}'",
                name
            )))
        }
    }

    pub fn alias_for_cube(&self, cube_name: &String) -> Result<String, CubeError> {
        let cube_definition = self.cube_evaluator().cube_from_path(cube_name.clone())?;
        let res = if let Some(sql_alias) = &cube_definition.static_data().sql_alias {
            sql_alias.clone()
        } else {
            cube_name.clone()
        };
        Ok(res)
    }

    pub fn templates_render(&self) -> Rc<dyn SqlTemplatesRender> {
        self.templates_render.clone()
    }

    pub fn allocate_param(&self, name: &str) -> String {
        self.params_allocator.borrow_mut().allocate_param(name)
    }
    pub fn get_allocated_params(&self) -> Vec<String> {
        self.params_allocator.borrow().get_params().clone()
    }
    pub fn build_sql_and_params(
        &self,
        sql: &str,
        should_reuse_params: bool,
        templates: &PlanSqlTemplates,
    ) -> Result<(String, Vec<String>), CubeError> {
        let native_allocated_params = self.base_tools.get_allocated_params()?;
        self.params_allocator.borrow().build_sql_and_params(
            sql,
            native_allocated_params,
            should_reuse_params,
            templates,
        )
    }
}
