use super::sql_evaluator::Compiler;
use super::ParamsAllocator;
use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::cube_bridge::join_graph::JoinGraph;
use crate::cube_bridge::sql_templates_render::SqlTemplatesRender;
use chrono_tz::Tz;
use convert_case::{Case, Casing};
use cubenativeutils::CubeError;
use lazy_static::lazy_static;
use regex::Regex;
use std::cell::{Ref, RefCell, RefMut};
use std::rc::Rc;

pub struct QueryToolsCachedData {
    join: Option<Rc<dyn JoinDefinition>>,
}

impl QueryToolsCachedData {
    pub fn new() -> Self {
        Self { join: None }
    }

    pub fn join(&self) -> Result<Rc<dyn JoinDefinition>, CubeError> {
        self.join.clone().ok_or(CubeError::internal(
            "Join not set in QueryToolsCachedData".to_string(),
        ))
    }

    pub fn set_join(&mut self, join: Rc<dyn JoinDefinition>) {
        self.join = Some(join);
    }
}

pub struct QueryTools {
    cube_evaluator: Rc<dyn CubeEvaluator>,
    base_tools: Rc<dyn BaseTools>,
    join_graph: Rc<dyn JoinGraph>,
    templates_render: Rc<dyn SqlTemplatesRender>,
    params_allocator: Rc<RefCell<ParamsAllocator>>,
    evaluator_compiler: Rc<RefCell<Compiler>>,
    cached_data: RefCell<QueryToolsCachedData>,
    timezone: Option<Tz>,
}

impl QueryTools {
    pub fn try_new(
        cube_evaluator: Rc<dyn CubeEvaluator>,
        base_tools: Rc<dyn BaseTools>,
        join_graph: Rc<dyn JoinGraph>,
        timezone_name: Option<String>,
    ) -> Result<Rc<Self>, CubeError> {
        let templates_render = base_tools.sql_templates()?;
        let evaluator_compiler = Rc::new(RefCell::new(Compiler::new(cube_evaluator.clone())));
        let timezone = if let Some(timezone) = timezone_name {
            Some(
                timezone
                    .parse::<Tz>()
                    .map_err(|_| CubeError::user(format!("Incorrect timezone {}", timezone)))?,
            )
        } else {
            None
        };
        Ok(Rc::new(Self {
            cube_evaluator,
            base_tools,
            join_graph,
            templates_render,
            params_allocator: Rc::new(RefCell::new(ParamsAllocator::new())),
            evaluator_compiler,
            cached_data: RefCell::new(QueryToolsCachedData::new()),
            timezone,
        }))
    }

    pub fn cube_evaluator(&self) -> &Rc<dyn CubeEvaluator> {
        &self.cube_evaluator
    }

    pub fn base_tools(&self) -> &Rc<dyn BaseTools> {
        &self.base_tools
    }

    pub fn join_graph(&self) -> &Rc<dyn JoinGraph> {
        &self.join_graph
    }

    pub fn timezone(&self) -> &Option<Tz> {
        &self.timezone
    }

    pub fn cached_data(&self) -> Ref<'_, QueryToolsCachedData> {
        self.cached_data.borrow()
    }

    pub fn cached_data_mut(&self) -> RefMut<'_, QueryToolsCachedData> {
        self.cached_data.borrow_mut()
    }

    pub fn evaluator_compiler(&self) -> &Rc<RefCell<Compiler>> {
        &self.evaluator_compiler
    }

    pub fn alias_name(&self, name: &str) -> String {
        name.to_case(Case::Snake).replace(".", "__")
    }

    pub fn escaped_alias_name(&self, name: &str) -> String {
        self.escape_column_name(&self.alias_name(name))
    }

    pub fn cube_alias_name(&self, name: &str, prefix: &Option<String>) -> String {
        if let Some(prefix) = prefix {
            self.alias_name(&format!("{}__{}", prefix, self.alias_name(name)))
        } else {
            self.alias_name(name)
        }
    }

    pub fn auto_prefix_with_cube_name(&self, cube_name: &str, sql: &str) -> String {
        lazy_static! {
            static ref SINGLE_MEMBER_RE: Regex = Regex::new(r"^[_a-zA-Z][_a-zA-Z0-9]*$").unwrap();
        }
        if SINGLE_MEMBER_RE.is_match(sql) {
            format!(
                "{}.{}",
                self.escape_column_name(&self.cube_alias_name(&cube_name, &None)),
                sql
            )
        } else {
            sql.to_string()
        }
    }

    pub fn escape_column_name(&self, column_name: &str) -> String {
        format!("\"{}\"", column_name)
    }

    pub fn templates_render(&self) -> Rc<dyn SqlTemplatesRender> {
        self.templates_render.clone()
    }

    pub fn allocaate_param(&self, name: &str) -> usize {
        self.params_allocator.borrow_mut().allocate_param(name)
    }
    pub fn get_allocated_params(&self) -> Vec<String> {
        self.params_allocator.borrow().get_params().clone()
    }
    pub fn build_sql_and_params(
        &self,
        sql: &str,
        should_reuse_params: bool,
    ) -> Result<(String, Vec<String>), CubeError> {
        self.params_allocator
            .borrow()
            .build_sql_and_params(sql, should_reuse_params)
    }
}
