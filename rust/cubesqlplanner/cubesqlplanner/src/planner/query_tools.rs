use super::sql_evaluator::{Compiler, MemberSymbol};
use super::{BaseMember, ParamsAllocator};
use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::cube_bridge::join_graph::JoinGraph;
use crate::cube_bridge::join_item::JoinItemStatic;
use crate::cube_bridge::sql_templates_render::SqlTemplatesRender;
use crate::plan::FilterItem;
use crate::planner::sql_evaluator::collectors::collect_join_hints;
use crate::planner::sql_templates::PlanSqlTemplates;
use chrono_tz::Tz;
use convert_case::{Case, Casing};
use cubenativeutils::CubeError;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use std::cell::{Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::rc::Rc;

pub struct QueryToolsCachedData {
    join_hints: HashMap<String, Rc<Vec<String>>>,
    join_hints_to_join_key: HashMap<Vec<Rc<Vec<String>>>, Rc<JoinKey>>,
    join_key_to_join: HashMap<Rc<JoinKey>, Rc<dyn JoinDefinition>>,
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct JoinKey {
    root: String,
    joins: Vec<JoinItemStatic>,
}

impl QueryToolsCachedData {
    pub fn new() -> Self {
        Self {
            join_hints: HashMap::new(),
            join_hints_to_join_key: HashMap::new(),
            join_key_to_join: HashMap::new(),
        }
    }

    pub fn join_hints_for_member(
        &mut self,
        node: &Rc<MemberSymbol>,
    ) -> Result<Rc<Vec<String>>, CubeError> {
        let full_name = node.full_name();
        if let Some(val) = self.join_hints.get(&full_name) {
            Ok(val.clone())
        } else {
            let join_hints = Rc::new(collect_join_hints(node)?);
            self.join_hints.insert(full_name, join_hints.clone());
            Ok(join_hints)
        }
    }

    pub fn join_hints_for_base_member_vec<T: BaseMember>(
        &mut self,
        vec: &Vec<Rc<T>>,
    ) -> Result<Vec<Rc<Vec<String>>>, CubeError> {
        vec.iter()
            .map(|b| self.join_hints_for_member(&b.member_evaluator()))
            .collect::<Result<Vec<_>, _>>()
    }

    pub fn join_hints_for_member_symbol_vec(
        &mut self,
        vec: &Vec<Rc<MemberSymbol>>,
    ) -> Result<Vec<Rc<Vec<String>>>, CubeError> {
        vec.iter()
            .map(|b| self.join_hints_for_member(b))
            .collect::<Result<Vec<_>, _>>()
    }

    pub fn join_hints_for_filter_item_vec(
        &mut self,
        vec: &Vec<FilterItem>,
    ) -> Result<Vec<Rc<Vec<String>>>, CubeError> {
        let mut member_symbols = Vec::new();
        for i in vec.iter() {
            i.find_all_member_evaluators(&mut member_symbols);
        }
        member_symbols
            .iter()
            .map(|b| self.join_hints_for_member(b))
            .collect::<Result<Vec<_>, _>>()
    }

    pub fn join_by_hints(
        &mut self,
        hints: Vec<Rc<Vec<String>>>,
        join_fn: impl FnOnce(Vec<String>) -> Result<Rc<dyn JoinDefinition>, CubeError>,
    ) -> Result<(Rc<JoinKey>, Rc<dyn JoinDefinition>), CubeError> {
        if let Some(key) = self.join_hints_to_join_key.get(&hints) {
            Ok((key.clone(), self.join_key_to_join.get(key).unwrap().clone()))
        } else {
            let join = join_fn(
                hints
                    .iter()
                    .flat_map(|h| h.as_ref().iter().cloned())
                    .collect(),
            )?;
            let join_key = Rc::new(JoinKey {
                root: join.static_data().root.to_string(),
                joins: join
                    .joins()?
                    .items()
                    .iter()
                    .map(|i| i.static_data().clone())
                    .collect(),
            });
            self.join_hints_to_join_key.insert(hints, join_key.clone());
            self.join_key_to_join.insert(join_key.clone(), join.clone());
            Ok((join_key, join))
        }
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
        let sql_templates = PlanSqlTemplates::new(templates_render.clone());
        Ok(Rc::new(Self {
            cube_evaluator,
            base_tools,
            join_graph,
            templates_render,
            params_allocator: Rc::new(RefCell::new(ParamsAllocator::new(sql_templates))),
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
    ) -> Result<(String, Vec<String>), CubeError> {
        let native_allocated_params = self.base_tools.get_allocated_params()?;
        self.params_allocator.borrow().build_sql_and_params(
            sql,
            native_allocated_params,
            should_reuse_params,
        )
    }
}
