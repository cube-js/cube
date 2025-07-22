use super::sql_evaluator::{Compiler, MemberSymbol};
use super::ParamsAllocator;
use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::cube_bridge::join_graph::JoinGraph;
use crate::cube_bridge::join_hints::JoinHintItem;
use crate::cube_bridge::join_item::JoinItemStatic;
use crate::cube_bridge::sql_templates_render::SqlTemplatesRender;
use crate::plan::FilterItem;
use crate::planner::sql_evaluator::collectors::collect_join_hints;
use crate::planner::sql_templates::PlanSqlTemplates;
use chrono_tz::Tz;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::cell::{Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::rc::Rc;

pub struct QueryToolsCachedData {
    join_hints: HashMap<String, Rc<Vec<JoinHintItem>>>,
    join_hints_to_join_key: HashMap<Vec<Rc<Vec<JoinHintItem>>>, Rc<JoinKey>>,
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
    ) -> Result<Rc<Vec<JoinHintItem>>, CubeError> {
        let full_name = node.full_name();
        if let Some(val) = self.join_hints.get(&full_name) {
            Ok(val.clone())
        } else {
            let join_hints = Rc::new(collect_join_hints(node)?);
            self.join_hints.insert(full_name, join_hints.clone());
            Ok(join_hints)
        }
    }

    pub fn join_hints_for_member_symbol_vec(
        &mut self,
        vec: &Vec<Rc<MemberSymbol>>,
    ) -> Result<Vec<Rc<Vec<JoinHintItem>>>, CubeError> {
        vec.iter()
            .map(|b| self.join_hints_for_member(b))
            .collect::<Result<Vec<_>, _>>()
    }

    pub fn join_hints_for_filter_item_vec(
        &mut self,
        vec: &Vec<FilterItem>,
    ) -> Result<Vec<Rc<Vec<JoinHintItem>>>, CubeError> {
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
        hints: Vec<Rc<Vec<JoinHintItem>>>,
        join_fn: impl FnOnce(Vec<JoinHintItem>) -> Result<Rc<dyn JoinDefinition>, CubeError>,
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
    timezone: Tz,
}

impl QueryTools {
    pub fn try_new(
        cube_evaluator: Rc<dyn CubeEvaluator>,
        base_tools: Rc<dyn BaseTools>,
        join_graph: Rc<dyn JoinGraph>,
        timezone_name: Option<String>,
        export_annotated_sql: bool,
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
            timezone.clone(),
        )));
        Ok(Rc::new(Self {
            cube_evaluator,
            base_tools,
            join_graph,
            templates_render,
            params_allocator: Rc::new(RefCell::new(ParamsAllocator::new(export_annotated_sql))),
            evaluator_compiler,
            cached_data: RefCell::new(QueryToolsCachedData::new()),
            timezone,
        }))
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
