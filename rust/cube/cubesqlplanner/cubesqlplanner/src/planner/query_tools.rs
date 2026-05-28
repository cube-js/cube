use super::Compiler;
use super::ParamsAllocator;
use crate::cube_bridge::base_query_options::MaskedMemberItem;
use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::cube_bridge::join_graph::JoinGraph;
use crate::cube_bridge::join_item::JoinItemStatic;
use crate::cube_bridge::security_context::SecurityContext;
use crate::cube_bridge::sql_templates_render::SqlTemplatesRender;
use crate::planner::filter::compiler::FilterCompiler;
use crate::planner::filter::{FilterGroup, FilterGroupOperator, FilterItem};
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
    // Compiled mask filters keyed by member full path. Populated in try_new
    // after the QueryTools Rc is constructed (FilterCompiler requires it),
    // then never mutated again — RefCell only carries the construction phase.
    member_mask_filters: RefCell<HashMap<String, FilterItem>>,
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
        masked_members: Option<Vec<MaskedMemberItem>>,
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

        // Phase 1: collect masked member names eagerly; mask filters are
        // compiled in Phase 2 below (FilterCompiler requires Rc<QueryTools>,
        // which doesn't exist yet at this point).
        let mut masked_set = HashSet::new();
        if let Some(items) = &masked_members {
            for item in items {
                masked_set.insert(item.member.clone());
            }
        }

        let result = Rc::new(Self {
            cube_evaluator,
            base_tools,
            join_graph,
            templates_render,
            params_allocator: Rc::new(RefCell::new(ParamsAllocator::new(export_annotated_sql))),
            evaluator_compiler: evaluator_compiler.clone(),
            timezone,
            convert_tz_for_raw_time_dimension,
            masked_members: masked_set,
            member_mask_filters: RefCell::new(HashMap::new()),
        });

        evaluator_compiler
            .borrow_mut()
            .set_query_tools(Rc::downgrade(&result));

        // Phase 2: compile mask filters once now that Rc<QueryTools> exists.
        // After this, member_mask_filters is treated as immutable for the
        // lifetime of QueryTools.
        if let Some(items) = masked_members {
            Self::compile_mask_filters(&result, items)?;
        }

        Ok(result)
    }

    fn compile_mask_filters(
        this: &Rc<Self>,
        items: Vec<MaskedMemberItem>,
    ) -> Result<(), CubeError> {
        let mut compiled = HashMap::new();
        for item in items {
            let Some(native_filter) = item.filter else {
                continue;
            };
            let mut compiler = this.evaluator_compiler.borrow_mut();
            let mut filter_compiler = FilterCompiler::new(&mut compiler, this.clone());
            filter_compiler.add_item(&native_filter)?;
            let (dimension_filters, _, _) = filter_compiler.extract_result();
            if dimension_filters.is_empty() {
                continue;
            }
            let filter_item = if dimension_filters.len() == 1 {
                dimension_filters.into_iter().next().unwrap()
            } else {
                FilterItem::Group(Rc::new(FilterGroup::new(
                    FilterGroupOperator::And,
                    dimension_filters,
                )))
            };
            compiled.insert(item.member, filter_item);
        }
        *this.member_mask_filters.borrow_mut() = compiled;
        Ok(())
    }

    pub fn is_member_masked(&self, member_path: &str) -> bool {
        self.masked_members.contains(member_path)
    }

    pub fn member_mask_filter(&self, member_path: &str) -> Option<FilterItem> {
        self.member_mask_filters.borrow().get(member_path).cloned()
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
