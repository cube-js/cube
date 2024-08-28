use super::{BaseDimension, BaseMeasure, BaseMember, ParamsAllocator};
use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::sql_templates_render::SqlTemplatesRender;
use convert_case::{Case, Casing};
use cubenativeutils::CubeError;
use lazy_static::lazy_static;
use regex::Regex;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

pub struct QueryTools {
    cube_evaluator: Rc<dyn CubeEvaluator>,
    base_tools: Rc<dyn BaseTools>,
    templates_render: Rc<dyn SqlTemplatesRender>,
    params_allocator: Rc<RefCell<ParamsAllocator>>,
}

impl QueryTools {
    pub fn try_new(
        cube_evaluator: Rc<dyn CubeEvaluator>,
        base_tools: Rc<dyn BaseTools>,
    ) -> Result<Rc<Self>, CubeError> {
        let templates_render = base_tools.sql_templates()?;
        Ok(Rc::new(Self {
            cube_evaluator,
            base_tools,
            templates_render,
            params_allocator: Rc::new(RefCell::new(ParamsAllocator::new())),
        }))
    }

    pub fn cube_evaluator(&self) -> &Rc<dyn CubeEvaluator> {
        &self.cube_evaluator
    }

    pub fn base_tools(&self) -> &Rc<dyn BaseTools> {
        &self.base_tools
    }

    pub fn alias_name(&self, name: &str) -> Result<String, CubeError> {
        Ok(name.to_case(Case::Snake).replace(".", "__"))
    }

    pub fn auto_prefix_with_cube_name(&self, cube_name: &str, sql: &str) -> String {
        lazy_static! {
            static ref SINGLE_MEMBER_RE: Regex = Regex::new(r"^[_a-zA-Z][_a-zA-Z0-9]*$").unwrap();
        }
        if SINGLE_MEMBER_RE.is_match(sql) {
            format!("{}.{}", self.escape_column_name(cube_name), sql)
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
}
