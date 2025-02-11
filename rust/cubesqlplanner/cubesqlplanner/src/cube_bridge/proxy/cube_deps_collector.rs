use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::evaluator::CubeEvaluator;

use super::{ProxyCollector, ProxyHandlerImpl};
use cubenativeutils::wrappers::context::NativeFinalize;
use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::serializer::NativeSerialize;
use cubenativeutils::wrappers::{
    NativeContextHolder, NativeContextHolderRef, NativeObjectHandle, RootHolder, Rootable,
};
use cubenativeutils::CubeError;
use std::cell::RefCell;
use std::rc::Rc;

pub type CubeDepsCollectorProxyHandler = ProxyHandlerImpl<CubeDepsCollector>;

pub enum CubeDepsCollectorProp {
    Symbol(String),
    Cube(Rc<CubeDepsCollectorProxyHandler>),
}

pub enum CubeDepsProxyRes {
    String(String),
    ToStringFn(String),
    Proxy(Rc<CubeDepsCollectorProxyHandler>),
}

impl<IT: InnerTypes> NativeSerialize<IT> for CubeDepsProxyRes {
    fn to_native(
        &self,
        context: Rc<NativeContextHolder<IT>>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        match self {
            CubeDepsProxyRes::String(s) => s.to_native(context),
            CubeDepsProxyRes::ToStringFn(s) => Ok(NativeObjectHandle::new_from_type(
                context.to_string_fn(s.clone())?,
            )),
            CubeDepsProxyRes::Proxy(proxy) => proxy.to_native(context),
        }
    }
}

pub struct CubeDepsCollector {
    cube_name: String,
    has_sql_fn: bool,
    has_to_string_fn: bool,
    deps: Vec<CubeDepsCollectorProp>,
    evaluator_root: Rc<dyn RootHolder<dyn CubeEvaluator>>,
    base_tools_root: Rc<dyn RootHolder<dyn BaseTools>>,
    context_holder_ref: Rc<dyn NativeContextHolderRef>,
}

impl CubeDepsCollector {
    pub fn try_new(
        cube_name: String,
        evaluator: Rc<dyn CubeEvaluator>,
        base_tools: Rc<dyn BaseTools>,
        context_holder_ref: Rc<dyn NativeContextHolderRef>,
    ) -> Result<Self, CubeError> {
        let evaluator_root = evaluator.to_root()?;
        let base_tools_root = base_tools.to_root()?;
        Ok(Self {
            cube_name,
            has_sql_fn: false,
            has_to_string_fn: false,
            deps: vec![],
            evaluator_root,
            base_tools_root,
            context_holder_ref,
        })
    }

    fn new_with_roots(
        cube_name: String,
        evaluator_root: Rc<dyn RootHolder<dyn CubeEvaluator>>,
        base_tools_root: Rc<dyn RootHolder<dyn BaseTools>>,
        context_holder_ref: Rc<dyn NativeContextHolderRef>,
    ) -> Self {
        Self {
            cube_name,
            has_sql_fn: false,
            has_to_string_fn: false,
            deps: vec![],
            evaluator_root,
            base_tools_root,
            context_holder_ref,
        }
    }

    pub fn set_has_sql_fn(&mut self) {
        self.has_sql_fn = true;
    }

    pub fn set_has_to_string_fn(&mut self) {
        self.has_to_string_fn = true;
    }

    pub fn add_dep(&mut self, dep: CubeDepsCollectorProp) {
        self.deps.push(dep);
    }

    pub fn has_sql_fn(&self) -> bool {
        self.has_sql_fn
    }

    pub fn has_to_string_fn(&self) -> bool {
        self.has_to_string_fn
    }

    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }

    pub fn deps(&self) -> &Vec<CubeDepsCollectorProp> {
        &self.deps
    }
}

impl NativeFinalize for CubeDepsCollector {}

impl ProxyCollector for CubeDepsCollector {
    type ResultType = CubeDepsProxyRes;

    fn on_get(
        &mut self,
        property_name: String,
        context_holder_ref: Rc<dyn NativeContextHolderRef>,
    ) -> Result<Option<Self::ResultType>, CubeError> {
        let evaluator = self
            .evaluator_root
            .clone()
            .to_inner(context_holder_ref.clone())?;
        let base_tools = self
            .base_tools_root
            .clone()
            .to_inner(context_holder_ref.clone())?;
        if property_name == "toString" {
            self.has_to_string_fn = true;
            return Ok(Some(CubeDepsProxyRes::ToStringFn(format!(""))));
        }
        if property_name == "valueOf" {
            return Ok(None);
        }
        if property_name == "sql" {
            self.has_sql_fn = true;
            return Ok(Some(CubeDepsProxyRes::ToStringFn(format!(""))));
        }
        if evaluator.is_name_of_symbol_in_cube(self.cube_name.clone(), property_name.clone())? {
            self.deps
                .push(CubeDepsCollectorProp::Symbol(property_name.clone()));
            return Ok(Some(CubeDepsProxyRes::String(format!(""))));
        }

        if evaluator.is_name_of_cube(property_name.clone())? {
            let collector = CubeDepsCollector::new_with_roots(
                property_name.clone(),
                self.evaluator_root.clone(),
                self.base_tools_root.clone(),
                self.context_holder_ref.clone(),
            );
            let proxy = CubeDepsCollectorProxyHandler::new(collector, base_tools.clone());
            self.deps.push(CubeDepsCollectorProp::Cube(proxy.clone()));
            return Ok(Some(CubeDepsProxyRes::Proxy(proxy)));
        }
        Err(CubeError::user(format!(
            "{}.{} cannot be resolved. There's no such member or cube.",
            self.cube_name, property_name
        )))
    }
}

impl Drop for CubeDepsCollector {
    fn drop(&mut self) {
        let _ = self
            .evaluator_root
            .clone()
            .drop(self.context_holder_ref.clone());

        let _ = self
            .base_tools_root
            .clone()
            .drop(self.context_holder_ref.clone());
    }
}
