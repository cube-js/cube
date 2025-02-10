use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::return_string_fn::ReturnStringFn;

use super::{ProxyCollector, ProxyHandlerImpl};
use cubenativeutils::wrappers::context::NativeFinalize;
use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::serializer::NativeSerialize;
use cubenativeutils::wrappers::{
    NativeContextHolder, NativeContextHolderRef, NativeObjectHandle, NativeStruct, RootHolder,
    Rootable,
};
use cubenativeutils::CubeError;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

pub type FilterParamsCollectorProxyHandler = ProxyHandlerImpl<FilterParamsCollector>;
pub type FilterParamsCubeFiltersCollectorProxyHandler =
    ProxyHandlerImpl<FilterParamsCubeFiltersCollector>;

pub struct FilterParamsCollector {
    cube_filter_proxies: Vec<Rc<FilterParamsCubeFiltersCollectorProxyHandler>>,
    base_tools_root: Rc<dyn RootHolder<dyn BaseTools>>,
}

pub enum FilterParamsCollectorRes {
    CupeFilterProxy(Rc<FilterParamsCubeFiltersCollectorProxyHandler>),
}

impl<IT: InnerTypes> NativeSerialize<IT> for FilterParamsCollectorRes {
    fn to_native(
        &self,
        context: Rc<NativeContextHolder<IT>>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        match self {
            FilterParamsCollectorRes::CupeFilterProxy(proxy) => proxy.to_native(context),
        }
    }
}

impl FilterParamsCollector {
    pub fn try_new(base_tools: Rc<dyn BaseTools>) -> Result<Self, CubeError> {
        let base_tools_root = base_tools.to_root()?;
        Ok(Self {
            cube_filter_proxies: vec![],
            base_tools_root,
        })
    }

    pub fn collected_result(&self) -> HashMap<String, HashSet<String>> {
        let mut result = HashMap::new();
        for cb in self.cube_filter_proxies.iter() {
            let collector = cb.get_collector();
            let entry: &mut HashSet<String> =
                result.entry(collector.cube_name.clone()).or_default();
            for member in collector.members.iter() {
                entry.insert(member.clone());
            }
        }
        println!("!!!! {:?}", result);
        result
    }
}

impl NativeFinalize for FilterParamsCollector {}

impl ProxyCollector for FilterParamsCollector {
    type ResultType = FilterParamsCollectorRes;

    fn on_get(
        &mut self,
        property_name: String,
        context_holder_ref: Rc<dyn NativeContextHolderRef>,
    ) -> Result<Option<Self::ResultType>, CubeError> {
        let base_tools = self
            .base_tools_root
            .clone()
            .to_inner(context_holder_ref.clone())?;
        let cube_filters_collector = FilterParamsCubeFiltersCollector::new(property_name.clone());
        let cube_filters_proxy =
            FilterParamsCubeFiltersCollectorProxyHandler::new(cube_filters_collector, base_tools);
        self.cube_filter_proxies.push(cube_filters_proxy.clone());
        let res = FilterParamsCollectorRes::CupeFilterProxy(cube_filters_proxy);
        Ok(Some(res))
    }
}

pub struct FilterParamsCubeFiltersCollector {
    cube_name: String,
    members: Vec<String>,
}

impl FilterParamsCubeFiltersCollector {
    pub fn new(cube_name: String) -> Self {
        Self {
            cube_name,
            members: vec![],
        }
    }
}

impl NativeFinalize for FilterParamsCubeFiltersCollector {}

impl ProxyCollector for FilterParamsCubeFiltersCollector {
    type ResultType = FilterParamsCubeFiltersCollectorRes;

    fn on_get(
        &mut self,
        property_name: String,
        _context_holder_ref: Rc<dyn NativeContextHolderRef>,
    ) -> Result<Option<Self::ResultType>, CubeError> {
        println!("!!!! prop name: {}", property_name);
        self.members.push(property_name);
        Ok(Some(FilterParamsCubeFiltersCollectorRes::new()))
    }
}

pub struct FilterParamsCubeFiltersCollectorRes {}

impl FilterParamsCubeFiltersCollectorRes {
    pub fn new() -> Self {
        Self {}
    }
}

impl<IT: InnerTypes> NativeSerialize<IT> for FilterParamsCubeFiltersCollectorRes {
    fn to_native(
        &self,
        context: Rc<NativeContextHolder<IT>>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        let res = context.empty_struct()?;
        res.set_field(
            "filter",
            NativeObjectHandle::new_from_type(context.to_string_fn("".to_string())?),
        )?;
        Ok(NativeObjectHandle::new_from_type(res))
    }
}
