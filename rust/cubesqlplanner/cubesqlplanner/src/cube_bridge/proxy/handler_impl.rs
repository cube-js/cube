use crate::cube_bridge::base_tools::BaseTools;

use super::{NativeProxy, Proxy};
use super::{NativeProxyHandler, NativeProxyHandlerFunction, ProxyHandler, ProxyHandlerFunction};
use crate::cube_bridge::evaluator::CubeEvaluator;
use cubenativeutils::wrappers::context::NativeFinalize;
use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::{
    object::NativeBox, NativeContextHolder, NativeContextHolderRef, NativeFunction,
    NativeObjectHandle, NativeString, NativeStruct, NativeType,
};
use cubenativeutils::CubeError;
use serde::Deserialize;
use std::any::Any;
use std::cell::{Ref, RefCell};
use std::marker::PhantomData;
use std::rc::Rc;

pub trait ProxyCollector: NativeFinalize {
    type ResultType;
    fn on_get(
        &mut self,
        property_name: String,
        context_holder_ref: Rc<dyn NativeContextHolderRef>,
    ) -> Result<Option<Self::ResultType>, CubeError>;
}

#[derive(Clone)]
pub struct ProxyHandlerImpl<T: ProxyCollector + 'static> {
    collector: Rc<RefCell<T>>,
    base_tools: Rc<dyn BaseTools>,
}

impl<T: ProxyCollector + 'static> ProxyHandlerImpl<T> {
    pub fn new(collector: T /*Rc<RefCell<T>>*/, base_tools: Rc<dyn BaseTools>) -> Rc<Self> {
        Rc::new(Self {
            collector: Rc::new(RefCell::new(collector)),
            base_tools,
        })
    }

    pub fn get_collector(&self) -> Ref<T> {
        self.collector.borrow()
    }
}

impl<
        IT: InnerTypes,
        RT: NativeSerialize<IT::FunctionIT>,
        T: ProxyCollector<ResultType = RT> + 'static,
    > NativeSerialize<IT> for ProxyHandlerImpl<T>
{
    fn to_native(
        &self,
        context: Rc<NativeContextHolder<IT>>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        let boxed_collector = context.boxed(self.collector.clone())?;

        let on_get_fn = context.function(move |fn_context, args| {
            if args.len() >= 2 {
                let collector_box = args[0].to_boxed::<Rc<RefCell<T>>>()?;
                let collector = collector_box.deref_value();
                let mut collector_mut = collector.borrow_mut();
                if let Ok(property_name) = args[1].to_string() {
                    if let Some(result) =
                        collector_mut.on_get(property_name.value()?, fn_context.as_context_ref())?
                    {
                        let result = result.to_native(fn_context.clone())?;
                        Ok(result)
                    } else {
                        let r = fn_context.empty_struct()?;
                        let r = NativeObjectHandle::new_from_type(r);
                        Ok(r)
                    }
                } else {
                    fn_context.undefined()
                }
            } else {
                Err(CubeError::internal(format!(
                    "Collector for cubeDepsProxy is not alive"
                )))
            }
        })?;

        let proxy = self.base_tools.native_proxy(
            NativeProxyHandler::new(boxed_collector),
            NativeProxyHandlerFunction::<IT>::new(on_get_fn),
        )?;

        let proxy = proxy.as_any().downcast::<NativeProxy<IT>>().unwrap();

        Ok(proxy.to_native(context)?)
    }
}
