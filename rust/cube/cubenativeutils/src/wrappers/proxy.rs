use super::{inner_types::InnerTypes, object_handle::NativeObjectHandle};
use crate::wrappers::serializer::NativeSerialize;
use crate::wrappers::*;
use crate::CubeError;
pub fn make_proxy<
    IT: InnerTypes,
    Ret: NativeSerialize<IT::FunctionIT>,
    F: Fn(
            NativeContextHolder<IT::FunctionIT>,
            NativeObjectHandle<IT::FunctionIT>,
            String,
        ) -> Result<Option<Ret>, CubeError>
        + 'static,
>(
    context: NativeContextHolder<IT>,
    target: Option<NativeObjectHandle<IT>>,
    get_fn: F,
) -> Result<NativeObjectHandle<IT>, CubeError> {
    let get_trap = context.make_function(
        move |context: NativeContextHolder<IT::FunctionIT>,
              target: NativeObjectHandle<IT::FunctionIT>,
              prop: NativeObjectHandle<IT::FunctionIT>|
              -> Result<NativeObjectHandle<IT::FunctionIT>, CubeError> {
            if let Ok(string_prop) = prop.to_string() {
                let string_prop = string_prop.value()?;
                if let Some(result) = get_fn(context.clone(), target.clone(), string_prop)? {
                    return result.to_native(context);
                }
            }
            let reflect = context.global("Reflect")?.into_struct()?;
            let reflect_get = reflect.get_field("get")?.into_function()?;
            reflect_get.call(vec![target, prop])
        },
    )?;

    let proxy = context.global("Proxy")?.into_function()?;
    let target = if let Some(target) = target {
        target
    } else {
        NativeObjectHandle::new(context.empty_struct()?.into_object())
    };
    let handler = context.empty_struct()?;
    handler.set_field("get", NativeObjectHandle::new(get_trap.into_object()))?;
    proxy.construct(vec![target, NativeObjectHandle::new(handler.into_object())])
}
