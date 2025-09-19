use super::{NeonObject, ObjectNeonTypeHolder, RootHolder};
use crate::wrappers::{
    neon::inner_types::NeonInnerTypes,
    object::{NativeFunction, NativeType},
    object_handle::NativeObjectHandle,
};
use cubesql::CubeError;
use lazy_static::lazy_static;
use neon::prelude::*;
use regex::Regex;

pub struct NeonFunction<C: Context<'static>> {
    object: ObjectNeonTypeHolder<C, JsFunction>,
}

impl<C: Context<'static> + 'static> NeonFunction<C> {
    pub fn new(object: ObjectNeonTypeHolder<C, JsFunction>) -> Self {
        Self { object }
    }
}

impl<C: Context<'static>> Clone for NeonFunction<C> {
    fn clone(&self) -> Self {
        Self {
            object: self.object.clone(),
        }
    }
}

impl<C: Context<'static> + 'static> NativeType<NeonInnerTypes<C>> for NeonFunction<C> {
    fn into_object(self) -> NeonObject<C> {
        let root_holder = RootHolder::from_typed(self.object);
        NeonObject::form_root(root_holder)
    }
}

impl<C: Context<'static> + 'static> NativeFunction<NeonInnerTypes<C>> for NeonFunction<C> {
    fn call(
        &self,
        args: Vec<NativeObjectHandle<NeonInnerTypes<C>>>,
    ) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        let neon_args = args
            .into_iter()
            .map(|arg| -> Result<_, CubeError> { arg.into_object().get_object() })
            .collect::<Result<Vec<_>, _>>()?;
        let neon_reuslt =
            self.object
                .map_neon_object_with_safe_call_fn(|cx, neon_object, safe_call_fn| {
                    let null = cx.null();
                    safe_call_fn.safe_call(cx, neon_object, null, neon_args)
                })??;
        Ok(NativeObjectHandle::new(NeonObject::new(
            self.object.get_context(),
            neon_reuslt,
        )?))
    }

    fn definition(&self) -> Result<String, CubeError> {
        let result =
            self.object
                .map_neon_object(|cx, neon_object| -> Result<String, CubeError> {
                    let res = neon_object
                        .to_string(cx)
                        .map_err(|_| {
                            CubeError::internal("Can't convert function to string".to_string())
                        })?
                        .value(cx);
                    Ok(res)
                })??;
        Ok(result)
    }

    fn args_names(&self) -> Result<Vec<String>, CubeError> {
        lazy_static! {
            static ref FUNCTION_RE: Regex = Regex::new(
                r"function\s+\w+\(([A-Za-z0-9_,]*)|\(([\s\S]*?)\)\s*=>|\(?(\w+)\)?\s*=>"
            )
            .unwrap();
        }
        let definition = self.definition()?;
        if let Some(captures) = FUNCTION_RE.captures(&definition) {
            let args_string = captures.get(1).or(captures.get(2)).or(captures.get(3));
            if let Some(args_string) = args_string {
                Ok(args_string
                    .as_str()
                    .split(',')
                    .filter_map(|s| {
                        let arg = s.trim().to_string();
                        if arg.is_empty() {
                            None
                        } else {
                            Some(arg)
                        }
                    })
                    .collect())
            } else {
                Ok(vec![])
            }
        } else {
            Ok(vec![])
        }
    }
}
