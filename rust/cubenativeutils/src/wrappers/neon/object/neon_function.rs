use super::{NeonObject, NeonTypeHandle};
use crate::wrappers::{
    neon::inner_types::NeonInnerTypes,
    object::{NativeFunction, NativeType},
    object_handle::NativeObjectHandle,
};
use cubesql::CubeError;
use lazy_static::lazy_static;
use neon::prelude::*;
use regex::Regex;

#[derive(Clone)]
pub struct NeonFunction<C: Context<'static>> {
    object: NeonTypeHandle<C, JsFunction>,
}

impl<C: Context<'static> + 'static> NeonFunction<C> {
    pub fn new(object: NeonTypeHandle<C, JsFunction>) -> Self {
        Self { object }
    }
}

impl<C: Context<'static> + 'static> NativeType<NeonInnerTypes<C>> for NeonFunction<C> {
    fn into_object(self) -> NeonObject<C> {
        self.object.upcast()
    }
}

impl<C: Context<'static> + 'static> NativeFunction<NeonInnerTypes<C>> for NeonFunction<C> {
    fn call(
        &self,
        args: Vec<NativeObjectHandle<NeonInnerTypes<C>>>,
    ) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        let neon_args = args
            .into_iter()
            .map(|arg| -> Result<_, CubeError> { Ok(arg.into_object().get_object()) })
            .collect::<Result<Vec<_>, _>>()?;
        let neon_reuslt = self.object.map_neon_object(|cx, neon_object| {
            let null = cx.null();
            neon_object
                .call(cx, null, neon_args)
                .map_err(|_| CubeError::internal("Failed to call function ".to_string()))
        })??;
        Ok(NativeObjectHandle::new(NeonObject::new(
            self.object.context.clone(),
            neon_reuslt,
        )))
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
