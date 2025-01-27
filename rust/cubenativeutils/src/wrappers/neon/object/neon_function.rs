use super::NeonObject;
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
pub struct NeonFunction<'cx: 'static, C: Context<'cx>> {
    object: NeonObject<'cx, C>,
}

impl<'cx, C: Context<'cx> + 'cx> NeonFunction<'cx, C> {
    pub fn new(object: NeonObject<'cx, C>) -> Self {
        Self { object }
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeType<NeonInnerTypes<'cx, C>> for NeonFunction<'cx, C> {
    fn into_object(self) -> NeonObject<'cx, C> {
        self.object
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeFunction<NeonInnerTypes<'cx, C>> for NeonFunction<'cx, C> {
    fn call(
        &self,
        args: Vec<NativeObjectHandle<NeonInnerTypes<'cx, C>>>,
    ) -> Result<NativeObjectHandle<NeonInnerTypes<'cx, C>>, CubeError> {
        let neon_args = args
            .into_iter()
            .map(|arg| -> Result<_, CubeError> { Ok(arg.into_object().get_object()) })
            .collect::<Result<Vec<_>, _>>()?;
        let neon_reuslt = self.object.map_neon_object(|cx, neon_object| {
            let this = neon_object
                .downcast::<JsFunction, _>(cx)
                .map_err(|_| CubeError::internal(format!("Neon object is not JsFunction")))?;
            let null = cx.null();
            this.call(cx, null, neon_args)
                .map_err(|_| CubeError::internal(format!("Failed to call function ")))
        })?;
        Ok(NativeObjectHandle::new(NeonObject::new(
            self.object.context.clone(),
            neon_reuslt,
        )))
    }

    fn definition(&self) -> Result<String, CubeError> {
        let result =
            self.object
                .map_neon_object(|cx, neon_object| -> Result<String, CubeError> {
                    let this = neon_object.downcast::<JsFunction, _>(cx).map_err(|_| {
                        CubeError::internal(format!("Neon object is not JsFunction"))
                    })?;
                    let res = this
                        .to_string(cx)
                        .map_err(|_| {
                            CubeError::internal(format!("Can't convert function to string"))
                        })?
                        .value(cx);
                    Ok(res)
                })?;
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
