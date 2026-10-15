use super::{
    primitive_root_holder::{read_js_string, read_js_string_into},
    NeonObject, ObjectNeonTypeHolder, RootHolder,
};
use crate::wrappers::{
    neon::{inner_types::NeonInnerTypes, object::IntoNeonObject},
    object::{NativeStruct, NativeType},
    object_handle::NativeObjectHandle,
};
use crate::CubeError;
use neon::prelude::*;
use neon::thread::LocalKey;
use std::mem::MaybeUninit;

static OBJECT_KEYS: LocalKey<Root<JsFunction>> = LocalKey::new();

/// N-API's `napi_get_all_property_names` always collects keys through V8's slow `KeyAccumulator`
/// (a hash set per call); `Object.keys` copies the enum cache of the object's map instead.
fn object_keys<C: Context<'static>>(
    cx: &mut C,
    object: Handle<'static, JsObject>,
) -> NeonResult<Handle<'static, JsArray>> {
    let keys_fn = OBJECT_KEYS
        .get_or_try_init(cx, |cx| {
            let object_ctor = cx.global::<JsFunction>("Object")?;
            let keys_fn = object_ctor.get::<JsFunction, _, _>(cx, "keys")?;
            NeonResult::Ok(keys_fn.root(cx))
        })?
        .to_inner(cx);
    let undefined = cx.undefined();
    // Checked rather than trusted: `Object.keys` may have been replaced by a polyfill.
    keys_fn
        .call(cx, undefined, [object.upcast::<JsValue>()])?
        .downcast_or_throw::<JsArray, _>(cx)
}

type Entries<K, C> = Vec<(K, NativeObjectHandle<NeonInnerTypes<C>>)>;

pub struct NeonStruct<C: Context<'static>> {
    object: ObjectNeonTypeHolder<C, JsObject>,
}

impl<C: Context<'static> + 'static> NeonStruct<C> {
    pub fn new(object: ObjectNeonTypeHolder<C, JsObject>) -> Self {
        Self { object }
    }

    fn collect_entries<K: std::fmt::Display>(
        &self,
        capacity_limit: usize,
        mut select: impl FnMut(&mut C, Handle<'static, JsString>) -> Option<K>,
    ) -> Result<Entries<K, C>, CubeError> {
        let context = self.object.get_context();
        self.object.map_neon_object_with_error(|cx, object| {
            let names = object_keys(cx, *object)?;
            let len = names.len(cx);
            let mut entries = Vec::with_capacity((len as usize).min(capacity_limit));
            for idx in 0..len {
                let key = names.get::<JsString, _, _>(cx, idx)?;
                let Some(name) = select(cx, key) else {
                    continue;
                };
                // Looked up by the original key handle: a fresh JsString built from `name`
                // would be re-internalized by V8 on every lookup.
                let value = object
                    .get_value(cx, key)
                    .map_err(CubeError::from)
                    .and_then(|value| RootHolder::new_in(cx, &context, value))
                    .map_err(|mut err| {
                        err.message =
                            format!("field `{name}`: failed to read value: {}", err.message);
                        err
                    })?;
                entries.push((name, NeonObject::from_root(value).into()));
            }
            Ok(entries)
        })
    }
}

impl<C: Context<'static>> Clone for NeonStruct<C> {
    fn clone(&self) -> Self {
        Self {
            object: self.object.clone(),
        }
    }
}

impl<C: Context<'static> + 'static> NativeType<NeonInnerTypes<C>> for NeonStruct<C> {
    fn into_object(self) -> NeonObject<C> {
        let root_holder = RootHolder::from_typed(self.object);
        NeonObject::from_root(root_holder)
    }
}

impl<C: Context<'static> + 'static> NativeStruct<NeonInnerTypes<C>> for NeonStruct<C> {
    fn get_field(
        &self,
        field_name: &str,
    ) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        let neon_result = self
            .object
            .map_neon_object(|cx, neon_object| neon_object.get::<JsValue, _, _>(cx, field_name))?;
        Ok(NativeObjectHandle::new(NeonObject::new(
            self.object.get_context(),
            neon_result,
        )?))
    }

    fn entries(&self) -> Result<Vec<(String, NativeObjectHandle<NeonInnerTypes<C>>)>, CubeError> {
        self.collect_entries(usize::MAX, |cx, key| Some(read_js_string(cx, key)))
    }

    fn entries_for_fields(
        &self,
        fields: &'static [&'static str],
    ) -> Result<Vec<(&'static str, NativeObjectHandle<NeonInnerTypes<C>>)>, CubeError> {
        self.collect_entries(fields.len(), |cx, key| {
            let mut buf = [MaybeUninit::uninit(); 128];
            let key = read_js_string_into(cx, key, &mut buf);
            fields
                .iter()
                .copied()
                .find(|field| field.as_bytes() == key.as_ref())
        })
    }

    fn has_field(&self, field_name: &str) -> Result<bool, CubeError> {
        let result = self.object.map_neon_object(|cx, neon_object| {
            let res = neon_object
                .get_opt::<JsValue, _, _>(cx, field_name)?
                .is_some();
            Ok(res)
        })?;
        Ok(result)
    }

    fn set_field(
        &self,
        field_name: &str,
        value: NativeObjectHandle<NeonInnerTypes<C>>,
    ) -> Result<bool, CubeError> {
        let value = value.into_object().get_js_value()?;
        self.object
            .map_neon_object::<_, _>(|cx, object| object.set(cx, field_name, value))
    }
    fn call_method(
        &self,
        method: &str,
        args: Vec<NativeObjectHandle<NeonInnerTypes<C>>>,
    ) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        let neon_args = args
            .into_iter()
            .map(|arg| -> Result<_, CubeError> { arg.into_object().get_js_value() })
            .collect::<Result<Vec<_>, _>>()?;

        let result = self
            .object
            .map_neon_object(|cx, neon_object| {
                neon_object
                    .get::<JsFunction, _, _>(cx, method)?
                    .call(cx, *neon_object, neon_args)
            })?
            .into_neon_object(self.object.get_context())?;
        Ok(result.into())
    }
}
