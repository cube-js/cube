use super::{NeonObject, ObjectNeonTypeHolder, RootHolder};
use crate::wrappers::{
    neon::inner_types::NeonInnerTypes,
    object::{NativeRustBox, NativeType},
    rust_handle::NativeRustHandle,
};
use neon::prelude::*;

impl Finalize for NativeRustHandle {}

/// Neon-backed `NativeRustBox`.
///
/// Holds the JS-side `Root<JsBox<NativeRustHandle>>` (so the JS reference
/// keeps the box alive), plus a cheap Rc-clone of the handle on the Rust
/// side. Reading the handle does not need the JS context — `Rc<dyn Any>`
/// inside is self-contained — which keeps the trait API context-free.
pub struct NeonRustBox<C: Context<'static>> {
    object: ObjectNeonTypeHolder<C, JsBox<NativeRustHandle>>,
    handle: NativeRustHandle,
}

impl<C: Context<'static> + 'static> NeonRustBox<C> {
    pub fn new(
        object: ObjectNeonTypeHolder<C, JsBox<NativeRustHandle>>,
        handle: NativeRustHandle,
    ) -> Self {
        Self { object, handle }
    }
}

impl<C: Context<'static>> Clone for NeonRustBox<C> {
    fn clone(&self) -> Self {
        Self {
            object: self.object.clone(),
            handle: self.handle.clone(),
        }
    }
}

impl<C: Context<'static> + 'static> NativeType<NeonInnerTypes<C>> for NeonRustBox<C> {
    fn into_object(self) -> NeonObject<C> {
        let root_holder = RootHolder::from_typed(self.object);
        NeonObject::form_root(root_holder)
    }
}

impl<C: Context<'static> + 'static> NativeRustBox<NeonInnerTypes<C>> for NeonRustBox<C> {
    fn handle(&self) -> &NativeRustHandle {
        &self.handle
    }
}
