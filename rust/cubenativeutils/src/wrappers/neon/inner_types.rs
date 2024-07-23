use super::context::ContextHolder;
use super::object::base_types::*;
use super::object::neon_array::NeonArray;
use super::object::neon_struct::NeonStruct;
use super::object::NeonObject;
use crate::wrappers::inner_types::InnerTypes;
use neon::prelude::*;
use std::marker::PhantomData;

pub struct NeonInnerTypes<'cx, C: Context<'cx>> {
    lifetime: PhantomData<&'cx ContextHolder<'cx, C>>,
}

impl<'cx, C: Context<'cx>> Clone for NeonInnerTypes<'cx, C> {
    fn clone(&self) -> Self {
        Self {
            lifetime: Default::default(),
        }
    }
}

impl<'cx, C: Context<'cx>> InnerTypes for NeonInnerTypes<'cx, C> {
    type Object = NeonObject<'cx, C>;
    type Context = ContextHolder<'cx, C>;
    type Array = NeonArray<'cx, C>;
    type Struct = NeonStruct<'cx, C>;
    type String = NeonString<'cx, C>;
    type Boolean = NeonBoolean<'cx, C>;
    type Number = NeonNumber<'cx, C>;
}
