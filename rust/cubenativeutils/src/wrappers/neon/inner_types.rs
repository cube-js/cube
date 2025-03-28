use super::{
    context::ContextHolder,
    object::{
        base_types::*, neon_array::NeonArray, neon_function::NeonFunction, neon_struct::NeonStruct,
        NeonObject,
    },
};
use crate::wrappers::inner_types::InnerTypes;
use neon::prelude::*;
use std::marker::PhantomData;

pub struct NeonInnerTypes<C: Context<'static>> {
    marker: PhantomData<ContextHolder<C>>,
}

impl<C: Context<'static>> Clone for NeonInnerTypes<C> {
    fn clone(&self) -> Self {
        Self {
            marker: Default::default(),
        }
    }
}

impl<C: Context<'static> + 'static> InnerTypes for NeonInnerTypes<C> {
    type Object = NeonObject<C>;
    type Context = ContextHolder<C>;
    type Array = NeonArray<C>;
    type Struct = NeonStruct<C>;
    type String = NeonString<C>;
    type Boolean = NeonBoolean<C>;
    type Function = NeonFunction<C>;
    type Number = NeonNumber<C>;
}
