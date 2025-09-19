use super::{
    inner_types::NeonInnerTypes,
    object::{
        base_types::*, neon_array::NeonArray, neon_function::NeonFunction, neon_struct::NeonStruct,
        NeonObject,
    },
};
use crate::CubeError;
use crate::{
    wrappers::{
        context::NativeContext, functions_args_def::FunctionArgsDef, object::NativeObject,
        object_handle::NativeObjectHandle, NativeContextHolder,
    },
    CubeErrorCauseType,
};
use neon::prelude::*;
use std::{
    cell::RefCell,
    marker::PhantomData,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    rc::{Rc, Weak},
};

pub struct ContextWrapper<C: Context<'static>> {
    cx: C,
}

impl<C: Context<'static>> ContextWrapper<C> {
    pub fn new(cx: C) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self { cx }))
    }

    pub fn with_context<T, F>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut C) -> T,
    {
        f(&mut self.cx)
    }

    pub fn get_context(&mut self) -> &mut C {
        &mut self.cx
    }
}
