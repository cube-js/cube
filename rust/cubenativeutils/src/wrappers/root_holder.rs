use super::NativeContextHolderRef;
use crate::CubeError;
use std::any::Any;
use std::rc::Rc;

pub trait RootHolder<T: ?Sized> {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any>;
    fn drop(
        self: Rc<Self>,
        context_holder_ref: Rc<dyn NativeContextHolderRef>,
    ) -> Result<(), CubeError>;
    fn to_inner(
        self: Rc<Self>,
        context_holder_ref: Rc<dyn NativeContextHolderRef>,
    ) -> Result<Rc<T>, CubeError>;
}

pub trait Rootable<T: ?Sized> {
    fn to_root(self: Rc<Self>) -> Result<Rc<dyn RootHolder<T>>, CubeError>;
}
