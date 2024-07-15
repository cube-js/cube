use cubesql::CubeError;
use std::any::Any;
use std::rc::Rc;

//Should have a lightweight implementation suitable for cloning
pub trait NativeObject {
    fn call(
        &self,
        method: &str,
        args: Vec<Rc<dyn NativeObject>>,
    ) -> Result<Rc<dyn NativeObject>, CubeError>;
    fn as_any(&self) -> &dyn Any;
}

pub trait NativeObjectHolder {
    fn new_from_native(native: Rc<dyn NativeObject>) -> Self;

    fn get_native_object(self) -> Rc<dyn NativeObject>;
}
