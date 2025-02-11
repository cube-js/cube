use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::serializer::NativeSerialize;
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct ReturnStringFn {
    value: String,
}

impl ReturnStringFn {
    pub fn new(value: String) -> Self {
        ReturnStringFn { value }
    }
}

impl<IT: InnerTypes> NativeSerialize<IT> for ReturnStringFn {
    fn to_native(
        &self,
        context: Rc<NativeContextHolder<IT>>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        Ok(NativeObjectHandle::new_from_type(
            context.to_string_fn(self.value.clone())?,
        ))
    }
}
