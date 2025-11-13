use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::serializer::{NativeDeserialize, NativeSerialize};
use cubenativeutils::wrappers::{NativeContextHolder, NativeFunction};
use cubenativeutils::wrappers::{NativeContextHolderRef, NativeObjectHandle};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub trait FilterParamsCallback {
    fn call(&self, filter_params: &Vec<String>) -> Result<String, CubeError>;
    fn as_any(self: Rc<Self>) -> Rc<dyn Any>;
    fn clone_to_context(
        &self,
        context_ref: &dyn NativeContextHolderRef,
    ) -> Result<Rc<dyn FilterParamsCallback>, CubeError>;
}

#[derive(Clone)]
pub struct NativeFilterParamsCallback<IT: InnerTypes> {
    native_object: NativeObjectHandle<IT>,
}

impl<IT: InnerTypes> NativeFilterParamsCallback<IT> {
    pub fn new(native_object: NativeObjectHandle<IT>) -> Self {
        Self { native_object }
    }
}

impl<IT: InnerTypes> FilterParamsCallback for NativeFilterParamsCallback<IT> {
    fn call(&self, filter_params: &Vec<String>) -> Result<String, CubeError> {
        let func = self.native_object.to_function()?;
        let context = NativeContextHolder::<IT>::new(self.native_object.get_context());
        let args = filter_params
            .iter()
            .map(|param| param.to_native(context.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        let res = func.call(args)?;
        String::from_native(res).map_err(|_| {
            CubeError::user("Callback for FILTER_PARAMS should return string".to_string())
        })
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
    fn clone_to_context(
        &self,
        context_ref: &dyn NativeContextHolderRef,
    ) -> Result<Rc<dyn FilterParamsCallback>, CubeError> {
        Ok(Rc::new(Self {
            native_object: self.native_object.try_clone_to_context_ref(context_ref)?,
        }))
    }
}

impl<IT: InnerTypes> NativeSerialize<IT> for NativeFilterParamsCallback<IT> {
    fn to_native(
        &self,
        _context: NativeContextHolder<IT>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        Ok(self.native_object.clone())
    }
}
impl<IT: InnerTypes> NativeDeserialize<IT> for NativeFilterParamsCallback<IT> {
    fn from_native(native_object: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        Ok(Self::new(native_object))
    }
}

impl<IT: InnerTypes> std::fmt::Debug for NativeFilterParamsCallback<IT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NativeFilterParamsCallback")
    }
}
