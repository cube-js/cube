use cubenativeutils::wrappers::serializer::{NativeDeserialize, NativeSerialize};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Opaque holder for the JS-side `SQL_UTILS` object. Tesseract only
/// forwards it back into member `sql` functions; the methods
/// invoked on it are a JS concern.
#[nativebridge::native_bridge]
pub trait SqlUtils {}
