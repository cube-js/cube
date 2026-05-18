use cubenativeutils::wrappers::serializer::{NativeDeserialize, NativeSerialize};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Type-erased reference to the JS-side security context object.
/// Used to materialise `SECURITY_CONTEXT.x.filter(...)` /
/// `unsafeValue()` proxies when compiling a member SQL function.
#[nativebridge::native_bridge]
pub trait SecurityContext {}
