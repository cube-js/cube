use super::member_sql::{MemberSql, NativeMemberSql};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// A `case.when` item whose branch is chosen by a QUERY_CONTEXT condition over the
/// query being planned, not by a row value: `context` is a JS function returning bool.
#[nativebridge::native_bridge]
pub trait CaseContextItem {
    #[nbridge(field)]
    fn context(&self) -> Result<Rc<dyn MemberSql>, CubeError>;
    #[nbridge(field)]
    fn sql(&self) -> Result<Rc<dyn MemberSql>, CubeError>;
}
