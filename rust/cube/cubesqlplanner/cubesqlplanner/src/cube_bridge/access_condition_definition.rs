use super::member_sql::{MemberSql, NativeMemberSql};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// One condition under `accessPolicy[].conditions[]` — wraps the
/// `if` predicate callable.
#[nativebridge::native_bridge]
pub trait AccessConditionDefinition {
    #[nbridge(field, rename = "if")]
    fn predicate(&self) -> Result<Rc<dyn MemberSql>, CubeError>;
}
