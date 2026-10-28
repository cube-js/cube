use super::member_sql::{MemberSql, NativeMemberSql};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug, nativebridge::NativeBridgeStatic)]
pub struct CaseContextItemStatic {
    /// Members the condition refers to, resolved by the schema compiler.
    #[serde(rename = "contextReferences")]
    pub context_references: Option<Vec<String>>,
}

/// A `case.when` item whose branch is chosen by a QUERY_CONTEXT condition over the
/// query being planned, not by a row value: `context` is a JS function returning bool.
#[nativebridge::native_bridge(CaseContextItemStatic, with_static_meta)]
pub trait CaseContextItem {
    #[nbridge(field)]
    fn context(&self) -> Result<Rc<dyn MemberSql>, CubeError>;
    #[nbridge(field)]
    fn sql(&self) -> Result<Rc<dyn MemberSql>, CubeError>;
}
