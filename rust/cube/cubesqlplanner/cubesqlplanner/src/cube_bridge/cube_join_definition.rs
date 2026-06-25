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

/// Raw join declared on a cube (post-`prepareJoins`). Distinct from
/// the graph-traversal `JoinItem` / `JoinItemDefinition` bridges used
/// at query time.
#[derive(Serialize, Deserialize, Debug)]
pub struct CubeJoinDefinitionStatic {
    /// Name of the join's target cube.
    pub name: String,
    /// Normalized to `belongsTo` | `hasMany` | `hasOne` by
    /// `prepareJoins`.
    pub relationship: String,
}

#[nativebridge::native_bridge(CubeJoinDefinitionStatic)]
pub trait CubeJoinDefinition {
    #[nbridge(field)]
    fn sql(&self) -> Result<Rc<dyn MemberSql>, CubeError>;
}
