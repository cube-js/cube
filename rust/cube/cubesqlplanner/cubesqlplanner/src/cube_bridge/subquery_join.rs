use super::member_expression::{MemberExpressionDefinition, NativeMemberExpressionDefinition};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::{NativeContextHolder, NativeObjectHandle};
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::rc::Rc;

/// A query-level join against an opaque sub-query, originating from the
/// SQL API (cubesql) `subqueryJoins`. `sql` is a complete, pre-rendered
/// SELECT; `on` is the join condition expressed as a member expression
/// (same shape as a parsed member expression: `cubeName` + `expression`).
#[derive(Serialize, Deserialize, Debug, Clone, nativebridge::NativeBridgeStatic)]
pub struct SubqueryJoinStatic {
    pub sql: String,
    #[serde(rename = "joinType")]
    pub join_type: Option<String>,
    pub alias: String,
}

#[nativebridge::native_bridge(SubqueryJoinStatic, with_static_meta)]
pub trait SubqueryJoin {
    #[nbridge(field)]
    fn on(&self) -> Result<Rc<dyn MemberExpressionDefinition>, CubeError>;
}
