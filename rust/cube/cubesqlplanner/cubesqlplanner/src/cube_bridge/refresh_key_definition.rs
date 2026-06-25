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

/// Raw refresh-key object on a pre-aggregation. The three schema
/// variants (`Sql` / `Every` / `Immutable`) share this single shape;
/// the model builder picks the variant by which fields are present.
#[derive(Serialize, Deserialize, Debug)]
pub struct RefreshKeyDefinitionStatic {
    pub every: Option<String>,
    pub timezone: Option<String>,
    pub incremental: Option<bool>,
    #[serde(rename = "updateWindow")]
    pub update_window: Option<String>,
    pub immutable: Option<bool>,
}

#[nativebridge::native_bridge(RefreshKeyDefinitionStatic)]
pub trait RefreshKeyDefinition {
    #[nbridge(field, optional)]
    fn sql(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;
}
