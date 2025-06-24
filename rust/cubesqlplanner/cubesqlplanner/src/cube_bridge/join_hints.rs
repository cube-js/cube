use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::serializer::NativeDeserialize;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::Serialize;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize)]
pub enum JoinHintItem {
    Single(String),
    Vector(Vec<String>),
}

impl<IT: InnerTypes> NativeDeserialize<IT> for JoinHintItem {
    fn from_native(native_object: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        match Vec::<String>::from_native(native_object.clone()) {
            Ok(value) => Ok(Self::Vector(value)),
            Err(_) => match String::from_native(native_object) {
                Ok(value) => Ok(Self::Single(value)),
                Err(_) => Err(CubeError::user(format!(
                    "Join hint item expected to be string or vector of strings"
                ))),
            },
        }
    }
}
