use super::member_expression::{MemberExpressionDefinition, NativeMemberExpressionDefinition};
use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::serializer::NativeDeserialize;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub enum OptionsMember {
    MemberName(String),
    MemberExpression(Rc<dyn MemberExpressionDefinition>),
}

impl<IT: InnerTypes> NativeDeserialize<IT> for OptionsMember {
    fn from_native(native_object: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        match String::from_native(native_object.clone()) {
            Ok(name) => Ok(Self::MemberName(name)),
            Err(_) => match NativeMemberExpressionDefinition::from_native(native_object) {
                Ok(expr) => Ok(Self::MemberExpression(Rc::new(expr))),
                Err(_) => Err(CubeError::user(format!(
                    "Member name or member expression map expected"
                ))),
            },
        }
    }
}
