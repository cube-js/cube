use super::struct_with_sql_member::{NativeStructWithSqlMember, StructWithSqlMember};
use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::serializer::NativeDeserialize;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub enum StringOrSql {
    String(String),
    MemberSql(Rc<dyn StructWithSqlMember>),
}

impl<IT: InnerTypes> NativeDeserialize<IT> for StringOrSql {
    fn from_native(native_object: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        match String::from_native(native_object.clone()) {
            Ok(label) => Ok(Self::String(label)),
            Err(_) => match NativeStructWithSqlMember::from_native(native_object) {
                Ok(obj) => Ok(Self::MemberSql(Rc::new(obj))),
                Err(_) => Err(CubeError::user(format!(
                    "String or object with sql property expected as label"
                ))),
            },
        }
    }
}
