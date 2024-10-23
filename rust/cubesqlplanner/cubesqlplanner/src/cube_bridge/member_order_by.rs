use super::memeber_sql::{MemberSql, NativeMemberSql};
use cubenativeutils::wrappers::object::NativeArray;
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use std::any::Any;
use std::marker::PhantomData;
use std::rc::Rc;

#[nativebridge::native_bridge]
pub trait MemberOrderBy {
    #[field]
    fn sql(&self) -> Result<Rc<dyn MemberSql>, CubeError>;
    #[field]
    fn dir(&self) -> Result<String, CubeError>;
}

pub trait MemberOrderByVec {
    fn items(&self) -> &Vec<Rc<dyn MemberOrderBy>>;
}

pub struct NativeMemberOrderByVec<IT: InnerTypes> {
    items: Vec<Rc<dyn MemberOrderBy>>,
    phantom: PhantomData<IT>,
}

impl<IT: InnerTypes> NativeMemberOrderByVec<IT> {
    pub fn try_new(native_items: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        let items = native_items
            .into_array()?
            .to_vec()?
            .into_iter()
            .map(|v| -> Result<Rc<dyn MemberOrderBy>, CubeError> {
                Ok(Rc::new(NativeMemberOrderBy::from_native(v)?))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            items,
            phantom: PhantomData::default(),
        })
    }
}

impl<IT: InnerTypes> MemberOrderByVec for NativeMemberOrderByVec<IT> {
    fn items(&self) -> &Vec<Rc<dyn MemberOrderBy>> {
        &self.items
    }
}

impl<IT: InnerTypes> NativeDeserialize<IT> for NativeMemberOrderByVec<IT> {
    fn from_native(v: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        Self::try_new(v)
    }
}
