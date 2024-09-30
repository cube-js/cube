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
pub trait MeasureFilter {
    #[field]
    fn sql(&self) -> Result<Rc<dyn MemberSql>, CubeError>;
}

pub trait MeasureFiltersVec {
    fn items(&self) -> &Vec<Rc<dyn MeasureFilter>>;
}

pub struct NativeMeasureFiltersVec<IT: InnerTypes> {
    items: Vec<Rc<dyn MeasureFilter>>,
    phantom: PhantomData<IT>,
}

impl<IT: InnerTypes> NativeMeasureFiltersVec<IT> {
    pub fn try_new(native_items: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        let items = native_items
            .into_array()?
            .to_vec()?
            .into_iter()
            .map(|v| -> Result<Rc<dyn MeasureFilter>, CubeError> {
                Ok(Rc::new(NativeMeasureFilter::from_native(v)?))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            items,
            phantom: PhantomData::default(),
        })
    }
}

impl<IT: InnerTypes> MeasureFiltersVec for NativeMeasureFiltersVec<IT> {
    fn items(&self) -> &Vec<Rc<dyn MeasureFilter>> {
        &self.items
    }
}

impl<IT: InnerTypes> NativeDeserialize<IT> for NativeMeasureFiltersVec<IT> {
    fn from_native(v: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        Self::try_new(v)
    }
}
