use super::join_item_definition::{JoinItemDefinition, NativeJoinItemDefinition};
use cubenativeutils::wrappers::object::NativeArray;
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::marker::PhantomData;
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
pub struct JoinItemStatic {
    pub from: String,
    pub to: String,
    #[serde(rename = "originalFrom")]
    pub original_from: String,
    #[serde(rename = "originalTo")]
    pub original_to: String,
}

#[nativebridge::native_bridge(JoinItemStatic)]
pub trait JoinItem {
    #[field]
    fn join(&self) -> Result<Rc<dyn JoinItemDefinition>, CubeError>;
}

pub trait JoinItemsVec {
    fn items(&self) -> &Vec<Rc<dyn JoinItem>>;
}

pub struct NativeJoinItemsVec<IT: InnerTypes> {
    items: Vec<Rc<dyn JoinItem>>,
    phantom: PhantomData<IT>,
}

impl<IT: InnerTypes> NativeJoinItemsVec<IT> {
    pub fn try_new(native_items: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        let items = native_items
            .into_array()?
            .to_vec()?
            .into_iter()
            .map(|v| -> Result<Rc<dyn JoinItem>, CubeError> {
                Ok(Rc::new(NativeJoinItem::from_native(v)?))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            items,
            phantom: PhantomData::default(),
        })
    }
}

impl<IT: InnerTypes> JoinItemsVec for NativeJoinItemsVec<IT> {
    fn items(&self) -> &Vec<Rc<dyn JoinItem>> {
        &self.items
    }
}

impl<IT: InnerTypes> NativeDeserialize<IT> for NativeJoinItemsVec<IT> {
    fn from_native(v: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        Self::try_new(v)
    }
}
