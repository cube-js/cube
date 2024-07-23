use super::context::NativeContext;
use super::object::{
    NativeArray, NativeBoolean, NativeNumber, NativeObject, NativeString, NativeStruct,
};
pub trait InnerTypes: Clone {
    type Object: NativeObject<Self>;
    type Struct: NativeStruct<Self>;
    type Array: NativeArray<Self>;
    type String: NativeString<Self>;
    type Boolean: NativeBoolean<Self>;
    type Number: NativeNumber<Self>;
    type Context: NativeContext<Self>;
}
