use super::{
    context::NativeContext,
    object::{
        NativeArray, NativeBoolean, NativeFunction, NativeNumber, NativeObject, NativeString,
        NativeStruct,
    },
};
pub trait InnerTypes: Clone + 'static {
    type Object: NativeObject<Self>;
    type Struct: NativeStruct<Self>;
    type Array: NativeArray<Self>;
    type String: NativeString<Self>;
    type Boolean: NativeBoolean<Self>;
    type Function: NativeFunction<Self>;
    type Number: NativeNumber<Self>;
    type Context: NativeContext<Self>;
}
