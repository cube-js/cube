use crate::cube_bridge::case_context_definition::{
    CaseContextDefinition, NativeCaseContextDefinition,
};
use crate::cube_bridge::case_definition::{CaseDefinition, NativeCaseDefinition};
use crate::cube_bridge::case_switch_definition::{
    CaseSwitchDefinition, NativeCaseSwitchDefinition,
};

use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::serializer::NativeDeserialize;
use cubenativeutils::wrappers::{NativeArray, NativeObjectHandle, NativeStruct};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub enum CaseVariant {
    Case(Rc<dyn CaseDefinition>),
    CaseSwitch(Rc<dyn CaseSwitchDefinition>),
    CaseContext(Rc<dyn CaseContextDefinition>),
}

// The three forms share `when`/`else`; only a `switch` field or a `context`
// field on the first `when` item tells them apart, and the trial
// deserialization below cannot see into `when`. A malformed `case` is not a
// context case; the trial deserialization reports what is wrong with it.
fn is_context_case<IT: InnerTypes>(native_object: &NativeObjectHandle<IT>) -> bool {
    let peek = || -> Option<bool> {
        let object = native_object.to_struct().ok()?;
        if object.has_field("switch").ok()? || !object.has_field("when").ok()? {
            return Some(false);
        }
        let when = object.get_field("when").ok()?.to_array().ok()?;
        if when.len().ok()? == 0 {
            return Some(false);
        }
        when.get(0)
            .ok()?
            .to_struct()
            .ok()?
            .has_field("context")
            .ok()
    };
    peek().unwrap_or(false)
}

impl<IT: InnerTypes> NativeDeserialize<IT> for CaseVariant {
    fn from_native(native_object: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        if is_context_case(&native_object) {
            let case = NativeCaseContextDefinition::from_native(native_object)?;
            return Ok(Self::CaseContext(Rc::new(case)));
        }
        let res = match NativeCaseSwitchDefinition::from_native(native_object.clone()) {
            Ok(case) => Ok(Self::CaseSwitch(Rc::new(case))),
            Err(_) => match NativeCaseDefinition::from_native(native_object) {
                Ok(case) => Ok(Self::Case(Rc::new(case))),
                Err(_) => Err(CubeError::user(format!("Case or Case Switch  expected"))),
            },
        };
        res
    }
}
