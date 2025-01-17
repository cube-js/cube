use super::{
    filter_group::{FilterGroup, NativeFilterGroup},
    filter_params::{FilterParams, NativeFilterParams},
    security_context::{NativeSecurityContext, SecurityContext},
};
use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::object::{NativeFunction, NativeStruct, NativeType};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::hash_map::HashMap;
use std::rc::Rc;

#[derive(Default)]
pub struct MemberSqlStruct {
    pub sql_fn: Option<String>,
    pub to_string_fn: Option<String>,
    pub properties: HashMap<String, MemberSqlArg>,
}

pub enum ContextSymbolArg {
    SecurityContext(Rc<dyn SecurityContext>),
    FilterParams(Rc<dyn FilterParams>),
    FilterGroup(Rc<dyn FilterGroup>),
}

pub enum MemberSqlArg {
    String(String),
    Struct(MemberSqlStruct),
    ContextSymbol(ContextSymbolArg),
}

pub trait MemberSql {
    fn call(&self, args: Vec<MemberSqlArg>) -> Result<String, CubeError>;
    fn args_names(&self) -> &Vec<String>;
    fn as_any(self: Rc<Self>) -> Rc<dyn Any>;
}

pub struct NativeMemberSql<IT: InnerTypes> {
    native_object: NativeObjectHandle<IT>,
    args_names: Vec<String>,
}

impl<IT: InnerTypes> NativeSerialize<IT> for MemberSqlStruct {
    fn to_native(
        &self,
        context: NativeContextHolder<IT>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        let res = context.empty_struct();
        for (k, v) in self.properties.iter() {
            res.set_field(k, v.to_native(context.clone())?)?;
        }
        if let Some(to_string_fn) = &self.to_string_fn {
            res.set_field(
                "toString",
                NativeObjectHandle::new(context.to_string_fn(to_string_fn.clone()).into_object()),
            )?;
        }
        Ok(NativeObjectHandle::new(res.into_object()))
    }
}

impl<IT: InnerTypes> NativeSerialize<IT> for MemberSqlArg {
    fn to_native(
        &self,
        context_holder: NativeContextHolder<IT>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        let res = match self {
            MemberSqlArg::String(s) => s.to_native(context_holder.clone()),
            MemberSqlArg::Struct(s) => s.to_native(context_holder.clone()),
            MemberSqlArg::ContextSymbol(symbol) => match symbol {
                ContextSymbolArg::SecurityContext(context) => context
                    .clone()
                    .as_any()
                    .downcast::<NativeSecurityContext<IT>>()
                    .unwrap()
                    .to_native(context_holder.clone()),
                ContextSymbolArg::FilterParams(params) => params
                    .clone()
                    .as_any()
                    .downcast::<NativeFilterParams<IT>>()
                    .unwrap()
                    .to_native(context_holder.clone()),
                ContextSymbolArg::FilterGroup(group) => group
                    .clone()
                    .as_any()
                    .downcast::<NativeFilterGroup<IT>>()
                    .unwrap()
                    .to_native(context_holder.clone()),
            },
        }?;
        Ok(NativeObjectHandle::new(res.into_object()))
    }
}

impl<IT: InnerTypes> NativeMemberSql<IT> {
    pub fn try_new(native_object: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        let args_names = native_object.to_function()?.args_names()?;
        Ok(Self {
            native_object,
            args_names,
        })
    }
}

impl<IT: InnerTypes> MemberSql for NativeMemberSql<IT> {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }
    fn call(&self, args: Vec<MemberSqlArg>) -> Result<String, CubeError> {
        if args.len() != self.args_names.len() {
            return Err(CubeError::internal(format!(
                "Invalid arguments count for MemberSql call: expected {}, got {}",
                self.args_names.len(),
                args.len()
            )));
        }
        let context_holder = NativeContextHolder::<IT>::new(self.native_object.get_context());
        let native_args = args
            .into_iter()
            .map(|a| a.to_native(context_holder.clone()))
            .collect::<Result<Vec<_>, _>>()?;

        let res = self.native_object.to_function()?.call(native_args)?;
        NativeDeserializer::deserialize::<IT, String>(res)
    }
    fn args_names(&self) -> &Vec<String> {
        &self.args_names
    }
}

impl<IT: InnerTypes> NativeSerialize<IT> for NativeMemberSql<IT> {
    fn to_native(
        &self,
        _context: NativeContextHolder<IT>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        Ok(self.native_object.clone())
    }
}
impl<IT: InnerTypes> NativeDeserialize<IT> for NativeMemberSql<IT> {
    fn from_native(native_object: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        Self::try_new(native_object)
    }
}
