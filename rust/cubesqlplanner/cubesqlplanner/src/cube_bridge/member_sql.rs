use super::{
    filter_group::{FilterGroup, NativeFilterGroup},
    filter_params::{FilterParams, NativeFilterParams},
    proxy::CubeDepsCollectorProxyHandler,
    security_context::{NativeSecurityContext, SecurityContext},
    sql_utils::{NativeSqlUtils, SqlUtils},
};
use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::object::{NativeFunction, NativeRoot, NativeStruct, NativeType};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::wrappers::{NativeContextHolder, NativeContextHolderRef};
use cubenativeutils::wrappers::{RootHolder, Rootable};
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::hash_map::HashMap;
use std::rc::Rc;

#[derive(Default, Clone)]
pub struct MemberSqlStruct {
    pub sql_fn: Option<String>,
    pub to_string_fn: Option<String>,
    pub properties: HashMap<String, MemberSqlArg>,
}

#[derive(Clone)]
pub enum ContextSymbolArg {
    SecurityContext(Rc<dyn SecurityContext>),
    SqlUtils(Rc<dyn SqlUtils>),
    FilterParams(Rc<dyn FilterParams>),
    FilterGroup(Rc<dyn FilterGroup>),
}

#[derive(Clone)]
pub enum MemberSqlArg {
    String(String),
    Struct(MemberSqlStruct),
    ContextSymbol(ContextSymbolArg),
}

#[derive(Clone)]
pub enum MemberSqlArgForResolve {
    String(String),
    CubeProxy(Rc<CubeDepsCollectorProxyHandler>),
    ContextSymbol(ContextSymbolArg),
}

pub trait MemberSql {
    fn call(&self, args: Vec<MemberSqlArg>) -> Result<String, CubeError>;
    fn deps_resolve(&self, args: Vec<MemberSqlArgForResolve>) -> Result<String, CubeError>;
    fn args_names(&self) -> &Vec<String>;
    fn need_deps_resolve(&self) -> bool;
    fn as_any(self: Rc<Self>) -> Rc<dyn Any>;
}

pub struct NativeMemberSql<IT: InnerTypes> {
    native_object: NativeObjectHandle<IT>,
    args_names: Vec<String>,
}

impl<IT: InnerTypes> NativeSerialize<IT> for MemberSqlStruct {
    fn to_native(
        &self,
        context: Rc<NativeContextHolder<IT>>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        let res = context.empty_struct()?;
        for (k, v) in self.properties.iter() {
            res.set_field(k, v.to_native(context.clone())?)?;
        }
        if let Some(to_string_fn) = &self.to_string_fn {
            res.set_field(
                "toString",
                NativeObjectHandle::new(context.to_string_fn(to_string_fn.clone())?.into_object()),
            )?;
        }
        if let Some(sql_fn) = &self.sql_fn {
            res.set_field(
                "sql",
                NativeObjectHandle::new(context.to_string_fn(sql_fn.clone())?.into_object()),
            )?;
        }
        Ok(NativeObjectHandle::new(res.into_object()))
    }
}

impl<IT: InnerTypes> NativeSerialize<IT> for ContextSymbolArg {
    fn to_native(
        &self,
        context_holder: Rc<NativeContextHolder<IT>>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        match self {
            Self::SecurityContext(context) => context
                .clone()
                .as_any()
                .downcast::<NativeSecurityContext<IT>>()
                .unwrap()
                .to_native(context_holder.clone()),
            Self::SqlUtils(context) => context
                .clone()
                .as_any()
                .downcast::<NativeSqlUtils<IT>>()
                .unwrap()
                .to_native(context_holder.clone()),
            Self::FilterParams(params) => params
                .clone()
                .as_any()
                .downcast::<NativeFilterParams<IT>>()
                .unwrap()
                .to_native(context_holder.clone()),
            Self::FilterGroup(group) => group
                .clone()
                .as_any()
                .downcast::<NativeFilterGroup<IT>>()
                .unwrap()
                .to_native(context_holder.clone()),
        }
    }
}
impl<IT: InnerTypes> NativeSerialize<IT> for MemberSqlArg {
    fn to_native(
        &self,
        context_holder: Rc<NativeContextHolder<IT>>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        let res = match self {
            Self::String(s) => s.to_native(context_holder.clone()),
            Self::Struct(s) => s.to_native(context_holder.clone()),
            Self::ContextSymbol(s) => s.to_native(context_holder.clone()),
        }?;
        Ok(NativeObjectHandle::new(res.into_object()))
    }
}

impl<IT: InnerTypes> NativeSerialize<IT> for MemberSqlArgForResolve {
    fn to_native(
        &self,
        context_holder: Rc<NativeContextHolder<IT>>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        let res = match self {
            Self::String(s) => s.to_native(context_holder.clone()),
            Self::CubeProxy(proxy) => proxy.to_native(context_holder.clone()),
            Self::ContextSymbol(s) => s.to_native(context_holder.clone()),
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

    fn call_impl<T: NativeSerialize<IT>>(&self, args: Vec<T>) -> Result<String, CubeError> {
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
}

impl<IT: InnerTypes> MemberSql for NativeMemberSql<IT> {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }
    fn call(&self, args: Vec<MemberSqlArg>) -> Result<String, CubeError> {
        self.call_impl(args)
    }

    fn deps_resolve(&self, args: Vec<MemberSqlArgForResolve>) -> Result<String, CubeError> {
        self.call_impl(args)
    }

    fn args_names(&self) -> &Vec<String> {
        &self.args_names
    }
    fn need_deps_resolve(&self) -> bool {
        !self.args_names.is_empty()
    }
}

impl<IT: InnerTypes> NativeSerialize<IT> for NativeMemberSql<IT> {
    fn to_native(
        &self,
        _context: Rc<NativeContextHolder<IT>>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        Ok(self.native_object.clone())
    }
}
impl<IT: InnerTypes> NativeDeserialize<IT> for NativeMemberSql<IT> {
    fn from_native(native_object: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        Self::try_new(native_object)
    }
}

pub struct NativeRootMemberSql<IT: InnerTypes> {
    root: IT::Root,
}

impl<IT: InnerTypes> NativeRootMemberSql<IT> {
    pub fn new(root: IT::Root) -> Rc<Self> {
        Rc::new(Self { root })
    }
}

impl<IT: InnerTypes> RootHolder<dyn MemberSql> for NativeRootMemberSql<IT> {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn drop(
        self: Rc<Self>,
        context_holder_ref: Rc<dyn NativeContextHolderRef>,
    ) -> Result<(), CubeError> {
        let context_holder = context_holder_ref
            .as_any()
            .downcast::<NativeContextHolder<IT>>()
            .map_err(|_| {
                CubeError::internal(format!(
                    "Wrong context holder type for obtain NativeMemberSql from root holder"
                ))
            })?;
        self.root.drop_root(context_holder.context())
    }

    fn to_inner(
        self: Rc<Self>,
        context_holder_ref: Rc<dyn NativeContextHolderRef>,
    ) -> Result<Rc<dyn MemberSql>, CubeError> {
        let context_holder = context_holder_ref
            .as_any()
            .downcast::<NativeContextHolder<IT>>()
            .map_err(|_| {
                CubeError::internal(format!(
                    "Wrong context holder type for obtain NativeMemberSql from root holder"
                ))
            })?;
        let result = self.root.to_inner(context_holder.context())?;
        let result = Rc::new(NativeMemberSql::from_native(result)?);
        Ok(result)
    }
}

impl<IT: InnerTypes> Rootable<dyn MemberSql> for NativeMemberSql<IT> {
    fn to_root(self: Rc<Self>) -> Result<Rc<dyn RootHolder<dyn MemberSql>>, CubeError> {
        let native_root = self.native_object.to_root()?;
        Ok(NativeRootMemberSql::<IT>::new(native_root))
    }
}
