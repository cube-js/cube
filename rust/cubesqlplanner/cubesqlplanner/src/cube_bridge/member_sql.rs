use super::{
    filter_group::{FilterGroup, NativeFilterGroup},
    filter_params::{FilterParams, NativeFilterParams},
    security_context::{NativeSecurityContext, SecurityContext},
    sql_utils::{NativeSqlUtils, SqlUtils},
};
use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::make_proxy;
use cubenativeutils::wrappers::object::{NativeFunction, NativeStruct, NativeType};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::HashMap;
use std::rc::Rc;
use std::{any::Any, cell::RefCell, rc::Weak};

#[derive(Clone, Debug)]
pub struct TemplatedSql {
    pub args: HashMap<Vec<String>, usize>,
    pub template: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FilterParamsItem {
    pub cube_name: String,
    pub name: String,
    pub column: String,
}

#[derive(Default, Clone, Debug)]
struct FilterGroupItem {
    pub filter_params: Vec<FilterParamsItem>,
}

#[derive(Default, Clone, Debug)]
struct ProxyStateInner {
    pub args: HashMap<Vec<String>, usize>,
    pub filter_params: Vec<(FilterParamsItem, usize)>,
    pub filter_groups: Vec<(FilterGroupItem, usize)>,
}

struct ProxyState {
    state: RefCell<ProxyStateInner>,
}

impl ProxyState {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            state: RefCell::new(ProxyStateInner::default()),
        })
    }

    fn get_args(self: &Rc<Self>) -> Result<HashMap<Vec<String>, usize>, CubeError> {
        self.with_state(|state| state.args.clone())
    }

    fn get_state(self: &Rc<Self>) -> Result<ProxyStateInner, CubeError> {
        self.with_state(|state| state.clone())
    }

    fn weak(self: &Rc<Self>) -> ProxyStateWeak {
        ProxyStateWeak {
            state: Rc::downgrade(self),
        }
    }

    fn with_state<T, F>(&self, f: F) -> Result<T, CubeError>
    where
        F: FnOnce(&ProxyStateInner) -> T,
    {
        let state = self
            .state
            .try_borrow()
            .map_err(|_| CubeError::internal(format!("Cant borrow dependency parsing state")))?;
        Ok(f(&state))
    }
    fn with_state_mut<T, F>(&self, f: F) -> Result<T, CubeError>
    where
        F: FnOnce(&mut ProxyStateInner) -> T,
    {
        let mut state = self
            .state
            .try_borrow_mut()
            .map_err(|_| CubeError::internal(format!("Cant borrow dependency parsing state")))?;
        Ok(f(&mut state))
    }
}

#[derive(Clone)]
struct ProxyStateWeak {
    state: Weak<ProxyState>,
}

impl ProxyStateWeak {
    fn with_state<T, F>(&self, f: F) -> Result<T, CubeError>
    where
        F: FnOnce(&ProxyStateInner) -> T,
    {
        let state = self.state.upgrade().ok_or(CubeError::internal(format!(
            "Cant upgrade dependency parsing state"
        )))?;
        state.with_state(f)
    }

    fn with_state_mut<T, F>(&self, f: F) -> Result<T, CubeError>
    where
        F: FnOnce(&mut ProxyStateInner) -> T,
    {
        let state = self.state.upgrade().ok_or(CubeError::internal(format!(
            "Cant upgrade dependency parsing state"
        )))?;
        state.with_state_mut(f)
    }

    fn add_arg(&self, path: &Vec<String>) -> Result<usize, CubeError> {
        self.with_state_mut(|state| {
            if let Some(ind) = state.args.get(path) {
                ind.clone()
            } else {
                let ind = state.args.len();
                state.args.insert(path.clone(), ind);
                ind
            }
        })
    }
}

#[derive(Default)]
pub struct MemberSqlStruct {
    pub sql_fn: Option<String>,
    pub to_string_fn: Option<String>,
    pub properties: HashMap<String, MemberSqlArg>,
}

pub enum ContextSymbolArg {
    SecurityContext(Rc<dyn SecurityContext>),
    SqlUtils(Rc<dyn SqlUtils>),
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
    fn need_deps_resolve(&self) -> bool;
    fn as_any(self: Rc<Self>) -> Rc<dyn Any>;
    fn into_template_sql(&self) -> Result<TemplatedSql, CubeError>;
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
                ContextSymbolArg::SqlUtils(context) => context
                    .clone()
                    .as_any()
                    .downcast::<NativeSqlUtils<IT>>()
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

    fn property_proxy<CIT: InnerTypes>(
        context_holder: NativeContextHolder<CIT>,
        proxy_state: ProxyStateWeak,
        path: Vec<String>,
    ) -> Result<NativeObjectHandle<CIT>, CubeError> {
        context_holder.make_proxy(None, move |inner_context, _, prop| {
            if prop == "sql" {
                let mut path_with_sql = path.clone();
                path_with_sql.push("sql".to_string());
                let index = proxy_state.add_arg(&path_with_sql)?;
                let str = format!("{{prop:{}}}", index);
                let result = inner_context.to_string_fn(str)?;
                let result = NativeObjectHandle::new(result.into_object());
                return Ok(Some(result));
            }
            if prop == "toString" || prop == "valueOf" {
                let index = proxy_state.add_arg(&path)?;
                let str = format!("{{prop:{}}}", index);
                let result = inner_context.to_string_fn(str)?;
                let result = NativeObjectHandle::new(result.into_object());
                return Ok(Some(result));
            }

            let mut new_path = path.clone();
            new_path.push(prop.clone());
            let result =
                Self::property_proxy(inner_context.clone(), proxy_state.clone(), new_path)?;

            Ok(Some(result))
        })
    }

    fn filter_goup_fn<CIT: InnerTypes>(
        context_holder: NativeContextHolder<CIT>,
        proxy_state: ProxyStateWeak,
    ) -> Result<NativeObjectHandle<CIT>, CubeError> {
        let proxy_state = proxy_state.clone();
        let result = context_holder.make_vararg_function(move |function_context, args| {
            let filter_params = args
                .iter()
                .map(|arg| -> Result<_, CubeError> {
                    let member = arg.to_struct()?.get_field("__member")?;
                    FilterParamsItem::from_native(member.clone())
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| {
                    CubeError::user(
                        "FILTER_GROUP expects FILTER_PARAMS args to be passed.".to_string(),
                    )
                })?;
            let filter_group = FilterGroupItem { filter_params };
            let index = proxy_state.with_state_mut(|state| {
                let i = state.filter_groups.len();
                state.filter_groups.push((filter_group, i));
                i
            })?;

            let str = format!("{{fg:{}}}", index);
            Ok(str)
        })?;
        Ok(NativeObjectHandle::new(result.into_object()))
    }

    fn filter_params_filter<CIT: InnerTypes>(
        context_holder: NativeContextHolder<CIT>,
        proxy_state: ProxyStateWeak,
        cube_name: String,
        name: String,
        column: String,
    ) -> Result<NativeObjectHandle<CIT>, CubeError> {
        let item = Rc::new(FilterParamsItem {
            cube_name: cube_name.clone(),
            name: name.clone(),
            column,
        });
        let item_native = item.to_native(context_holder.clone())?;
        let to_string_fn = context_holder.make_function(move |_| {
            let index = proxy_state.with_state_mut(|state| {
                let i = state.filter_params.len();
                state.filter_params.push((item.as_ref().clone(), i));
                i
            })?;

            let str = format!("{{fp:{}}}", index);
            Ok(str)
        })?;
        let result = context_holder.empty_struct()?;
        result.set_field("__member", item_native)?;
        result.set_field(
            "toString",
            NativeObjectHandle::new(to_string_fn.into_object()),
        )?;
        Ok(NativeObjectHandle::new(result.into_object()))
    }

    fn filter_params_cube_proxy<CIT: InnerTypes>(
        context_holder: NativeContextHolder<CIT>,
        proxy_state: ProxyStateWeak,
        cube_name: String,
    ) -> Result<NativeObjectHandle<CIT>, CubeError> {
        context_holder.make_proxy(None, move |inner_context, _, prop| {
            let name = prop.clone();
            let cube_name_to_move = Rc::new(cube_name.clone());
            let proxy_state = proxy_state.clone();
            let filter_func =
                inner_context.make_function(move |filter_context, column: String| {
                    Self::filter_params_filter(
                        filter_context,
                        proxy_state.clone(),
                        cube_name_to_move.as_ref().clone(),
                        name.clone(),
                        column.clone(),
                    )
                })?;
            let filter_func = NativeObjectHandle::new(filter_func.into_object());
            let result_struct = inner_context.empty_struct()?;
            result_struct.set_field("filter", filter_func)?;
            Ok(Some(NativeObjectHandle::new(result_struct.into_object())))
        })
    }
    fn filter_params_proxy<CIT: InnerTypes>(
        context_holder: NativeContextHolder<CIT>,
        proxy_state: ProxyStateWeak,
    ) -> Result<NativeObjectHandle<CIT>, CubeError> {
        context_holder.make_proxy(None, move |inner_context, _, prop| {
            let cube_name = prop;
            Ok(Some(Self::filter_params_cube_proxy(
                inner_context,
                proxy_state.clone(),
                cube_name,
            )?))
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
    fn need_deps_resolve(&self) -> bool {
        !self.args_names.is_empty()
    }

    fn into_template_sql(&self) -> Result<TemplatedSql, CubeError> {
        let state = ProxyState::new();
        let weak_state = state.weak();
        let context_holder = NativeContextHolder::<IT>::new(self.native_object.get_context());
        let mut proxy_args = vec![];
        println!("!!!! ============");
        for arg in self.args_names.iter().cloned() {
            let proxy_arg = if arg == "FILTER_PARAMS" {
                println!("!!! --- filter params");
                Self::filter_params_proxy(context_holder.clone(), weak_state.clone())?
            } else if arg == "FILTER_GROUP" {
                println!("!!! --- filter group");
                Self::filter_goup_fn(context_holder.clone(), weak_state.clone())?
            } else {
                println!("!!! --- arg {}", arg);
                let path = vec![arg];
                Self::property_proxy(context_holder.clone(), weak_state.clone(), path.clone())?
            };
            proxy_args.push(proxy_arg);
        }
        println!("!!!! ------ {}", proxy_args.len());
        let native_func = self.native_object.to_function()?;
        println!("!!!! eeeee");
        let evaluation_result = native_func.call(proxy_args)?;
        println!("!!!! kkkk");
        if let Ok(t) = Vec::<String>::from_native(evaluation_result.clone()) {
            println!("!!!! fff {:?}", t);
        }
        let template = String::from_native(evaluation_result)?;
        println!("!!!!! state: {:#?}", state.get_state()?);
        let property_args = state.get_args()?;
        let result = TemplatedSql {
            args: property_args,
            template,
        };

        Ok(result)
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
