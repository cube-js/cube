use super::filter_params_callback::{FilterParamsCallback, NativeFilterParamsCallback};
use super::{
    security_context::{NativeSecurityContext, SecurityContext},
    sql_utils::NativeSqlUtils,
};
use crate::cube_bridge::base_tools::BaseTools;
use crate::planner::sql_evaluator::SqlCallArg;
use crate::utils::UniqueVector;
use cubenativeutils::wrappers::object::{NativeFunction, NativeStruct, NativeType};
use cubenativeutils::wrappers::serializer::{NativeDeserialize, NativeSerialize};
use cubenativeutils::wrappers::NativeContextHolderRef;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::wrappers::{inner_types::InnerTypes, NativeString};
use cubenativeutils::wrappers::{NativeArray, NativeContextHolder};
use cubenativeutils::CubeError;
use std::rc::Rc;
use std::{any::Any, cell::RefCell, rc::Weak};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlTemplate {
    String(String),
    StringVec(Vec<String>),
}

impl<IT: InnerTypes> NativeDeserialize<IT> for SqlTemplate {
    fn from_native(v: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        if let Ok(vec) = v.to_array() {
            let mut result = vec![];
            for v in vec.to_vec()? {
                let string_value = v.convert_to_string()?;

                result.push(string_value);
            }
            Ok(SqlTemplate::StringVec(result))
        } else {
            let val = v.convert_to_string()?;

            Ok(SqlTemplate::String(val))
        }
    }
}

#[derive(Clone)]
pub enum FilterParamsColumn {
    String(String),
    Callback(Rc<dyn FilterParamsCallback>),
}

impl FilterParamsColumn {
    fn clone_to_context(
        &self,
        context_ref: &dyn NativeContextHolderRef,
    ) -> Result<Self, CubeError> {
        let res = match self {
            Self::String(s) => Self::String(s.clone()),
            Self::Callback(callback) => Self::Callback(callback.clone_to_context(context_ref)?),
        };
        Ok(res)
    }
}

impl<IT: InnerTypes> NativeSerialize<IT> for FilterParamsColumn {
    fn to_native(
        &self,
        context: NativeContextHolder<IT>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        match self {
            FilterParamsColumn::String(s) => s.to_native(context.clone()),
            FilterParamsColumn::Callback(cb) => {
                if let Ok(callback) = cb
                    .clone()
                    .as_any()
                    .downcast::<NativeFilterParamsCallback<IT>>()
                {
                    callback.to_native(context.clone())
                } else {
                    Err(CubeError::internal(
                        "Cannot downcast filter params callback".to_string(),
                    ))
                }
            }
        }
    }
}
impl<IT: InnerTypes> NativeDeserialize<IT> for FilterParamsColumn {
    fn from_native(native_object: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        let column = if let Ok(string_column) = String::from_native(native_object.clone()) {
            FilterParamsColumn::String(string_column)
        } else {
            let callback = NativeFilterParamsCallback::from_native(native_object.clone())?;
            FilterParamsColumn::Callback(Rc::new(callback))
        };
        Ok(column)
    }
}

impl std::fmt::Debug for FilterParamsColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(arg0) => f.debug_tuple("String").field(arg0).finish(),
            Self::Callback(_) => f
                .debug_tuple("Callback")
                .field(&"JsFunc".to_string())
                .finish(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FilterParamsItem {
    pub cube_name: String,
    pub name: String,
    pub column: FilterParamsColumn,
}

impl FilterParamsItem {
    fn clone_to_context(
        &self,
        context_ref: &dyn NativeContextHolderRef,
    ) -> Result<Self, CubeError> {
        Ok(Self {
            cube_name: self.cube_name.clone(),
            name: self.name.clone(),
            column: self.column.clone_to_context(context_ref)?,
        })
    }
}

impl<IT: InnerTypes> NativeSerialize<IT> for FilterParamsItem {
    fn to_native(
        &self,
        context: NativeContextHolder<IT>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        let result = context.empty_struct()?;
        result.set_field("cube_name", self.cube_name.to_native(context.clone())?)?;
        result.set_field("name", self.name.to_native(context.clone())?)?;
        result.set_field("column", self.column.to_native(context.clone())?)?;

        Ok(NativeObjectHandle::new(result.into_object()))
    }
}
impl<IT: InnerTypes> NativeDeserialize<IT> for FilterParamsItem {
    fn from_native(native_object: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        let object = native_object.to_struct()?;
        let cube_name = String::from_native(object.get_field("cube_name")?)?;
        let name = String::from_native(object.get_field("name")?)?;
        let native_column = object.get_field("column")?;
        let column = FilterParamsColumn::from_native(native_column)?;
        let result = Self {
            cube_name,
            name,
            column,
        };
        Ok(result)
    }
}

#[derive(Default, Clone, Debug)]
pub struct FilterGroupItem {
    pub filter_params: Vec<FilterParamsItem>,
}

impl FilterGroupItem {
    fn clone_to_context(
        &self,
        context_ref: &dyn NativeContextHolderRef,
    ) -> Result<Self, CubeError> {
        let filter_params = self
            .filter_params
            .iter()
            .map(|itm| itm.clone_to_context(context_ref))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { filter_params })
    }
}

#[derive(Default, Clone, Debug)]
pub struct SecutityContextProps {
    pub values: Vec<String>,
}

#[derive(Default, Clone, Debug)]
pub struct SqlTemplateArgs {
    pub symbol_paths: Vec<Vec<String>>,
    pub filter_params: Vec<FilterParamsItem>,
    pub filter_groups: Vec<FilterGroupItem>,
    pub security_context: SecutityContextProps,
}

impl SqlTemplateArgs {
    pub fn insert_symbol_path(&mut self, path: Vec<String>) -> usize {
        self.symbol_paths.unique_insert(path)
    }

    pub fn insert_filter_params(&mut self, params: FilterParamsItem) -> usize {
        let index = self.filter_params.len();
        self.filter_params.push(params);
        index
    }

    pub fn insert_filter_group(&mut self, group: FilterGroupItem) -> usize {
        let index = self.filter_groups.len();
        self.filter_groups.push(group);
        index
    }

    pub fn insert_security_context_value(&mut self, value: String) -> usize {
        self.security_context.values.unique_insert(value)
    }

    pub fn clone_to_context(
        &self,
        context_ref: &dyn NativeContextHolderRef,
    ) -> Result<Self, CubeError> {
        let filter_params = self
            .filter_params
            .iter()
            .map(|itm| itm.clone_to_context(context_ref))
            .collect::<Result<Vec<_>, _>>()?;
        let filter_groups = self
            .filter_groups
            .iter()
            .map(|itm| itm.clone_to_context(context_ref))
            .collect::<Result<Vec<_>, _>>()?;
        let result = Self {
            symbol_paths: self.symbol_paths.clone(),
            filter_params,
            filter_groups,
            security_context: self.security_context.clone(),
        };
        Ok(result)
    }
}

struct ProxyState {
    state: RefCell<SqlTemplateArgs>,
}

impl ProxyState {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            state: RefCell::new(SqlTemplateArgs::default()),
        })
    }

    fn get_args(self: &Rc<Self>) -> Result<SqlTemplateArgs, CubeError> {
        self.with_state(|state| state.clone())
    }

    fn weak(self: &Rc<Self>) -> ProxyStateWeak {
        ProxyStateWeak {
            state: Rc::downgrade(self),
        }
    }

    fn with_state<T, F>(&self, f: F) -> Result<T, CubeError>
    where
        F: FnOnce(&SqlTemplateArgs) -> T,
    {
        let state = self
            .state
            .try_borrow()
            .map_err(|_| CubeError::internal(format!("Cant borrow dependency parsing state")))?;
        Ok(f(&state))
    }
    fn with_state_mut<T, F>(&self, f: F) -> Result<T, CubeError>
    where
        F: FnOnce(&mut SqlTemplateArgs) -> T,
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
    fn with_state_mut<T, F>(&self, f: F) -> Result<T, CubeError>
    where
        F: FnOnce(&mut SqlTemplateArgs) -> T,
    {
        let state = self.state.upgrade().ok_or(CubeError::internal(format!(
            "Cant upgrade dependency parsing state"
        )))?;
        state.with_state_mut(f)
    }

    fn insert_symbol_path(&self, path: &Vec<String>) -> Result<usize, CubeError> {
        self.with_state_mut(|state| state.insert_symbol_path(path.clone()))
    }
}

pub trait MemberSql {
    fn args_names(&self) -> &Vec<String>;
    fn as_any(self: Rc<Self>) -> Rc<dyn Any>;
    fn compile_template_sql(
        &self,
        base_tools: Rc<dyn BaseTools>,
        security_context: Rc<dyn SecurityContext>,
    ) -> Result<(SqlTemplate, SqlTemplateArgs), CubeError>;
}

pub struct NativeMemberSql<IT: InnerTypes> {
    native_object: NativeObjectHandle<IT>,
    args_names: Vec<String>,
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
                path_with_sql.push("__sql_fn".to_string());
                let index = proxy_state.insert_symbol_path(&path_with_sql)?;
                let str = SqlCallArg::dependency(index);
                let result = inner_context.to_string_fn(str)?;
                let result = NativeObjectHandle::new(result.into_object());
                return Ok(Some(result));
            }
            if prop == "toString" || prop == "valueOf" {
                let index = proxy_state.insert_symbol_path(&path)?;
                let str = SqlCallArg::dependency(index);
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

    fn process_secutity_context_value(
        proxy_state: &ProxyStateWeak,
        value: &String,
    ) -> Result<String, CubeError> {
        let index = proxy_state.with_state_mut(|state| {
            let i = state.security_context.values.len();
            state.security_context.values.push(value.clone());
            i
        })?;
        Ok(SqlCallArg::security_value(index))
    }

    fn security_context_filter_fn<CIT: InnerTypes>(
        context_holder: NativeContextHolder<CIT>,
        property_value: NativeObjectHandle<CIT>,
        required: bool,
        proxy_state: ProxyStateWeak,
    ) -> Result<NativeObjectHandle<CIT>, CubeError> {
        enum ParamValue {
            String(String),
            StringVec(Vec<String>),
            None,
        }
        let param_value = if let Ok(prop_vec) = Vec::<String>::from_native(property_value.clone()) {
            ParamValue::StringVec(prop_vec)
        } else if let Ok(prop) = String::from_native(property_value.clone()) {
            ParamValue::String(prop)
        } else if property_value.is_undefined()? || property_value.is_null()? {
            ParamValue::None
        } else {
            return Err(CubeError::user(
                "Invalid param for security context".to_string(),
            ));
        };
        let result =
            context_holder.make_vararg_function(move |context, args| -> Result<_, CubeError> {
                if args.is_empty() {
                    return Ok("".to_string());
                }

                let column = args[0].clone();

                let res = match &param_value {
                    ParamValue::String(value) => {
                        let value = Self::process_secutity_context_value(&proxy_state, value)?;
                        if let Ok(column) = column.to_function() {
                            let native_value = value.to_native(context.clone())?;
                            let result = column.call(vec![native_value])?;
                            if let Ok(result) = result.to_string() {
                                result.value()?
                            } else {
                                "".to_string()
                            }
                        } else if let Ok(column) = column.to_string() {
                            let column_value = column.value()?;
                            format!("{} = {}", column_value, value)
                        } else {
                            "".to_string()
                        }
                    }
                    ParamValue::StringVec(items) => {
                        let values = items
                            .iter()
                            .map(|v| Self::process_secutity_context_value(&proxy_state, &v))
                            .collect::<Result<Vec<_>, _>>()?;

                        let values = values.to_native(context)?;

                        if let Ok(column) = column.to_function() {
                            let result = column.call(vec![values])?;
                            if let Ok(result) = result.to_string() {
                                result.value()?
                            } else {
                                "".to_string()
                            }
                        } else {
                            "".to_string()
                        }
                    }
                    ParamValue::None => {
                        if required {
                            let column_name = String::from_native(column).unwrap_or_default();
                            return Err(CubeError::user(format!(
                                "Filter for {} is required",
                                column_name
                            )));
                        }
                        "1 = 1".to_string()
                    }
                };

                Ok(res)
            })?;
        Ok(NativeObjectHandle::new(result.into_object()))
    }

    fn security_context_unsafe_value_fn<CIT: InnerTypes>(
        context_holder: NativeContextHolder<CIT>,
        property_value: NativeObjectHandle<CIT>,
    ) -> Result<NativeObjectHandle<CIT>, CubeError> {
        let result = context_holder.make_vararg_function(
            move |context, _| -> Result<NativeObjectHandle<_>, CubeError> {
                property_value.clone_to_function_context_ref(context.as_holder_ref())
            },
        )?;
        Ok(NativeObjectHandle::new(result.into_object()))
    }

    fn security_context_proxy<CIT: InnerTypes>(
        context_holder: NativeContextHolder<CIT>,
        proxy_state: ProxyStateWeak,
        base_object: NativeObjectHandle<CIT>,
    ) -> Result<NativeObjectHandle<CIT>, CubeError> {
        context_holder.make_proxy(Some(base_object), move |inner_context, target, prop| {
            if &prop == "filter" {
                return Ok(Some(Self::security_context_filter_fn(
                    inner_context.clone(),
                    target.clone(),
                    false,
                    proxy_state.clone(),
                )?));
            }
            if &prop == "requiredFilter" {
                return Ok(Some(Self::security_context_filter_fn(
                    inner_context.clone(),
                    target.clone(),
                    true,
                    proxy_state.clone(),
                )?));
            }
            if &prop == "unsafeValue" {
                return Ok(Some(Self::security_context_unsafe_value_fn(
                    inner_context,
                    target.clone(),
                )?));
            }
            let target_obj = target.to_struct()?;
            let property_value = target_obj.get_field(&prop)?;
            if property_value.to_struct().is_ok() {
                return Ok(Some(Self::security_context_proxy(
                    inner_context,
                    proxy_state.clone(),
                    property_value,
                )?));
            }

            let result = inner_context.empty_struct()?;
            result.set_field(
                "filter",
                Self::security_context_filter_fn(
                    inner_context.clone(),
                    property_value.clone(),
                    false,
                    proxy_state.clone(),
                )?,
            )?;
            result.set_field(
                "requiredFilter",
                Self::security_context_filter_fn(
                    inner_context.clone(),
                    property_value.clone(),
                    true,
                    proxy_state.clone(),
                )?,
            )?;
            result.set_field(
                "unsafeValue",
                Self::security_context_unsafe_value_fn(inner_context, target.clone())?,
            )?;
            let result = NativeObjectHandle::new(result.into_object());
            Ok(Some(result))
        })
    }

    fn filter_group_fn<CIT: InnerTypes>(
        context_holder: NativeContextHolder<CIT>,
        proxy_state: ProxyStateWeak,
    ) -> Result<NativeObjectHandle<CIT>, CubeError> {
        let proxy_state = proxy_state.clone();
        let result = context_holder.make_vararg_function(move |_, args| {
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
            let index =
                proxy_state.with_state_mut(|state| state.insert_filter_group(filter_group))?;

            let str = SqlCallArg::filter_group(index);
            Ok(str)
        })?;
        Ok(NativeObjectHandle::new(result.into_object()))
    }

    fn filter_params_filter<CIT: InnerTypes>(
        context_holder: NativeContextHolder<CIT>,
        proxy_state: ProxyStateWeak,
        cube_name: String,
        name: String,
        column: FilterParamsColumn,
    ) -> Result<NativeObjectHandle<CIT>, CubeError> {
        let item = Rc::new(FilterParamsItem {
            cube_name: cube_name.clone(),
            name: name.clone(),
            column,
        });
        let item_native = item.to_native(context_holder.clone())?;
        let to_string_fn = context_holder.make_function(move |_| {
            let index = proxy_state
                .with_state_mut(|state| state.insert_filter_params(item.as_ref().clone()))?;

            let str = SqlCallArg::filter_param(index);
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
            let filter_func = inner_context.make_function(
                move |filter_context, column: FilterParamsColumn| {
                    Self::filter_params_filter(
                        filter_context,
                        proxy_state.clone(),
                        cube_name_to_move.as_ref().clone(),
                        name.clone(),
                        column.clone(),
                    )
                },
            )?;
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
    fn args_names(&self) -> &Vec<String> {
        &self.args_names
    }

    fn compile_template_sql(
        &self,
        base_tools: Rc<dyn BaseTools>,
        security_context: Rc<dyn SecurityContext>,
    ) -> Result<(SqlTemplate, SqlTemplateArgs), CubeError> {
        let state = ProxyState::new();
        let weak_state = state.weak();
        let context_holder = NativeContextHolder::<IT>::new(self.native_object.get_context());
        let mut proxy_args = vec![];
        for arg in self.args_names.iter().cloned() {
            let proxy_arg = if arg == "FILTER_PARAMS" {
                Self::filter_params_proxy(context_holder.clone(), weak_state.clone())?
            } else if arg == "FILTER_GROUP" {
                Self::filter_group_fn(context_holder.clone(), weak_state.clone())?
            } else if arg == "SECURITY_CONTEXT"
                || arg == "security_context"
                || arg == "securityContext"
            {
                let context_obj = if let Some(security_context) = security_context
                    .clone()
                    .as_any()
                    .downcast_ref::<NativeSecurityContext<IT>>(
                ) {
                    security_context.to_native(context_holder.clone())?
                } else {
                    return Err(CubeError::internal(format!(
                        "Cannot dowcast security_context to native type"
                    )));
                };
                Self::security_context_proxy(
                    context_holder.clone(),
                    weak_state.clone(),
                    context_obj,
                )?
            } else if arg == "SQL_UTILS" {
                base_tools
                    .sql_utils_for_rust()?
                    .as_any()
                    .downcast::<NativeSqlUtils<IT>>()
                    .unwrap()
                    .to_native(context_holder.clone())?
            } else {
                let path = vec![arg];
                Self::property_proxy(context_holder.clone(), weak_state.clone(), path.clone())?
            };
            proxy_args.push(proxy_arg);
        }
        let native_func = self.native_object.to_function()?;
        let evaluation_result = native_func.call(proxy_args)?;
        let template = SqlTemplate::from_native(evaluation_result)?;
        let context_ref = context_holder.as_holder_ref();
        let sql_args = state.get_args()?.clone_to_context(context_ref)?;

        Ok((template, sql_args))
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
