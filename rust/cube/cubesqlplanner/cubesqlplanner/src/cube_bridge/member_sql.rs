use super::filter_params_callback::{FilterParamsCallback, NativeFilterParamsCallback};
use crate::utils::UniqueVector;
use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::object::{NativeFunction, NativeStruct, NativeType};
use cubenativeutils::wrappers::serializer::{NativeDeserialize, NativeSerialize};
use cubenativeutils::wrappers::NativeContextHolderRef;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::wrappers::{NativeArray, NativeContextHolder};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Result of evaluating a member's `sql` JS function: a single SQL
/// string, or — for pre-aggregation `dimensions:` / `measures:`
/// reference lists — one string per referenced member.
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

/// Column argument passed to
/// `FILTER_PARAMS.cube.member.filter(...)`: either a plain column
/// name string, or a JS callback that produces the SQL snippet.
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

fn deserialize_filter_params_vec<IT: InnerTypes>(
    handle: NativeObjectHandle<IT>,
) -> Result<Vec<FilterParamsItem>, CubeError> {
    handle
        .to_array()?
        .to_vec()?
        .into_iter()
        .map(FilterParamsItem::from_native)
        .collect()
}

impl<IT: InnerTypes> NativeDeserialize<IT> for FilterGroupItem {
    fn from_native(native_object: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        let object = native_object.to_struct()?;
        let filter_params = deserialize_filter_params_vec(object.get_field("filterParams")?)?;
        Ok(Self { filter_params })
    }
}

#[derive(Default, Clone, Debug)]
pub struct SecutityContextProps {
    pub values: Vec<String>,
}

/// Result of compiling a member `sql` function on the JS side: the produced
/// template plus the dependencies it recorded.
pub struct CompiledMemberTemplate {
    pub template: SqlTemplate,
    pub args: SqlTemplateArgs,
}

impl<IT: InnerTypes> NativeDeserialize<IT> for CompiledMemberTemplate {
    fn from_native(native_object: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        let object = native_object.to_struct()?;
        let template = SqlTemplate::from_native(object.get_field("template")?)?;
        let symbol_paths = Vec::<Vec<String>>::from_native(object.get_field("symbolPaths")?)?;
        let filter_params = deserialize_filter_params_vec(object.get_field("filterParams")?)?;
        let filter_groups = object
            .get_field("filterGroups")?
            .to_array()?
            .to_vec()?
            .into_iter()
            .map(FilterGroupItem::from_native)
            .collect::<Result<Vec<_>, _>>()?;
        let values = Vec::<String>::from_native(object.get_field("securityContextValues")?)?;
        Ok(Self {
            template,
            args: SqlTemplateArgs {
                symbol_paths,
                filter_params,
                filter_groups,
                security_context: SecutityContextProps { values },
            },
        })
    }
}

/// Dependencies collected while compiling a member `sql` function.
/// Each `{arg:N}` / `{fp:N}` / `{fg:N}` / `{sv:N}` placeholder in
/// the produced `SqlTemplate` indexes into one of these vectors.
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

/// A member's `sql:` function as provided by the JS schema compiler.
/// `compile_template_sql` invokes the function under proxied
/// arguments (`{CUBE}`, `FILTER_PARAMS`, `FILTER_GROUP`,
/// `SECURITY_CONTEXT`, `SQL_UTILS`) and returns the resulting SQL
/// template together with the dependencies the function touched.
pub trait MemberSql {
    fn args_names(&self) -> &Vec<String>;
    fn as_any(self: Rc<Self>) -> Rc<dyn Any>;
}

/// Neon-backed implementation of `MemberSql`. `compile_template_sql`
/// calls the JS function with proxy objects that record every
/// accessed member path, `FILTER_PARAMS` / `FILTER_GROUP` call, and
/// `SECURITY_CONTEXT.x.filter(...)` / `unsafeValue()` reference into
/// a shared state, then returns the produced template together with
/// that state as `SqlTemplateArgs`.
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
}

impl<IT: InnerTypes> MemberSql for NativeMemberSql<IT> {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
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
