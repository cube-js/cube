//! Test endpoints for the Tesseract bridge layer.
//!
//! These functions are exported on the native module under names prefixed with
//! `__testBridge` (e.g. `__testBridgeCompileMemberSql`). They drive real V8
//! through the production bridge code so
//! that bridge logic can be regression-tested at the unit level rather than
//! only via end-to-end JS planner tests.
//!
//! Stub implementations for trait dependencies (e.g. `BaseTools`) live in this
//! module; they should fail loudly when an unsupported code path is exercised.

use cubenativeutils::wrappers::neon::neon_guarded_funcion_call;
use cubenativeutils::wrappers::object::{NativeArray, NativeFunction, NativeStruct, NativeType};
use cubenativeutils::wrappers::serializer::NativeSerialize;
use cubenativeutils::wrappers::{inner_types::InnerTypes, NativeContextHolder, NativeObjectHandle};
use cubenativeutils::CubeError;
use cubesqlplanner::cube_bridge::base_tools::BaseTools;
use cubesqlplanner::cube_bridge::driver_tools::DriverTools;
use cubesqlplanner::cube_bridge::filter_params_callback::{
    FilterParamsCallback, NativeFilterParamsCallback,
};
use cubesqlplanner::cube_bridge::join_definition::JoinDefinition;
use cubesqlplanner::cube_bridge::join_hints::JoinHintItem;
use cubesqlplanner::cube_bridge::member_sql::{
    FilterGroupItem, FilterParamsItem, MemberSql, NativeMemberSql, SqlTemplate, SqlTemplateArgs,
};
use cubesqlplanner::cube_bridge::pre_aggregation_obj::PreAggregationObj;
use cubesqlplanner::cube_bridge::security_context::{NativeSecurityContext, SecurityContext};
use cubesqlplanner::cube_bridge::sql_templates_render::SqlTemplatesRender;
use cubesqlplanner::cube_bridge::sql_utils::SqlUtils;
use neon::prelude::*;
use std::any::Any;
use std::rc::Rc;

struct StubBaseTools;

fn stub_err(method: &str) -> CubeError {
    CubeError::internal(format!(
        "StubBaseTools::{} called from bridge test harness — \
         this test path requires a real BaseTools implementation",
        method
    ))
}

impl BaseTools for StubBaseTools {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
    fn driver_tools(&self, _external: bool) -> Result<Rc<dyn DriverTools>, CubeError> {
        Err(stub_err("driver_tools"))
    }
    fn sql_templates(&self) -> Result<Rc<dyn SqlTemplatesRender>, CubeError> {
        Err(stub_err("sql_templates"))
    }
    fn sql_utils_for_rust(&self) -> Result<Rc<dyn SqlUtils>, CubeError> {
        Err(stub_err("sql_utils_for_rust"))
    }
    fn generate_time_series(
        &self,
        _granularity: String,
        _date_range: Vec<String>,
    ) -> Result<Vec<Vec<String>>, CubeError> {
        Err(stub_err("generate_time_series"))
    }
    fn generate_custom_time_series(
        &self,
        _granularity: String,
        _date_range: Vec<String>,
        _origin: String,
    ) -> Result<Vec<Vec<String>>, CubeError> {
        Err(stub_err("generate_custom_time_series"))
    }
    fn get_allocated_params(&self) -> Result<Vec<String>, CubeError> {
        Err(stub_err("get_allocated_params"))
    }
    fn all_cube_members(&self, _path: String) -> Result<Vec<String>, CubeError> {
        Err(stub_err("all_cube_members"))
    }
    fn interval_and_minimal_time_unit(&self, _interval: String) -> Result<Vec<String>, CubeError> {
        Err(stub_err("interval_and_minimal_time_unit"))
    }
    fn get_pre_aggregation_by_name(
        &self,
        _cube_name: String,
        _name: String,
    ) -> Result<Rc<dyn PreAggregationObj>, CubeError> {
        Err(stub_err("get_pre_aggregation_by_name"))
    }
    fn pre_aggregation_table_name(
        &self,
        _cube_name: String,
        _name: String,
    ) -> Result<String, CubeError> {
        Err(stub_err("pre_aggregation_table_name"))
    }
    fn join_tree_for_hints(
        &self,
        _hints: Vec<JoinHintItem>,
    ) -> Result<Rc<dyn JoinDefinition>, CubeError> {
        Err(stub_err("join_tree_for_hints"))
    }
}

fn handles_to_array<IT: InnerTypes>(
    items: Vec<NativeObjectHandle<IT>>,
    context: NativeContextHolder<IT>,
) -> Result<NativeObjectHandle<IT>, CubeError> {
    let arr = context.empty_array()?;
    for (i, item) in items.into_iter().enumerate() {
        arr.set(i as u32, item)?;
    }
    Ok(NativeObjectHandle::new(arr.into_object()))
}

fn template_to_native<IT: InnerTypes>(
    template: &SqlTemplate,
    context: NativeContextHolder<IT>,
) -> Result<NativeObjectHandle<IT>, CubeError> {
    match template {
        SqlTemplate::String(s) => s.to_native(context),
        SqlTemplate::StringVec(strings) => strings.to_native(context),
    }
}

fn filter_params_to_native<IT: InnerTypes>(
    items: &[FilterParamsItem],
    context: NativeContextHolder<IT>,
) -> Result<NativeObjectHandle<IT>, CubeError> {
    let serialized = items
        .iter()
        .map(|itm| itm.to_native(context.clone()))
        .collect::<Result<Vec<_>, _>>()?;
    handles_to_array(serialized, context)
}

fn filter_group_to_native<IT: InnerTypes>(
    group: &FilterGroupItem,
    context: NativeContextHolder<IT>,
) -> Result<NativeObjectHandle<IT>, CubeError> {
    let result = context.empty_struct()?;
    result.set_field(
        "filter_params",
        filter_params_to_native(&group.filter_params, context.clone())?,
    )?;
    Ok(NativeObjectHandle::new(result.into_object()))
}

fn args_to_native<IT: InnerTypes>(
    args: &SqlTemplateArgs,
    context: NativeContextHolder<IT>,
) -> Result<NativeObjectHandle<IT>, CubeError> {
    let result = context.empty_struct()?;
    result.set_field(
        "symbol_paths",
        args.symbol_paths.to_native(context.clone())?,
    )?;
    result.set_field(
        "filter_params",
        filter_params_to_native(&args.filter_params, context.clone())?,
    )?;
    let groups = args
        .filter_groups
        .iter()
        .map(|g| filter_group_to_native(g, context.clone()))
        .collect::<Result<Vec<_>, _>>()?;
    result.set_field("filter_groups", handles_to_array(groups, context.clone())?)?;
    let security_context = context.empty_struct()?;
    security_context.set_field(
        "values",
        args.security_context.values.to_native(context.clone())?,
    )?;
    result.set_field(
        "security_context",
        NativeObjectHandle::new(security_context.into_object()),
    )?;
    Ok(NativeObjectHandle::new(result.into_object()))
}

fn compile_member_sql_inner<IT: InnerTypes>(
    context_holder: NativeContextHolder<IT>,
    js_fn: NativeObjectHandle<IT>,
    security_context_obj: NativeObjectHandle<IT>,
) -> Result<NativeObjectHandle<IT>, CubeError> {
    let member_sql = NativeMemberSql::try_new(js_fn)?;
    let security_context: Rc<dyn SecurityContext> =
        Rc::new(NativeSecurityContext::try_new(security_context_obj)?);
    let base_tools: Rc<dyn BaseTools> = Rc::new(StubBaseTools);

    let (template, args) = member_sql.compile_template_sql(base_tools, security_context)?;

    let result = context_holder.empty_struct()?;
    result.set_field(
        "template",
        template_to_native(&template, context_holder.clone())?,
    )?;
    result.set_field("args", args_to_native(&args, context_holder.clone())?)?;
    Ok(NativeObjectHandle::new(result.into_object()))
}

fn compile_member_sql(cx: FunctionContext) -> JsResult<JsValue> {
    neon_guarded_funcion_call(
        cx,
        |context_holder: NativeContextHolder<_>,
         js_fn: NativeObjectHandle<_>,
         security_context_obj: NativeObjectHandle<_>| {
            compile_member_sql_inner(context_holder, js_fn, security_context_obj)
        },
    )
}

fn parse_args_names_inner<IT: InnerTypes>(
    context_holder: NativeContextHolder<IT>,
    js_fn: NativeObjectHandle<IT>,
) -> Result<NativeObjectHandle<IT>, CubeError> {
    let func = js_fn.to_function()?;
    let names = func.args_names()?;
    names.to_native(context_holder)
}

fn parse_args_names(cx: FunctionContext) -> JsResult<JsValue> {
    neon_guarded_funcion_call(
        cx,
        |context_holder: NativeContextHolder<_>, js_fn: NativeObjectHandle<_>| {
            parse_args_names_inner(context_holder, js_fn)
        },
    )
}

fn invoke_filter_params_callback_inner<IT: InnerTypes>(
    context_holder: NativeContextHolder<IT>,
    js_fn: NativeObjectHandle<IT>,
    args: Vec<String>,
) -> Result<NativeObjectHandle<IT>, CubeError> {
    let callback = NativeFilterParamsCallback::new(js_fn);
    let result = callback.call(&args)?;
    result.to_native(context_holder)
}

fn invoke_filter_params_callback(cx: FunctionContext) -> JsResult<JsValue> {
    neon_guarded_funcion_call(
        cx,
        |context_holder: NativeContextHolder<_>,
         js_fn: NativeObjectHandle<_>,
         args: Vec<String>| {
            invoke_filter_params_callback_inner(context_holder, js_fn, args)
        },
    )
}

pub fn register_module(cx: &mut ModuleContext) -> NeonResult<()> {
    cx.export_function("__testBridgeCompileMemberSql", compile_member_sql)?;
    cx.export_function("__testBridgeParseArgsNames", parse_args_names)?;
    cx.export_function(
        "__testBridgeInvokeFilterParamsCallback",
        invoke_filter_params_callback,
    )?;
    Ok(())
}
