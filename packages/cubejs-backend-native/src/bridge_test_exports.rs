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

use cubenativeutils::wrappers::bridge_meta::BridgeFieldMeta;
use cubenativeutils::wrappers::neon::neon_guarded_funcion_call;
use cubenativeutils::wrappers::object::{NativeArray, NativeFunction, NativeStruct, NativeType};
use cubenativeutils::wrappers::serializer::NativeSerialize;
use cubenativeutils::wrappers::{inner_types::InnerTypes, NativeContextHolder, NativeObjectHandle};
use cubenativeutils::CubeError;
use cubesqlplanner::cube_bridge::{
    base_query_options::{base_query_options_bridge_fields_meta, NativeBaseQueryOptions},
    base_tools::{base_tools_bridge_fields_meta, BaseTools, NativeBaseTools},
    case_definition::{case_definition_bridge_fields_meta, NativeCaseDefinition},
    case_else_item::{case_else_item_bridge_fields_meta, NativeCaseElseItem},
    case_item::{case_item_bridge_fields_meta, NativeCaseItem},
    case_switch_definition::{
        case_switch_definition_bridge_fields_meta, NativeCaseSwitchDefinition,
    },
    case_switch_else_item::{case_switch_else_item_bridge_fields_meta, NativeCaseSwitchElseItem},
    case_switch_item::{case_switch_item_bridge_fields_meta, NativeCaseSwitchItem},
    cube_definition::{cube_definition_bridge_fields_meta, NativeCubeDefinition},
    dimension_definition::{dimension_definition_bridge_fields_meta, NativeDimensionDefinition},
    driver_tools::{driver_tools_bridge_fields_meta, DriverTools, NativeDriverTools},
    evaluator::{cube_evaluator_bridge_fields_meta, NativeCubeEvaluator},
    filter_group::{filter_group_bridge_fields_meta, NativeFilterGroup},
    filter_params::{filter_params_bridge_fields_meta, NativeFilterParams},
    filter_params_callback::{FilterParamsCallback, NativeFilterParamsCallback},
    geo_item::{geo_item_bridge_fields_meta, NativeGeoItem},
    granularity_definition::{
        granularity_definition_bridge_fields_meta, NativeGranularityDefinition,
    },
    join_definition::{join_definition_bridge_fields_meta, JoinDefinition, NativeJoinDefinition},
    join_graph::{join_graph_bridge_fields_meta, NativeJoinGraph},
    join_hints::JoinHintItem,
    join_item::{join_item_bridge_fields_meta, NativeJoinItem},
    join_item_definition::{join_item_definition_bridge_fields_meta, NativeJoinItemDefinition},
    measure_definition::{measure_definition_bridge_fields_meta, NativeMeasureDefinition},
    member_definition::{member_definition_bridge_fields_meta, NativeMemberDefinition},
    member_expression::{
        expression_struct_bridge_fields_meta, member_expression_definition_bridge_fields_meta,
        NativeExpressionStruct, NativeMemberExpressionDefinition,
    },
    member_order_by::{member_order_by_bridge_fields_meta, NativeMemberOrderBy},
    member_sql::{
        FilterGroupItem, FilterParamsItem, MemberSql, NativeMemberSql, SqlTemplate, SqlTemplateArgs,
    },
    pre_aggregation_description::{
        pre_aggregation_description_bridge_fields_meta, NativePreAggregationDescription,
    },
    pre_aggregation_obj::{
        pre_aggregation_obj_bridge_fields_meta, NativePreAggregationObj, PreAggregationObj,
    },
    pre_aggregation_time_dimension::{
        pre_aggregation_time_dimension_bridge_fields_meta, NativePreAggregationTimeDimension,
    },
    security_context::{
        security_context_bridge_fields_meta, NativeSecurityContext, SecurityContext,
    },
    segment_definition::{segment_definition_bridge_fields_meta, NativeSegmentDefinition},
    sql_templates_render::SqlTemplatesRender,
    sql_utils::{sql_utils_bridge_fields_meta, NativeSqlUtils, SqlUtils},
    struct_with_sql_member::{
        struct_with_sql_member_bridge_fields_meta, NativeStructWithSqlMember,
    },
    timeshift_definition::{time_shift_definition_bridge_fields_meta, NativeTimeShiftDefinition},
};
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

fn unknown_bridge_err(name: &str) -> CubeError {
    CubeError::user(format!(
        "Unknown bridge type: {} (test harness dispatcher does not register it)",
        name
    ))
}

macro_rules! bridge_registry {
    ( $( $key:literal => $native:ident , $meta_fn:path );* $(;)? ) => {
        fn fields_meta_for_bridge(name: &str) -> Result<Vec<BridgeFieldMeta>, CubeError> {
            match name {
                $( $key => Ok($meta_fn()), )*
                other => Err(unknown_bridge_err(other)),
            }
        }

        fn try_new_bridge<IT: InnerTypes>(
            name: &str,
            obj: NativeObjectHandle<IT>,
        ) -> Result<(), CubeError> {
            match name {
                $( $key => { $native::try_new(obj)?; Ok(()) } )*
                other => Err(unknown_bridge_err(other)),
            }
        }
    };
}

bridge_registry! {
    "baseQueryOptions"            => NativeBaseQueryOptions,            base_query_options_bridge_fields_meta;
    "baseTools"                   => NativeBaseTools,                   base_tools_bridge_fields_meta;
    "caseDefinition"              => NativeCaseDefinition,              case_definition_bridge_fields_meta;
    "caseElseItem"                => NativeCaseElseItem,                case_else_item_bridge_fields_meta;
    "caseItem"                    => NativeCaseItem,                    case_item_bridge_fields_meta;
    "caseSwitchDefinition"        => NativeCaseSwitchDefinition,        case_switch_definition_bridge_fields_meta;
    "caseSwitchElseItem"          => NativeCaseSwitchElseItem,          case_switch_else_item_bridge_fields_meta;
    "caseSwitchItem"              => NativeCaseSwitchItem,              case_switch_item_bridge_fields_meta;
    "cubeDefinition"              => NativeCubeDefinition,              cube_definition_bridge_fields_meta;
    "cubeEvaluator"               => NativeCubeEvaluator,               cube_evaluator_bridge_fields_meta;
    "dimensionDefinition"         => NativeDimensionDefinition,         dimension_definition_bridge_fields_meta;
    "driverTools"                 => NativeDriverTools,                 driver_tools_bridge_fields_meta;
    "expressionStruct"            => NativeExpressionStruct,            expression_struct_bridge_fields_meta;
    "filterGroup"                 => NativeFilterGroup,                 filter_group_bridge_fields_meta;
    "filterParams"                => NativeFilterParams,                filter_params_bridge_fields_meta;
    "geoItem"                     => NativeGeoItem,                     geo_item_bridge_fields_meta;
    "granularityDefinition"       => NativeGranularityDefinition,       granularity_definition_bridge_fields_meta;
    "joinDefinition"              => NativeJoinDefinition,              join_definition_bridge_fields_meta;
    "joinGraph"                   => NativeJoinGraph,                   join_graph_bridge_fields_meta;
    "joinItem"                    => NativeJoinItem,                    join_item_bridge_fields_meta;
    "joinItemDefinition"          => NativeJoinItemDefinition,          join_item_definition_bridge_fields_meta;
    "measureDefinition"           => NativeMeasureDefinition,           measure_definition_bridge_fields_meta;
    "memberDefinition"            => NativeMemberDefinition,            member_definition_bridge_fields_meta;
    "memberExpressionDefinition"  => NativeMemberExpressionDefinition,  member_expression_definition_bridge_fields_meta;
    "memberOrderBy"               => NativeMemberOrderBy,               member_order_by_bridge_fields_meta;
    "preAggregationDescription"   => NativePreAggregationDescription,   pre_aggregation_description_bridge_fields_meta;
    "preAggregationObj"           => NativePreAggregationObj,           pre_aggregation_obj_bridge_fields_meta;
    "preAggregationTimeDimension" => NativePreAggregationTimeDimension, pre_aggregation_time_dimension_bridge_fields_meta;
    "securityContext"             => NativeSecurityContext,             security_context_bridge_fields_meta;
    "segmentDefinition"           => NativeSegmentDefinition,           segment_definition_bridge_fields_meta;
    "sqlUtils"                    => NativeSqlUtils,                    sql_utils_bridge_fields_meta;
    "structWithSqlMember"         => NativeStructWithSqlMember,         struct_with_sql_member_bridge_fields_meta;
    "timeShiftDefinition"         => NativeTimeShiftDefinition,         time_shift_definition_bridge_fields_meta;
}

fn list_bridge_fields_inner<IT: InnerTypes>(
    context_holder: NativeContextHolder<IT>,
    name: String,
) -> Result<NativeObjectHandle<IT>, CubeError> {
    let meta = fields_meta_for_bridge(&name)?;
    let arr = context_holder.empty_array()?;
    for (i, m) in meta.iter().enumerate() {
        let entry = context_holder.empty_struct()?;
        entry.set_field(
            "name",
            m.name.to_string().to_native(context_holder.clone())?,
        )?;
        entry.set_field(
            "jsName",
            m.js_name.to_string().to_native(context_holder.clone())?,
        )?;
        entry.set_field(
            "kind",
            m.kind
                .as_str()
                .to_string()
                .to_native(context_holder.clone())?,
        )?;
        entry.set_field("optional", m.optional.to_native(context_holder.clone())?)?;
        entry.set_field("vec", m.vec.to_native(context_holder.clone())?)?;
        arr.set(i as u32, NativeObjectHandle::new(entry.into_object()))?;
    }
    Ok(NativeObjectHandle::new(arr.into_object()))
}

fn list_bridge_fields(cx: FunctionContext) -> JsResult<JsValue> {
    neon_guarded_funcion_call(
        cx,
        |context_holder: NativeContextHolder<_>, name: String| {
            list_bridge_fields_inner(context_holder, name)
        },
    )
}

fn parse_bridge_inner<IT: InnerTypes>(
    context_holder: NativeContextHolder<IT>,
    name: String,
    obj: NativeObjectHandle<IT>,
) -> Result<NativeObjectHandle<IT>, CubeError> {
    try_new_bridge(&name, obj)?;
    true.to_native(context_holder)
}

fn parse_bridge(cx: FunctionContext) -> JsResult<JsValue> {
    neon_guarded_funcion_call(
        cx,
        |context_holder: NativeContextHolder<_>, name: String, obj: NativeObjectHandle<_>| {
            parse_bridge_inner(context_holder, name, obj)
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
    cx.export_function("__testBridgeListFields", list_bridge_fields)?;
    cx.export_function("__testBridgeParse", parse_bridge)?;
    Ok(())
}
