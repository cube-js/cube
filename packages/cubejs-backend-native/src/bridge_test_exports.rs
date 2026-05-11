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

use cubenativeutils::wrappers::bridge_meta::{BridgeFieldKind, BridgeFieldMeta};
use cubenativeutils::wrappers::neon::neon_guarded_funcion_call;
use cubenativeutils::wrappers::object::{NativeArray, NativeFunction, NativeStruct, NativeType};
use cubenativeutils::wrappers::serializer::NativeSerialize;
use cubenativeutils::wrappers::{inner_types::InnerTypes, NativeContextHolder, NativeObjectHandle};
use cubenativeutils::CubeError;
use cubesqlplanner::cube_bridge::{
    base_query_options::{
        base_query_options_bridge_fields_meta, BaseQueryOptions, NativeBaseQueryOptions,
    },
    base_tools::{base_tools_bridge_fields_meta, BaseTools, NativeBaseTools},
    case_definition::{case_definition_bridge_fields_meta, CaseDefinition, NativeCaseDefinition},
    case_else_item::{case_else_item_bridge_fields_meta, CaseElseItem, NativeCaseElseItem},
    case_item::{case_item_bridge_fields_meta, CaseItem, NativeCaseItem},
    case_switch_definition::{
        case_switch_definition_bridge_fields_meta, CaseSwitchDefinition, NativeCaseSwitchDefinition,
    },
    case_switch_else_item::{
        case_switch_else_item_bridge_fields_meta, CaseSwitchElseItem, NativeCaseSwitchElseItem,
    },
    case_switch_item::{case_switch_item_bridge_fields_meta, CaseSwitchItem, NativeCaseSwitchItem},
    cube_definition::{cube_definition_bridge_fields_meta, CubeDefinition, NativeCubeDefinition},
    dimension_definition::{
        dimension_definition_bridge_fields_meta, DimensionDefinition, NativeDimensionDefinition,
    },
    driver_tools::{driver_tools_bridge_fields_meta, DriverTools, NativeDriverTools},
    evaluator::{cube_evaluator_bridge_fields_meta, CubeEvaluator, NativeCubeEvaluator},
    filter_group::{filter_group_bridge_fields_meta, NativeFilterGroup},
    filter_params::{filter_params_bridge_fields_meta, NativeFilterParams},
    filter_params_callback::{FilterParamsCallback, NativeFilterParamsCallback},
    geo_item::{geo_item_bridge_fields_meta, GeoItem, NativeGeoItem},
    granularity_definition::{
        granularity_definition_bridge_fields_meta, GranularityDefinition,
        NativeGranularityDefinition,
    },
    join_definition::{join_definition_bridge_fields_meta, JoinDefinition, NativeJoinDefinition},
    join_graph::{join_graph_bridge_fields_meta, JoinGraph, NativeJoinGraph},
    join_hints::JoinHintItem,
    join_item::{join_item_bridge_fields_meta, JoinItem, NativeJoinItem},
    join_item_definition::{
        join_item_definition_bridge_fields_meta, JoinItemDefinition, NativeJoinItemDefinition,
    },
    measure_definition::{
        measure_definition_bridge_fields_meta, MeasureDefinition, NativeMeasureDefinition,
    },
    member_definition::{
        member_definition_bridge_fields_meta, MemberDefinition, NativeMemberDefinition,
    },
    member_expression::{
        expression_struct_bridge_fields_meta, member_expression_definition_bridge_fields_meta,
        ExpressionStruct, MemberExpressionDefinition, NativeExpressionStruct,
        NativeMemberExpressionDefinition,
    },
    member_order_by::{member_order_by_bridge_fields_meta, MemberOrderBy, NativeMemberOrderBy},
    member_sql::{
        FilterGroupItem, FilterParamsItem, MemberSql, NativeMemberSql, SqlTemplate, SqlTemplateArgs,
    },
    pre_aggregation_description::{
        pre_aggregation_description_bridge_fields_meta, NativePreAggregationDescription,
        PreAggregationDescription,
    },
    pre_aggregation_obj::{
        pre_aggregation_obj_bridge_fields_meta, NativePreAggregationObj, PreAggregationObj,
    },
    pre_aggregation_time_dimension::{
        pre_aggregation_time_dimension_bridge_fields_meta, NativePreAggregationTimeDimension,
        PreAggregationTimeDimension,
    },
    security_context::{
        security_context_bridge_fields_meta, NativeSecurityContext, SecurityContext,
    },
    segment_definition::{
        segment_definition_bridge_fields_meta, NativeSegmentDefinition, SegmentDefinition,
    },
    sql_templates_render::SqlTemplatesRender,
    sql_utils::{sql_utils_bridge_fields_meta, NativeSqlUtils, SqlUtils},
    struct_with_sql_member::{
        struct_with_sql_member_bridge_fields_meta, NativeStructWithSqlMember, StructWithSqlMember,
    },
    timeshift_definition::{
        time_shift_definition_bridge_fields_meta, NativeTimeShiftDefinition, TimeShiftDefinition,
    },
};
use neon::prelude::*;
use std::any::Any;
use std::collections::HashSet;
use std::rc::Rc;

enum InvokeStatus {
    Ok,
    Err(String),
    Skipped(String),
}

struct InvokeResult {
    entries: Vec<(&'static str, InvokeStatus)>,
}

impl InvokeResult {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    fn record<T>(&mut self, name: &'static str, result: Result<T, CubeError>) {
        let status = match result {
            Ok(_) => InvokeStatus::Ok,
            Err(e) => InvokeStatus::Err(e.to_string()),
        };
        self.entries.push((name, status));
    }

    fn skip(&mut self, name: &'static str, reason: &'static str) {
        self.entries
            .push((name, InvokeStatus::Skipped(reason.to_string())));
    }

    fn invoked_names(&self) -> HashSet<&'static str> {
        self.entries.iter().map(|(n, _)| *n).collect()
    }

    fn to_native<IT: InnerTypes>(
        &self,
        ctx: NativeContextHolder<IT>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        let map = ctx.empty_struct()?;
        for (name, status) in &self.entries {
            let entry = ctx.empty_struct()?;
            match status {
                InvokeStatus::Ok => {
                    entry.set_field("status", "ok".to_string().to_native(ctx.clone())?)?;
                }
                InvokeStatus::Err(msg) => {
                    entry.set_field("status", "error".to_string().to_native(ctx.clone())?)?;
                    entry.set_field("message", msg.to_string().to_native(ctx.clone())?)?;
                }
                InvokeStatus::Skipped(reason) => {
                    entry.set_field("status", "skipped".to_string().to_native(ctx.clone())?)?;
                    entry.set_field("reason", reason.to_string().to_native(ctx.clone())?)?;
                }
            }
            map.set_field(name, NativeObjectHandle::new(entry.into_object()))?;
        }
        Ok(NativeObjectHandle::new(map.into_object()))
    }
}

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
    ( $( $key:literal => $native:ident , $meta_fn:path , $invoke_fn:path );* $(;)? ) => {
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

        fn invoke_bridge_dispatch<IT: InnerTypes>(
            name: &str,
            obj: NativeObjectHandle<IT>,
        ) -> Result<InvokeResult, CubeError> {
            match name {
                $( $key => {
                    let bridge = $native::try_new(obj)?;
                    Ok($invoke_fn(&bridge))
                } )*
                other => Err(unknown_bridge_err(other)),
            }
        }

        fn registered_bridge_names() -> &'static [&'static str] {
            &[ $( $key ),* ]
        }
    };
}

bridge_registry! {
    "baseQueryOptions"            => NativeBaseQueryOptions,            base_query_options_bridge_fields_meta,            invoke_base_query_options;
    "baseTools"                   => NativeBaseTools,                   base_tools_bridge_fields_meta,                   invoke_base_tools;
    "caseDefinition"              => NativeCaseDefinition,              case_definition_bridge_fields_meta,              invoke_case_definition;
    "caseElseItem"                => NativeCaseElseItem,                case_else_item_bridge_fields_meta,               invoke_case_else_item;
    "caseItem"                    => NativeCaseItem,                    case_item_bridge_fields_meta,                    invoke_case_item;
    "caseSwitchDefinition"        => NativeCaseSwitchDefinition,        case_switch_definition_bridge_fields_meta,       invoke_case_switch_definition;
    "caseSwitchElseItem"          => NativeCaseSwitchElseItem,          case_switch_else_item_bridge_fields_meta,        invoke_case_switch_else_item;
    "caseSwitchItem"              => NativeCaseSwitchItem,              case_switch_item_bridge_fields_meta,             invoke_case_switch_item;
    "cubeDefinition"              => NativeCubeDefinition,              cube_definition_bridge_fields_meta,              invoke_cube_definition;
    "cubeEvaluator"               => NativeCubeEvaluator,               cube_evaluator_bridge_fields_meta,               invoke_cube_evaluator;
    "dimensionDefinition"         => NativeDimensionDefinition,         dimension_definition_bridge_fields_meta,         invoke_dimension_definition;
    "driverTools"                 => NativeDriverTools,                 driver_tools_bridge_fields_meta,                 invoke_driver_tools;
    "expressionStruct"            => NativeExpressionStruct,            expression_struct_bridge_fields_meta,            invoke_expression_struct;
    "filterGroup"                 => NativeFilterGroup,                 filter_group_bridge_fields_meta,                 invoke_filter_group;
    "filterParams"                => NativeFilterParams,                filter_params_bridge_fields_meta,                invoke_filter_params;
    "geoItem"                     => NativeGeoItem,                     geo_item_bridge_fields_meta,                     invoke_geo_item;
    "granularityDefinition"       => NativeGranularityDefinition,       granularity_definition_bridge_fields_meta,       invoke_granularity_definition;
    "joinDefinition"              => NativeJoinDefinition,              join_definition_bridge_fields_meta,              invoke_join_definition;
    "joinGraph"                   => NativeJoinGraph,                   join_graph_bridge_fields_meta,                   invoke_join_graph;
    "joinItem"                    => NativeJoinItem,                    join_item_bridge_fields_meta,                    invoke_join_item;
    "joinItemDefinition"          => NativeJoinItemDefinition,          join_item_definition_bridge_fields_meta,         invoke_join_item_definition;
    "measureDefinition"           => NativeMeasureDefinition,           measure_definition_bridge_fields_meta,           invoke_measure_definition;
    "memberDefinition"            => NativeMemberDefinition,            member_definition_bridge_fields_meta,            invoke_member_definition;
    "memberExpressionDefinition"  => NativeMemberExpressionDefinition,  member_expression_definition_bridge_fields_meta, invoke_member_expression_definition;
    "memberOrderBy"               => NativeMemberOrderBy,               member_order_by_bridge_fields_meta,              invoke_member_order_by;
    "preAggregationDescription"   => NativePreAggregationDescription,   pre_aggregation_description_bridge_fields_meta,  invoke_pre_aggregation_description;
    "preAggregationObj"           => NativePreAggregationObj,           pre_aggregation_obj_bridge_fields_meta,          invoke_pre_aggregation_obj;
    "preAggregationTimeDimension" => NativePreAggregationTimeDimension, pre_aggregation_time_dimension_bridge_fields_meta, invoke_pre_aggregation_time_dimension;
    "securityContext"             => NativeSecurityContext,             security_context_bridge_fields_meta,             invoke_security_context;
    "segmentDefinition"           => NativeSegmentDefinition,           segment_definition_bridge_fields_meta,           invoke_segment_definition;
    "sqlUtils"                    => NativeSqlUtils,                    sql_utils_bridge_fields_meta,                    invoke_sql_utils;
    "structWithSqlMember"         => NativeStructWithSqlMember,         struct_with_sql_member_bridge_fields_meta,       invoke_struct_with_sql_member;
    "timeShiftDefinition"         => NativeTimeShiftDefinition,         time_shift_definition_bridge_fields_meta,        invoke_time_shift_definition;
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

fn invoke_bridge_inner<IT: InnerTypes>(
    context_holder: NativeContextHolder<IT>,
    name: String,
    obj: NativeObjectHandle<IT>,
) -> Result<NativeObjectHandle<IT>, CubeError> {
    let meta = fields_meta_for_bridge(&name)?;
    let result = invoke_bridge_dispatch(&name, obj)?;

    // Guard: per-bridge `invoke_<snake>` must touch every field/call that the
    // macro emits in meta. Drift here means a new trait method landed without
    // a matching invoke entry — silent coverage loss otherwise.
    let expected: HashSet<&'static str> = meta
        .iter()
        .filter(|m| matches!(m.kind, BridgeFieldKind::Field | BridgeFieldKind::Call))
        .map(|m| m.name)
        .collect();
    let invoked = result.invoked_names();
    if invoked != expected {
        let missing: Vec<_> = expected.difference(&invoked).copied().collect();
        let extra: Vec<_> = invoked.difference(&expected).copied().collect();
        return Err(CubeError::internal(format!(
            "invoke dispatcher out of sync with bridge_fields_meta for '{}': missing={:?}, extra={:?}",
            name, missing, extra,
        )));
    }

    result.to_native(context_holder)
}

fn invoke_bridge(cx: FunctionContext) -> JsResult<JsValue> {
    neon_guarded_funcion_call(
        cx,
        |context_holder: NativeContextHolder<_>, name: String, obj: NativeObjectHandle<_>| {
            invoke_bridge_inner(context_holder, name, obj)
        },
    )
}

fn list_bridge_names_inner<IT: InnerTypes>(
    context_holder: NativeContextHolder<IT>,
) -> Result<NativeObjectHandle<IT>, CubeError> {
    let names = registered_bridge_names();
    let arr = context_holder.empty_array()?;
    for (i, n) in names.iter().enumerate() {
        arr.set(i as u32, n.to_string().to_native(context_holder.clone())?)?;
    }
    Ok(NativeObjectHandle::new(arr.into_object()))
}

fn list_bridge_names(cx: FunctionContext) -> JsResult<JsValue> {
    neon_guarded_funcion_call(cx, |context_holder: NativeContextHolder<_>| {
        list_bridge_names_inner(context_holder)
    })
}

// ---------------------------------------------------------------------------
// Per-bridge invoke dispatchers.
//
// Every `#[nbridge(field)]` getter and every plain `#[nbridge]` call-method
// is invoked once. Field-getters get `r.record(name, bridge.method())` —
// success means the JS-side value was successfully read and deserialized.
// Call-methods get `r.record(name, bridge.method(<default-args>))` for
// methods whose argument types have an obvious default (String, bool, Vec,
// HashMap, JoinHintItem). Methods whose arguments include `Rc<dyn X>` or
// other custom types that cannot be synthesized in Rust without a real JS
// object are skipped via `r.skip(name, reason)`.
//
// The `invoke_bridge_inner` endpoint guards drift: if a method appears in
// `<snake>_bridge_fields_meta()` but is not recorded here, the whole
// invocation fails — silent coverage holes are not possible.
// ---------------------------------------------------------------------------

fn invoke_filter_group<IT: InnerTypes>(_b: &NativeFilterGroup<IT>) -> InvokeResult {
    InvokeResult::new()
}
fn invoke_filter_params<IT: InnerTypes>(_b: &NativeFilterParams<IT>) -> InvokeResult {
    InvokeResult::new()
}
fn invoke_security_context<IT: InnerTypes>(_b: &NativeSecurityContext<IT>) -> InvokeResult {
    InvokeResult::new()
}
fn invoke_sql_utils<IT: InnerTypes>(_b: &NativeSqlUtils<IT>) -> InvokeResult {
    InvokeResult::new()
}
fn invoke_pre_aggregation_obj<IT: InnerTypes>(_b: &NativePreAggregationObj<IT>) -> InvokeResult {
    InvokeResult::new()
}

fn invoke_geo_item<IT: InnerTypes>(b: &NativeGeoItem<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("sql", b.sql());
    r
}
fn invoke_struct_with_sql_member<IT: InnerTypes>(
    b: &NativeStructWithSqlMember<IT>,
) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("sql", b.sql());
    r
}
fn invoke_case_else_item<IT: InnerTypes>(b: &NativeCaseElseItem<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("label", b.label());
    r
}
fn invoke_case_item<IT: InnerTypes>(b: &NativeCaseItem<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("sql", b.sql());
    r.record("label", b.label());
    r
}
fn invoke_case_definition<IT: InnerTypes>(b: &NativeCaseDefinition<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("when", b.when());
    r.record("else_label", b.else_label());
    r
}
fn invoke_case_switch_else_item<IT: InnerTypes>(b: &NativeCaseSwitchElseItem<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("sql", b.sql());
    r
}
fn invoke_case_switch_item<IT: InnerTypes>(b: &NativeCaseSwitchItem<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("sql", b.sql());
    r
}
fn invoke_case_switch_definition<IT: InnerTypes>(
    b: &NativeCaseSwitchDefinition<IT>,
) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("switch", b.switch());
    r.record("when", b.when());
    r.record("else_sql", b.else_sql());
    r
}

fn invoke_member_order_by<IT: InnerTypes>(b: &NativeMemberOrderBy<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("sql", b.sql());
    r.record("dir", b.dir());
    r
}

fn invoke_member_definition<IT: InnerTypes>(b: &NativeMemberDefinition<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("sql", b.sql());
    r
}

fn invoke_segment_definition<IT: InnerTypes>(b: &NativeSegmentDefinition<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("sql", b.sql());
    r
}

fn invoke_join_item_definition<IT: InnerTypes>(b: &NativeJoinItemDefinition<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("sql", b.sql());
    r
}
fn invoke_join_item<IT: InnerTypes>(b: &NativeJoinItem<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("join", b.join());
    r
}
fn invoke_join_definition<IT: InnerTypes>(b: &NativeJoinDefinition<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("joins", b.joins());
    r
}
fn invoke_join_graph<IT: InnerTypes>(b: &NativeJoinGraph<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("build_join", b.build_join(vec![]));
    r
}

fn invoke_granularity_definition<IT: InnerTypes>(
    b: &NativeGranularityDefinition<IT>,
) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("sql", b.sql());
    r
}
fn invoke_pre_aggregation_time_dimension<IT: InnerTypes>(
    b: &NativePreAggregationTimeDimension<IT>,
) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("dimension", b.dimension());
    r
}

fn invoke_time_shift_definition<IT: InnerTypes>(b: &NativeTimeShiftDefinition<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("sql", b.sql());
    r
}

fn invoke_cube_definition<IT: InnerTypes>(b: &NativeCubeDefinition<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("sql_table", b.sql_table());
    r.record("sql", b.sql());
    r
}

fn invoke_dimension_definition<IT: InnerTypes>(b: &NativeDimensionDefinition<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("sql", b.sql());
    r.record("case", b.case());
    r.record("latitude", b.latitude());
    r.record("longitude", b.longitude());
    r.record("time_shift", b.time_shift());
    r.record("mask_sql", b.mask_sql());
    r
}

fn invoke_measure_definition<IT: InnerTypes>(b: &NativeMeasureDefinition<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("sql", b.sql());
    r.record("case", b.case());
    r.record("filters", b.filters());
    r.record("drill_filters", b.drill_filters());
    r.record("order_by", b.order_by());
    r.record("mask_sql", b.mask_sql());
    r
}

fn invoke_expression_struct<IT: InnerTypes>(b: &NativeExpressionStruct<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("add_filters", b.add_filters());
    r
}

fn invoke_member_expression_definition<IT: InnerTypes>(
    b: &NativeMemberExpressionDefinition<IT>,
) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("expression", b.expression());
    r
}

fn invoke_pre_aggregation_description<IT: InnerTypes>(
    b: &NativePreAggregationDescription<IT>,
) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("measure_references", b.measure_references());
    r.record("dimension_references", b.dimension_references());
    r.record("time_dimension_reference", b.time_dimension_reference());
    r.record("segment_references", b.segment_references());
    r.record("rollup_references", b.rollup_references());
    r.record("time_dimension_references", b.time_dimension_references());
    r
}

fn invoke_base_query_options<IT: InnerTypes>(b: &NativeBaseQueryOptions<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("measures", b.measures());
    r.record("dimensions", b.dimensions());
    r.record("segments", b.segments());
    r.record("cube_evaluator", b.cube_evaluator());
    r.record("base_tools", b.base_tools());
    r.record("join_graph", b.join_graph());
    r.record("security_context", b.security_context());
    r.record("join_hints", b.join_hints());
    r
}

fn invoke_base_tools<IT: InnerTypes>(b: &NativeBaseTools<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    r.record("driver_tools", b.driver_tools(false));
    r.record("sql_templates", b.sql_templates());
    r.record("sql_utils_for_rust", b.sql_utils_for_rust());
    r.record(
        "generate_time_series",
        b.generate_time_series("day".to_string(), vec![]),
    );
    r.record(
        "generate_custom_time_series",
        b.generate_custom_time_series("day".to_string(), vec![], "2024-01-01".to_string()),
    );
    r.record("get_allocated_params", b.get_allocated_params());
    r.record("all_cube_members", b.all_cube_members("Orders".to_string()));
    r.record(
        "interval_and_minimal_time_unit",
        b.interval_and_minimal_time_unit("1 day".to_string()),
    );
    r.record(
        "get_pre_aggregation_by_name",
        b.get_pre_aggregation_by_name("Orders".to_string(), "main".to_string()),
    );
    r.record(
        "pre_aggregation_table_name",
        b.pre_aggregation_table_name("Orders".to_string(), "main".to_string()),
    );
    r.record("join_tree_for_hints", b.join_tree_for_hints(vec![]));
    r
}

fn invoke_driver_tools<IT: InnerTypes>(b: &NativeDriverTools<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    let s = || "x".to_string();
    r.record("convert_tz", b.convert_tz(s()));
    r.record("time_grouped_column", b.time_grouped_column(s(), s()));
    r.record("sql_templates", b.sql_templates());
    r.record("timestamp_precision", b.timestamp_precision());
    r.record("time_stamp_cast", b.time_stamp_cast(s()));
    r.record("date_time_cast", b.date_time_cast(s()));
    r.record("in_db_time_zone", b.in_db_time_zone(s()));
    r.record("get_allocated_params", b.get_allocated_params());
    r.record("subtract_interval", b.subtract_interval(s(), s()));
    r.record("add_interval", b.add_interval(s(), s()));
    r.record("interval_string", b.interval_string(s()));
    r.record("add_timestamp_interval", b.add_timestamp_interval(s(), s()));
    r.record(
        "interval_and_minimal_time_unit",
        b.interval_and_minimal_time_unit(s()),
    );
    r.record("hll_init", b.hll_init(s()));
    r.record("hll_merge", b.hll_merge(s()));
    r.record("hll_cardinality_merge", b.hll_cardinality_merge(s()));
    r.record("count_distinct_approx", b.count_distinct_approx(s()));
    r.record(
        "support_generated_series_for_custom_td",
        b.support_generated_series_for_custom_td(),
    );
    r.record("date_bin", b.date_bin(s(), s(), s()));
    r
}

fn invoke_cube_evaluator<IT: InnerTypes>(b: &NativeCubeEvaluator<IT>) -> InvokeResult {
    let mut r = InvokeResult::new();
    let s = || "x".to_string();
    r.record("parse_path", b.parse_path(s(), s()));
    r.record("measure_by_path", b.measure_by_path(s()));
    r.record("dimension_by_path", b.dimension_by_path(s()));
    r.record("segment_by_path", b.segment_by_path(s()));
    r.record("cube_from_path", b.cube_from_path(s()));
    r.record("is_measure", b.is_measure(vec![s()]));
    r.record("is_dimension", b.is_dimension(vec![s()]));
    r.record("is_segment", b.is_segment(vec![s()]));
    r.record("cube_exists", b.cube_exists(s()));
    r.record("resolve_granularity", b.resolve_granularity(vec![s()]));
    r.record(
        "pre_aggregations_for_cube_as_array",
        b.pre_aggregations_for_cube_as_array(s()),
    );
    r.record(
        "pre_aggregation_description_by_name",
        b.pre_aggregation_description_by_name(s(), s()),
    );
    r.skip(
        "evaluate_rollup_references",
        "Rc<dyn MemberSql> argument has no auto-default in Rust",
    );
    r
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
    cx.export_function("__testBridgeInvoke", invoke_bridge)?;
    cx.export_function("__testBridgeListBridgeNames", list_bridge_names)?;
    Ok(())
}
