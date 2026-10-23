use crate::node_obj_deserializer::JsValueDeserializer;
use crate::transport::MapCubeErrExt;
use cubeorchestrator::query_message_parser::QueryResult;
use cubeorchestrator::query_result_transform::{
    columnar_plan, transform_value, ColumnarColumnSource, DBResponsePrimitive, RequestResultData,
    RequestResultDataMulti,
};
use cubeorchestrator::transport::{JsRawColumnarData, TransformDataRequest};
use cubesql::compile::engine::df::scan::{ColumnarValueObject, FieldValue};
use cubesql::CubeError;
use neon::context::{Context, FunctionContext, ModuleContext};
use neon::handle::{Handle, Root};
use neon::object::Object;
use neon::prelude::{
    JsArray, JsArrayBuffer, JsBox, JsBuffer, JsFunction, JsObject, JsPromise, JsResult, JsString,
    JsValue, NeonResult,
};
use neon::types::buffer::TypedArray;
use serde::Deserialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

pub fn register_module(cx: &mut ModuleContext) -> NeonResult<()> {
    cx.export_function(
        "parseCubestoreResultMessage",
        parse_cubestore_result_message,
    )?;
    cx.export_function("getCubestoreResult", get_cubestore_result)?;
    cx.export_function("getFinalQueryResult", final_query_result)?;
    cx.export_function("getFinalQueryResultMulti", final_query_result_multi)?;

    Ok(())
}

#[derive(Debug)]
enum ColumnSource {
    Db { index: usize, is_time: bool },
    Constant(DBResponsePrimitive),
}

#[derive(Debug)]
pub struct ResultWrapper {
    transform_data: TransformDataRequest,
    data: Arc<QueryResult>,
    /// Member name -> source column. Cells are read straight from `data`
    /// instead of materializing a transformed copy of the whole result.
    columns: Option<HashMap<String, ColumnSource>>,
    pub last_refresh_time: Option<String>,
    pub external: bool,
    pub used_pre_aggregations: Option<serde_json::Value>,
}

impl ResultWrapper {
    pub fn from_js_result_wrapper(
        cx: &mut FunctionContext<'_>,
        js_result_wrapper_val: Handle<JsValue>,
    ) -> Result<Self, CubeError> {
        let js_result_wrapper = js_result_wrapper_val
            .downcast::<JsObject, _>(cx)
            .map_cube_err("Can't downcast JS ResultWrapper to object")?;

        let get_transform_data_js_method: Handle<JsFunction> = js_result_wrapper
            .get(cx, "getTransformData")
            .map_cube_err("Can't get getTransformData() method from JS ResultWrapper object")?;

        let transform_data_js_arr = get_transform_data_js_method
            .call(cx, js_result_wrapper.upcast::<JsValue>(), [])
            .map_cube_err("Error calling getTransformData() method of ResultWrapper object")?
            .downcast::<JsArray, _>(cx)
            .map_cube_err("Can't downcast JS transformData to array")?
            .to_vec(cx)
            .map_cube_err("Can't convert JS transformData to array")?;

        let transform_data_js = transform_data_js_arr.first().unwrap();

        let deserializer = JsValueDeserializer::new(cx, *transform_data_js);
        let transform_request: TransformDataRequest = match Deserialize::deserialize(deserializer) {
            Ok(data) => data,
            Err(_) => {
                return Err(CubeError::internal(
                    "Can't deserialize transformData from JS ResultWrapper object".to_string(),
                ))
            }
        };

        let get_raw_data_js_method: Handle<JsFunction> = js_result_wrapper
            .get(cx, "getRawData")
            .map_cube_err("Can't get getRawData() method from JS ResultWrapper object")?;

        let raw_data_js_arr = get_raw_data_js_method
            .call(cx, js_result_wrapper.upcast::<JsValue>(), [])
            .map_cube_err("Error calling getRawData() method of ResultWrapper object")?
            .downcast::<JsArray, _>(cx)
            .map_cube_err("Can't downcast JS rawData to array")?
            .to_vec(cx)
            .map_cube_err("Can't convert JS rawData to array")?;

        let raw_data_js = raw_data_js_arr.first().unwrap();

        let query_result = if let Ok(js_box) =
            raw_data_js.downcast::<JsBox<Arc<QueryResult>>, _>(cx)
        {
            Arc::clone(&js_box)
        } else if let Ok(js_buffer) = raw_data_js.downcast::<JsBuffer, _>(cx) {
            let bytes = js_buffer.as_slice(cx);
            let js_raw_data: JsRawColumnarData = serde_json::from_slice(bytes).map_err(|e| {
                CubeError::internal(format!(
                    "Can't parse raw data JSON from JS ResultWrapper: {}",
                    e
                ))
            })?;

            QueryResult::from_js_raw_data(js_raw_data)
                .map(Arc::new)
                .map_cube_err("Can't build results data from JS rawData")?
        } else {
            return Err(CubeError::internal(
                "Can't deserialize results raw data from JS ResultWrapper object".to_string(),
            ));
        };

        Ok(Self {
            transform_data: transform_request,
            data: query_result,
            columns: None,
            last_refresh_time: None,
            external: false,
            used_pre_aggregations: None,
        })
    }

    fn prepare_columns(&mut self) -> Result<(), CubeError> {
        if self.columns.is_none() {
            let (members, plan) = columnar_plan(&self.transform_data, &self.data)
                .map_cube_err("Can't prepare transformed data")?;

            let mut columns = HashMap::with_capacity(members.len());
            for (member, entry) in members.into_iter().zip(plan) {
                let source = match entry.source {
                    ColumnarColumnSource::DbColumn { index } => ColumnSource::Db {
                        index,
                        is_time: entry.member_type == "time",
                    },
                    ColumnarColumnSource::Constant(value) => ColumnSource::Constant(value),
                    ColumnarColumnSource::NullFilled => continue,
                };
                // The first occurrence wins, as with a positional lookup in `members`.
                columns.entry(member).or_insert(source);
            }

            self.columns = Some(columns);
        }

        Ok(())
    }
}

fn db_primitive_to_field_value(value: &DBResponsePrimitive) -> FieldValue<'_> {
    match value {
        DBResponsePrimitive::String(s) => FieldValue::String(Cow::Borrowed(s)),
        DBResponsePrimitive::Int64(n) => FieldValue::Number(*n as f64),
        DBResponsePrimitive::UInt64(n) => FieldValue::Number(*n as f64),
        DBResponsePrimitive::Float64(n) => FieldValue::Number(*n),
        DBResponsePrimitive::Boolean(b) => FieldValue::Bool(*b),
        DBResponsePrimitive::Timestamp(_) => FieldValue::String(Cow::Owned(value.to_string())),
        DBResponsePrimitive::Uncommon(v) => FieldValue::String(Cow::Owned(
            serde_json::to_string(&v).unwrap_or_else(|_| v.to_string()),
        )),
        DBResponsePrimitive::Null => FieldValue::Null,
    }
}

fn db_time_to_field_value(value: &DBResponsePrimitive) -> FieldValue<'_> {
    match value {
        DBResponsePrimitive::String(_) => match transform_value(value.clone(), "time") {
            DBResponsePrimitive::String(s) => FieldValue::String(Cow::Owned(s)),
            _ => unreachable!("transform_value keeps strings as strings"),
        },
        other => db_primitive_to_field_value(other),
    }
}

impl ColumnarValueObject for ResultWrapper {
    fn len(&mut self) -> Result<usize, CubeError> {
        self.prepare_columns()?;

        Ok(self.data.row_count())
    }

    fn column<'a>(
        &'a mut self,
        field_name: &str,
    ) -> Result<Box<dyn Iterator<Item = Result<FieldValue<'a>, CubeError>> + 'a>, CubeError> {
        self.prepare_columns()?;

        let row_count = self.data.row_count();
        let Some(source) = self.columns.as_ref().unwrap().get(field_name) else {
            // Missing field → column of NULLs. See JsonColumnarValueObject::column.
            return Ok(Box::new((0..row_count).map(|_| Ok(FieldValue::Null))));
        };

        match source {
            ColumnSource::Db { index, is_time } => {
                let column = self.data.column(*index).map_err(|_| {
                    CubeError::user(format!(
                        "Unexpected response from Cube, missing column for '{}'",
                        field_name
                    ))
                })?;

                if *is_time {
                    Ok(Box::new(
                        column.iter().map(|v| Ok(db_time_to_field_value(v))),
                    ))
                } else {
                    Ok(Box::new(
                        column.iter().map(|v| Ok(db_primitive_to_field_value(v))),
                    ))
                }
            }
            ColumnSource::Constant(value) => Ok(Box::new(
                (0..row_count).map(move |_| Ok(db_primitive_to_field_value(value))),
            )),
        }
    }
}

fn json_to_array_buffer<'a, C>(
    mut cx: C,
    json_data: Result<String, anyhow::Error>,
) -> JsResult<'a, JsArrayBuffer>
where
    C: Context<'a>,
{
    match json_data {
        Ok(json_data) => {
            let json_bytes = json_data.as_bytes();
            let mut js_buffer = cx.array_buffer(json_bytes.len())?;
            {
                let buffer = js_buffer.as_mut_slice(&mut cx);
                buffer.copy_from_slice(json_bytes);
            }
            Ok(js_buffer)
        }
        Err(err) => cx.throw_error(err.to_string()),
    }
}

fn extract_query_result(
    cx: &mut FunctionContext<'_>,
    data_arg: Handle<JsValue>,
) -> Result<Arc<QueryResult>, anyhow::Error> {
    if let Ok(js_box) = data_arg.downcast::<JsBox<Arc<QueryResult>>, _>(cx) {
        Ok(Arc::clone(&js_box))
    } else if let Ok(js_buffer) = data_arg.downcast::<JsBuffer, _>(cx) {
        let bytes = js_buffer.as_slice(cx);
        let js_raw_data: JsRawColumnarData = serde_json::from_slice(bytes)?;

        QueryResult::from_js_raw_data(js_raw_data)
            .map(Arc::new)
            .map_err(anyhow::Error::from)
    } else {
        Err(anyhow::anyhow!(
            "Second argument must be a JsBox<Arc<QueryResult>> or a JsBuffer with columnar JsRawColumnarData JSON"
        ))
    }
}

/// Zero-copy view of a JS `Buffer` that keeps it rooted.
struct JsBufferView {
    root: Root<JsBuffer>,
    ptr: *const u8,
    len: usize,
}

// SAFETY: `ptr` is only read, and the `Root` keeps its backing store alive; GC never moves
// ArrayBuffer backing stores.
unsafe impl Send for JsBufferView {}

impl JsBufferView {
    fn new<'a, C: Context<'a>>(cx: &mut C, buffer: Handle<'a, JsBuffer>) -> Self {
        let slice = buffer.as_slice(cx);
        let (ptr, len) = (slice.as_ptr(), slice.len());

        Self {
            root: buffer.root(cx),
            ptr,
            len,
        }
    }

    /// # Safety
    /// JS must not mutate, detach or transfer the buffer while the slice is in use.
    unsafe fn as_slice(&self) -> &[u8] {
        // N-API may return null for an empty buffer.
        if self.len == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(self.ptr, self.len)
        }
    }

    fn release<'a, C: Context<'a>>(self, cx: &mut C) {
        self.root.drop(cx);
    }
}

pub fn parse_cubestore_result_message(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let msg = cx.argument::<JsBuffer>(0)?;
    let msg = JsBufferView::new(&mut cx, msg);

    let promise = cx
        .task(move || {
            // SAFETY: JS doesn't mutate the message until the promise settles.
            let res = QueryResult::from_cubestore_fb(unsafe { msg.as_slice() });
            (msg, res)
        })
        .promise(move |mut cx, (msg, res)| {
            msg.release(&mut cx);

            match res {
                Ok(result) => Ok(cx.boxed(Arc::new(result))),
                Err(err) => cx.throw_error(err.to_string()),
            }
        });

    Ok(promise)
}

pub fn get_cubestore_result(mut cx: FunctionContext) -> JsResult<JsValue> {
    let result = cx.argument::<JsBox<Arc<QueryResult>>>(0)?;

    let js_array = cx.execute_scoped(|mut cx| {
        let js_keys: Vec<Handle<JsString>> =
            result.members().iter().map(|k| cx.string(k)).collect();

        let row_count = result.row_count();
        let columns: Vec<_> = (0..js_keys.len())
            .map(|i| result.column(i))
            .collect::<Result<_, _>>()
            .or_else(|err| cx.throw_error(err.to_string()))?;
        let js_array = JsArray::new(&mut cx, row_count);

        for row_idx in 0..row_count {
            let js_row = cx.execute_scoped(|mut cx| {
                let js_row = JsObject::new(&mut cx);

                for (js_key, column) in js_keys.iter().zip(columns.iter()) {
                    let value = &column[row_idx];
                    let js_value: Handle<'_, JsValue> = match value {
                        DBResponsePrimitive::Null => cx.null().upcast(),
                        // For compatibility, we convert all primitives to strings
                        other => cx.string(other.to_string()).upcast(),
                    };

                    js_row.set(&mut cx, *js_key, js_value)?;
                }

                Ok(js_row)
            })?;

            js_array.set(&mut cx, row_idx as u32, js_row)?;
        }

        Ok(js_array)
    })?;

    Ok(js_array.upcast())
}

pub fn final_query_result(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let transform_data_js_object = cx.argument::<JsValue>(0)?;
    let deserializer = JsValueDeserializer::new(&mut cx, transform_data_js_object);
    let transform_request_data: TransformDataRequest = match Deserialize::deserialize(deserializer)
    {
        Ok(data) => data,
        Err(err) => return cx.throw_error(err.to_string()),
    };

    let data_arg = cx.argument::<JsValue>(1)?;
    let cube_store_result: Arc<QueryResult> = match extract_query_result(&mut cx, data_arg) {
        Ok(query_result) => query_result,
        Err(err) => return cx.throw_error(err.to_string()),
    };

    let result_data_js_object = cx.argument::<JsValue>(2)?;
    let deserializer = JsValueDeserializer::new(&mut cx, result_data_js_object);
    let mut result_data: RequestResultData = match Deserialize::deserialize(deserializer) {
        Ok(data) => data,
        Err(err) => return cx.throw_error(err.to_string()),
    };

    let promise = cx
        .task(move || {
            result_data.prepare_results(&transform_request_data, &cube_store_result)?;

            match serde_json::to_string(&result_data) {
                Ok(json) => Ok(json),
                Err(err) => Err(anyhow::Error::from(err)),
            }
        })
        .promise(move |cx, json_data| json_to_array_buffer(cx, json_data));

    Ok(promise)
}

pub fn final_query_result_multi(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let transform_data_array = cx.argument::<JsValue>(0)?;
    let deserializer = JsValueDeserializer::new(&mut cx, transform_data_array);
    let transform_requests: Vec<TransformDataRequest> = match Deserialize::deserialize(deserializer)
    {
        Ok(data) => data,
        Err(err) => return cx.throw_error(err.to_string()),
    };

    let data_array = cx.argument::<JsArray>(1)?;
    let mut cube_store_results: Vec<Arc<QueryResult>> = vec![];
    for data_arg in data_array.to_vec(&mut cx)? {
        match extract_query_result(&mut cx, data_arg) {
            Ok(query_result) => cube_store_results.push(query_result),
            Err(err) => return cx.throw_error(err.to_string()),
        };
    }

    let result_data_js_object = cx.argument::<JsValue>(2)?;
    let deserializer = JsValueDeserializer::new(&mut cx, result_data_js_object);
    let mut result_data: RequestResultDataMulti = match Deserialize::deserialize(deserializer) {
        Ok(data) => data,
        Err(err) => return cx.throw_error(err.to_string()),
    };

    let promise = cx
        .task(move || {
            result_data.prepare_results(&transform_requests, &cube_store_results)?;

            match serde_json::to_string(&result_data) {
                Ok(json) => Ok(json),
                Err(err) => Err(anyhow::Error::from(err)),
            }
        })
        .promise(move |cx, json_data| json_to_array_buffer(cx, json_data));

    Ok(promise)
}
