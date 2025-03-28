use crate::node_obj_deserializer::JsValueDeserializer;
use crate::transport::MapCubeErrExt;
use cubeorchestrator::query_message_parser::QueryResult;
use cubeorchestrator::query_result_transform::{
    DBResponsePrimitive, RequestResultData, RequestResultDataMulti, TransformedData,
};
use cubeorchestrator::transport::{JsRawData, TransformDataRequest};
use cubesql::compile::engine::df::scan::{FieldValue, ValueObject};
use cubesql::CubeError;
use neon::context::{Context, FunctionContext, ModuleContext};
use neon::handle::Handle;
use neon::object::Object;
use neon::prelude::{
    JsArray, JsArrayBuffer, JsBox, JsBuffer, JsFunction, JsObject, JsPromise, JsResult, JsValue,
    NeonResult,
};
use neon::types::buffer::TypedArray;
use serde::Deserialize;
use std::borrow::Cow;
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

#[derive(Debug, Clone)]
pub struct ResultWrapper {
    transform_data: TransformDataRequest,
    data: Arc<QueryResult>,
    transformed_data: Option<TransformedData>,
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

        let query_result =
            if let Ok(js_box) = raw_data_js.downcast::<JsBox<Arc<QueryResult>>, _>(cx) {
                Arc::clone(&js_box)
            } else if let Ok(js_array) = raw_data_js.downcast::<JsArray, _>(cx) {
                let deserializer = JsValueDeserializer::new(cx, js_array.upcast());
                let js_raw_data: JsRawData = match Deserialize::deserialize(deserializer) {
                    Ok(data) => data,
                    Err(_) => {
                        return Err(CubeError::internal(
                            "Can't deserialize results raw data from JS ResultWrapper object"
                                .to_string(),
                        ));
                    }
                };

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
            transformed_data: None,
        })
    }

    pub fn transform_result(&mut self) -> Result<(), CubeError> {
        self.transformed_data = Some(
            TransformedData::transform(&self.transform_data, &self.data)
                .map_cube_err("Can't prepare transformed data")?,
        );

        Ok(())
    }
}

impl ValueObject for ResultWrapper {
    fn len(&mut self) -> Result<usize, CubeError> {
        if self.transformed_data.is_none() {
            self.transform_result()?;
        }

        let data = self.transformed_data.as_ref().unwrap();

        match data {
            TransformedData::Compact {
                members: _members,
                dataset,
            } => Ok(dataset.len()),
            TransformedData::Vanilla(dataset) => Ok(dataset.len()),
        }
    }

    fn get(&mut self, index: usize, field_name: &str) -> Result<FieldValue, CubeError> {
        if self.transformed_data.is_none() {
            self.transform_result()?;
        }

        let data = self.transformed_data.as_ref().unwrap();

        let value = match data {
            TransformedData::Compact { members, dataset } => {
                let Some(row) = dataset.get(index) else {
                    return Err(CubeError::user(format!(
                        "Unexpected response from Cube, can't get {} row",
                        index
                    )));
                };

                let Some(member_index) = members.iter().position(|m| m == field_name) else {
                    return Err(CubeError::user(format!(
                        "Field name '{}' not found in members",
                        field_name
                    )));
                };

                row.get(member_index).unwrap_or(&DBResponsePrimitive::Null)
            }
            TransformedData::Vanilla(dataset) => {
                let Some(row) = dataset.get(index) else {
                    return Err(CubeError::user(format!(
                        "Unexpected response from Cube, can't get {} row",
                        index
                    )));
                };

                row.get(field_name).unwrap_or(&DBResponsePrimitive::Null)
            }
        };

        Ok(match value {
            DBResponsePrimitive::String(s) => FieldValue::String(Cow::Borrowed(s)),
            DBResponsePrimitive::Number(n) => FieldValue::Number(*n),
            DBResponsePrimitive::Boolean(b) => FieldValue::Bool(*b),
            DBResponsePrimitive::Uncommon(v) => FieldValue::String(Cow::Owned(
                serde_json::to_string(&v).unwrap_or_else(|_| v.to_string()),
            )),
            DBResponsePrimitive::Null => FieldValue::Null,
        })
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
    } else if let Ok(js_array) = data_arg.downcast::<JsArray, _>(cx) {
        let deserializer = JsValueDeserializer::new(cx, js_array.upcast());
        let js_raw_data: JsRawData = Deserialize::deserialize(deserializer)?;

        QueryResult::from_js_raw_data(js_raw_data)
            .map(Arc::new)
            .map_err(anyhow::Error::from)
    } else {
        Err(anyhow::anyhow!(
            "Second argument must be an Array of JsBox<Arc<QueryResult>> or JsArray"
        ))
    }
}

pub fn parse_cubestore_result_message(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let msg = cx.argument::<JsBuffer>(0)?;
    let msg_data = msg.as_slice(&cx).to_vec();

    let promise = cx
        .task(move || QueryResult::from_cubestore_fb(&msg_data))
        .promise(move |mut cx, res| match res {
            Ok(result) => Ok(cx.boxed(Arc::new(result))),
            Err(err) => cx.throw_error(err.to_string()),
        });

    Ok(promise)
}

pub fn get_cubestore_result(mut cx: FunctionContext) -> JsResult<JsValue> {
    let result = cx.argument::<JsBox<Arc<QueryResult>>>(0)?;

    let js_array = cx.execute_scoped(|mut cx| {
        let js_array = JsArray::new(&mut cx, result.rows.len());

        for (i, row) in result.rows.iter().enumerate() {
            let js_row = cx.execute_scoped(|mut cx| {
                let js_row = JsObject::new(&mut cx);
                for (key, value) in result.columns.iter().zip(row.iter()) {
                    let js_key = cx.string(key);
                    let js_value = cx.string(value.to_string());
                    js_row.set(&mut cx, js_key, js_value)?;
                }
                Ok(js_row)
            })?;

            js_array.set(&mut cx, i as u32, js_row)?;
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
