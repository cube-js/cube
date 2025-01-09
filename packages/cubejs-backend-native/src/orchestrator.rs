use crate::node_obj_deserializer::JsValueDeserializer;
use cubeorchestrator::query_message_parser::QueryResult;
use cubeorchestrator::query_result_transform::{
    get_final_cubestore_result_array, RequestResultArray, RequestResultData, RequestResultDataMulti,
};
use cubeorchestrator::transport::{JsRawData, TransformDataRequest};
use neon::context::{Context, FunctionContext, ModuleContext};
use neon::handle::Handle;
use neon::object::Object;
use neon::prelude::{
    JsArray, JsArrayBuffer, JsBox, JsBuffer, JsObject, JsPromise, JsResult, JsValue, NeonResult,
};
use neon::types::buffer::TypedArray;
use serde::Deserialize;
use std::sync::Arc;

pub fn register_module(cx: &mut ModuleContext) -> NeonResult<()> {
    cx.export_function(
        "parseCubestoreResultMessage",
        parse_cubestore_result_message,
    )?;
    cx.export_function("getCubestoreResult", get_cubestore_result)?;
    cx.export_function("getFinalQueryResult", final_query_result)?;
    cx.export_function("getFinalQueryResultMulti", final_query_result_multi)?;
    cx.export_function("getFinalQueryResultArray", final_query_result_array)?;

    Ok(())
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

pub type JsResultDataVectors = (
    Vec<TransformDataRequest>,
    Vec<Arc<QueryResult>>,
    Vec<RequestResultData>,
);

pub fn convert_final_query_result_array_from_js(
    cx: &mut FunctionContext<'_>,
    transform_data_array: Handle<JsValue>,
    data_array: Handle<JsArray>,
    results_data_array: Handle<JsValue>,
) -> NeonResult<JsResultDataVectors> {
    let deserializer = JsValueDeserializer::new(cx, transform_data_array);
    let transform_requests: Vec<TransformDataRequest> = match Deserialize::deserialize(deserializer)
    {
        Ok(data) => data,
        Err(err) => return cx.throw_error(err.to_string()),
    };

    let mut cube_store_results: Vec<Arc<QueryResult>> = vec![];
    for data_arg in data_array.to_vec(cx)? {
        match extract_query_result(cx, data_arg) {
            Ok(query_result) => cube_store_results.push(query_result),
            Err(err) => return cx.throw_error(err.to_string()),
        };
    }

    let deserializer = JsValueDeserializer::new(cx, results_data_array);
    let request_results: Vec<RequestResultData> = match Deserialize::deserialize(deserializer) {
        Ok(data) => data,
        Err(err) => return cx.throw_error(err.to_string()),
    };

    Ok((transform_requests, cube_store_results, request_results))
}

pub fn final_query_result_array(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let transform_data_array = cx.argument::<JsValue>(0)?;
    let data_array = cx.argument::<JsArray>(1)?;
    let results_data_array = cx.argument::<JsValue>(2)?;

    let convert_res = convert_final_query_result_array_from_js(
        &mut cx,
        transform_data_array,
        data_array,
        results_data_array,
    );
    match convert_res {
        Ok((transform_requests, cube_store_results, mut request_results)) => {
            let promise = cx
                .task(move || {
                    get_final_cubestore_result_array(
                        &transform_requests,
                        &cube_store_results,
                        &mut request_results,
                    )?;

                    let final_obj = RequestResultArray {
                        results: request_results,
                    };

                    match serde_json::to_string(&final_obj) {
                        Ok(json) => Ok(json),
                        Err(err) => Err(anyhow::Error::from(err)),
                    }
                })
                .promise(move |cx, json_data| json_to_array_buffer(cx, json_data));

            Ok(promise)
        }
        Err(err) => cx.throw_error(err.to_string()),
    }
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
