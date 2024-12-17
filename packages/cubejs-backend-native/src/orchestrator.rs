use crate::node_obj_deserializer::JsValueDeserializer;
use cubeorchestrator::cubestore_message_parser::CubeStoreResult;
use cubeorchestrator::cubestore_result_transform::{
    get_final_cubestore_result_array, RequestResultArray, RequestResultData,
    RequestResultDataMulti, TransformedData,
};
use cubeorchestrator::transport::TransformDataRequest;
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
    cx.export_function("transformQueryData", transform_query_data)?;
    cx.export_function("getFinalCubestoreResult", final_cubestore_result)?;
    cx.export_function("getFinalCubestoreResultMulti", final_cubestore_result_multi)?;
    cx.export_function("getFinalCubestoreResultArray", final_cubestore_result_array)?;

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

pub fn parse_cubestore_result_message(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let msg = cx.argument::<JsBuffer>(0)?;
    let msg_data = msg.as_slice(&cx).to_vec();

    let promise = cx
        .task(move || CubeStoreResult::from_fb(&msg_data))
        .promise(move |mut cx, res| match res {
            Ok(result) => Ok(cx.boxed(Arc::new(result))),
            Err(err) => cx.throw_error(err.to_string()),
        });

    Ok(promise)
}

pub fn get_cubestore_result(mut cx: FunctionContext) -> JsResult<JsValue> {
    let result = cx.argument::<JsBox<Arc<CubeStoreResult>>>(0)?;

    let js_array = cx.execute_scoped(|mut cx| {
        let js_array = JsArray::new(&mut cx, result.rows.len());

        for (i, row) in result.rows.iter().enumerate() {
            let js_row = cx.execute_scoped(|mut cx| {
                let js_row = JsObject::new(&mut cx);
                for (key, value) in result.columns.iter().zip(row.iter()) {
                    let js_key = cx.string(key);
                    let js_value = cx.string(value);
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

pub fn transform_query_data(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let transform_data_js_object = cx.argument::<JsValue>(0)?;
    let deserializer = JsValueDeserializer::new(&mut cx, transform_data_js_object);

    let request_data: TransformDataRequest = match Deserialize::deserialize(deserializer) {
        Ok(data) => data,
        Err(err) => return cx.throw_error(err.to_string()),
    };

    let cube_store_result = cx.argument::<JsBox<Arc<CubeStoreResult>>>(1)?;
    let cube_store_result = Arc::clone(&cube_store_result);

    let promise = cx
        .task(move || {
            let transformed = TransformedData::transform(&request_data, &cube_store_result)?;

            match serde_json::to_string(&transformed) {
                Ok(json) => Ok(json),
                Err(err) => Err(anyhow::Error::from(err)),
            }
        })
        .promise(move |mut cx, json_data| match json_data {
            Ok(json_data) => {
                let js_string = cx.string(json_data);

                let js_result = cx.empty_object();
                js_result.set(&mut cx, "result", js_string)?;

                Ok(js_result)
            }
            Err(err) => cx.throw_error(err.to_string()),
        });

    Ok(promise)
}

pub fn final_cubestore_result(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let transform_data_js_object = cx.argument::<JsValue>(0)?;
    let deserializer = JsValueDeserializer::new(&mut cx, transform_data_js_object);
    let transform_request_data: TransformDataRequest = match Deserialize::deserialize(deserializer)
    {
        Ok(data) => data,
        Err(err) => return cx.throw_error(err.to_string()),
    };

    let cube_store_result = cx.argument::<JsBox<Arc<CubeStoreResult>>>(1)?;
    let cube_store_result = Arc::clone(&cube_store_result);
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

pub fn final_cubestore_result_array(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let transform_data_array = cx.argument::<JsValue>(0)?;
    let deserializer = JsValueDeserializer::new(&mut cx, transform_data_array);
    let transform_requests: Vec<TransformDataRequest> = match Deserialize::deserialize(deserializer)
    {
        Ok(data) => data,
        Err(err) => return cx.throw_error(err.to_string()),
    };

    let cube_store_array = cx.argument::<JsArray>(1)?;
    let cube_store_results_boxed: Vec<Handle<JsBox<Arc<CubeStoreResult>>>> = cube_store_array
        .to_vec(&mut cx)?
        .into_iter()
        .map(|js_value| js_value.downcast_or_throw::<JsBox<Arc<CubeStoreResult>>, _>(&mut cx))
        .collect::<Result<_, _>>()?;
    let cube_store_results: Vec<Arc<CubeStoreResult>> = cube_store_results_boxed
        .iter()
        .map(|handle| (**handle).clone())
        .collect();

    let results_data_array = cx.argument::<JsValue>(2)?;
    let deserializer = JsValueDeserializer::new(&mut cx, results_data_array);
    let mut request_results: Vec<RequestResultData> = match Deserialize::deserialize(deserializer) {
        Ok(data) => data,
        Err(err) => return cx.throw_error(err.to_string()),
    };

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

pub fn final_cubestore_result_multi(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let transform_data_array = cx.argument::<JsValue>(0)?;
    let deserializer = JsValueDeserializer::new(&mut cx, transform_data_array);
    let transform_requests: Vec<TransformDataRequest> = match Deserialize::deserialize(deserializer)
    {
        Ok(data) => data,
        Err(err) => return cx.throw_error(err.to_string()),
    };

    let cube_store_array = cx.argument::<JsArray>(1)?;
    let cube_store_results_boxed: Vec<Handle<JsBox<Arc<CubeStoreResult>>>> = cube_store_array
        .to_vec(&mut cx)?
        .into_iter()
        .map(|js_value| js_value.downcast_or_throw::<JsBox<Arc<CubeStoreResult>>, _>(&mut cx))
        .collect::<Result<_, _>>()?;
    let cube_store_results: Vec<Arc<CubeStoreResult>> = cube_store_results_boxed
        .iter()
        .map(|handle| (**handle).clone())
        .collect();

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
