use crate::node_obj_deserializer::JsValueDeserializer;
use crate::node_obj_serializer::NodeObjSerializer;
use anyhow::anyhow;
use cubetranspilers::{run_transpilers, TransformConfig, Transpilers};
use lru::LruCache;
use neon::context::{Context, FunctionContext, ModuleContext};
use neon::prelude::{JsPromise, JsResult, JsValue, NeonResult};
use neon::types::JsString;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::env;
use std::num::NonZeroUsize;
use std::sync::{LazyLock, Mutex};

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TransformMetaData {
    pub cube_names: HashSet<String>,
    pub cube_symbols: HashMap<String, HashMap<String, bool>>,
    pub context_symbols: HashMap<String, String>,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TransformRequestConfig {
    pub file_name: String,
    pub transpilers: Vec<Transpilers>,
    pub compiler_id: String,
    pub meta_data: Option<TransformMetaData>,
}

/// It should be equal or more then number of internal libuv threads used by Neon
/// By 01.2025 it defaults to 4. But maybe changed via `UV_THREADPOOL_SIZE` env var.
/// `CUBEJS_TRANSPILER_METADATA_CACHE_SIZE` env var is provided for fine tuning.
/// @see https://docs.libuv.org/en/v1.x/threadpool.html
/// @see https://nodejs.org/api/cli.html#cli_uv_threadpool_size_size
static DEFAULT_CACHE_SIZE: usize = 16;

static METADATA_CACHE: LazyLock<Mutex<LruCache<String, TransformMetaData>>> = LazyLock::new(|| {
    let cache_size = env::var("CUBEJS_TRANSPILER_METADATA_CACHE_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .and_then(NonZeroUsize::new)
        .unwrap_or(NonZeroUsize::new(DEFAULT_CACHE_SIZE).unwrap());
    Mutex::new(LruCache::new(cache_size))
});

pub fn register_module(cx: &mut ModuleContext) -> NeonResult<()> {
    cx.export_function("transpileJs", transpile_js)?;

    Ok(())
}

pub fn transpile_js(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let content = cx.argument::<JsString>(0)?.value(&mut cx);
    let transform_data_js_object = cx.argument::<JsValue>(1)?;
    let deserializer = JsValueDeserializer::new(&mut cx, transform_data_js_object);
    let transform_request_config = TransformRequestConfig::deserialize(deserializer);

    let promise = cx
        .task(move || {
            let transform_config: TransformConfig = match transform_request_config {
                Ok(data) => match data.meta_data {
                    Some(meta_data) => {
                        let mut config_lock = METADATA_CACHE.lock().unwrap();
                        let cache = TransformMetaData {
                            cube_names: meta_data.cube_names,
                            cube_symbols: meta_data.cube_symbols,
                            context_symbols: meta_data.context_symbols,
                        };
                        let cfg = TransformConfig {
                            file_name: data.file_name,
                            transpilers: data.transpilers,
                            cube_names: cache.cube_names.clone(),
                            cube_symbols: cache.cube_symbols.clone(),
                            context_symbols: cache.context_symbols.clone(),
                        };
                        config_lock.put(data.compiler_id.clone(), cache);
                        cfg
                    }
                    None => {
                        let mut config_lock = METADATA_CACHE.lock().unwrap();

                        match config_lock.get(&data.compiler_id) {
                            Some(cached) => TransformConfig {
                                file_name: data.file_name,
                                transpilers: data.transpilers,
                                cube_names: cached.cube_names.clone(),
                                cube_symbols: cached.cube_symbols.clone(),
                                context_symbols: cached.context_symbols.clone(),
                            },
                            None => TransformConfig {
                                file_name: data.file_name,
                                transpilers: data.transpilers,
                                cube_names: HashSet::new(),
                                cube_symbols: HashMap::new(),
                                context_symbols: HashMap::new(),
                            },
                        }
                    }
                },
                Err(err) => return Err(anyhow!("Failed to deserialize input data: {}", err)),
            };

            run_transpilers(content, transform_config)
        })
        .promise(move |mut cx, res| match res {
            Ok(result) => {
                let obj = match NodeObjSerializer::serialize(&result, &mut cx) {
                    Ok(data) => data,
                    Err(err) => return cx.throw_error(err.to_string()),
                };
                Ok(obj)
            }
            Err(err) => cx.throw_error(err.to_string()),
        });

    Ok(promise)
}
