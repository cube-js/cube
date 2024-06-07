use convert_case::{Case, Casing};
use neon::prelude::*;
use pyo3::{PyAny, PyResult};

use crate::cross::{CLRepr, CLReprObject, CLReprPython};

pub struct CubeConfigPy {
    properties: CLReprObject,
}

impl CubeConfigPy {
    pub fn new() -> Self {
        Self {
            properties: CLReprObject::new(),
        }
    }

    pub fn get_attrs(&self) -> Vec<&'static str> {
        vec![
            "web_sockets",
            "http",
            "graceful_shutdown",
            "process_subscriptions_interval",
            "web_sockets_base_path",
            "schema_path",
            "base_path",
            "dev_server",
            "api_secret",
            "cache_and_queue_driver",
            "allow_js_duplicate_props_in_schema",
            "jwt",
            "scheduled_refresh_timer",
            "scheduled_refresh_timezones",
            "scheduled_refresh_concurrency",
            "scheduled_refresh_batch_size",
            "compiler_cache_size",
            "update_compiler_cache_keep_alive",
            "max_compiler_cache_keep_alive",
            "telemetry",
            "sql_cache",
            "live_preview",
            "pg_sql_port",
            "sql_super_user",
            "sql_user",
            "sql_password",
            // functions
            "logger",
            "context_to_app_id",
            "context_to_orchestrator_id",
            "driver_factory",
            "external_driver_factory",
            "db_type",
            "check_auth",
            "check_sql_auth",
            "can_switch_sql_user",
            "query_rewrite",
            "extend_context",
            "scheduled_refresh_contexts",
            "context_to_api_scopes",
            "repository_factory",
            "semantic_layer_sync",
            "schema_version",
            "pre_aggregations_schema",
            "orchestrator_options",
        ]
    }

    pub fn attr(&mut self, config_module: &PyAny, key: &str) -> PyResult<()> {
        let v = config_module.getattr(&*key)?;
        if !v.is_none() {
            let value = CLRepr::from_python_ref(v)?;
            self.properties.insert(key.to_case(Case::Camel), value);
        };

        Ok(())
    }
}

impl Finalize for CubeConfigPy {}

impl CubeConfigPy {
    #[allow(clippy::wrong_self_convention)]
    pub fn to_object<'a, C: Context<'a>>(self, cx: &mut C) -> JsResult<'a, JsValue> {
        let obj = CLRepr::Object(self.properties);
        obj.into_js(cx)
    }
}
