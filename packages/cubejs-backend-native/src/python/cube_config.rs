use convert_case::{Case, Casing};
use neon::prelude::*;
use pyo3::{PyAny, PyResult};

use crate::cross::{CLRepr, CLReprObject, CLReprObjectKind};

pub struct CubeConfigPy {
    properties: CLReprObject,
}

impl CubeConfigPy {
    pub fn new() -> Self {
        Self {
            properties: CLReprObject::new(CLReprObjectKind::Object),
        }
    }

    pub fn get_attrs(&self) -> Vec<&'static str> {
        vec![
            "allow_js_duplicate_props_in_schema",
            "api_secret",
            "base_path",
            "cache_and_queue_driver",
            "compiler_cache_size",
            "dev_server",
            "graceful_shutdown",
            "http",
            "jwt",
            "live_preview",
            "max_compiler_cache_keep_alive",
            "pg_sql_port",
            "process_subscriptions_interval",
            "scheduled_refresh_batch_size",
            "scheduled_refresh_concurrency",
            "scheduled_refresh_timer",
            "scheduled_refresh_time_zones",
            "schema_path",
            "sql_cache",
            "sql_password",
            "sql_super_user",
            "sql_user",
            "telemetry",
            "update_compiler_cache_keep_alive",
            "web_sockets",
            "web_sockets_base_path",
            // functions
            "can_switch_sql_user",
            "check_auth",
            "check_sql_auth",
            "context_to_api_scopes",
            "context_to_app_id",
            "context_to_orchestrator_id",
            "context_to_cube_store_router_id",
            "context_to_roles",
            "context_to_groups",
            "db_type",
            "driver_factory",
            "extend_context",
            "external_driver_factory",
            "logger",
            "orchestrator_options",
            "pre_aggregations_schema",
            "query_rewrite",
            "repository_factory",
            "scheduled_refresh_contexts",
            "schema_version",
            "semantic_layer_sync",
            "fast_reload",
        ]
    }

    pub fn attr(&mut self, config_module: &PyAny, key: &str) -> PyResult<()> {
        let v = config_module.getattr(key)?;
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
