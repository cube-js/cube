//! https://github.com/cube-js/cube/issues/3158
//! Kubernetes injects docker-link style env vars for every Service in the namespace
//! (unless `enableServiceLinks: false`). A Service named `cubestore` yields
//! `CUBESTORE_PORT=tcp://<cluster-ip>:<port>`, one named `cubestore-worker` yields
//! `CUBESTORE_WORKER_PORT=tcp://...`, etc. Cube Store must not panic on startup because of them.
//!
//! All cases live in a single #[test] because env vars are process-global.

use cubestore::config::{Config, ConfigObj};
use std::env;

fn bind_addresses_with(var: &str, value: &str) -> (Option<String>, Option<String>, Option<String>) {
    env::set_var(var, value);
    let result = std::panic::catch_unwind(|| {
        let config = Config::default();
        let obj = config.config_obj();
        (
            obj.bind_address().clone(),
            obj.worker_bind_address().clone(),
            obj.metastore_bind_address().clone(),
        )
    });
    env::remove_var(var);
    result.unwrap_or_else(|_| panic!("Config::default() panicked with {}={}", var, value))
}

#[test]
fn k8s_service_link_env_vars_do_not_panic() {
    // Service `cubestore` -> CUBESTORE_PORT; fall back to the documented default 3306.
    let (bind, _, _) = bind_addresses_with("CUBESTORE_PORT", "tcp://10.96.12.34:3030");
    assert_eq!(bind.as_deref(), Some("0.0.0.0:3306"));

    // Service `cubestore-worker` / `cubestore-meta` -> CUBESTORE_WORKER_PORT / CUBESTORE_META_PORT.
    // Must not panic (whether the value is ignored or its port extracted is up to the fix).
    bind_addresses_with("CUBESTORE_WORKER_PORT", "tcp://10.96.1.2:10001");
    bind_addresses_with("CUBESTORE_META_PORT", "tcp://10.96.1.5:9999");

    // Service `cubestore-http` / `cubestore-status`.
    bind_addresses_with("CUBESTORE_HTTP_PORT", "tcp://10.96.1.6:3030");
    bind_addresses_with("CUBESTORE_STATUS_PORT", "tcp://10.96.1.7:3031");
}
