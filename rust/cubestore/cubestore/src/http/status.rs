use crate::config::injection::Injector;
use crate::config::{is_router, uses_remote_metastore, Config};
use crate::metastore::MetaStore;
use crate::sql::SqlService;
use crate::CubeError;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use warp::http::StatusCode;
use warp::Filter;

pub fn serve_status_probes(c: &Config) {
    let addr = match c.config_obj().status_bind_address() {
        Some(a) => a.clone(),
        None => return,
    };

    let p = match RouterProbes::try_new(c) {
        Some(p) => p,
        None => return,
    };

    let pc = p.clone();
    let l = warp::path!("livez").and_then(move || {
        let pc = pc.clone();
        async move { status_probe_reply("liveness", pc.is_live().await) }
    });
    let r = warp::path!("readyz").and_then(move || {
        let p = p.clone();
        async move { status_probe_reply("readiness", p.is_ready().await) }
    });

    let addr: SocketAddr = addr.parse().expect("cannot parse status probe address");
    match warp::serve(l.or(r)).try_bind_ephemeral(addr) {
        Ok((addr, f)) => {
            log::info!("Serving status probes at {}", addr);
            tokio::spawn(f);
        }
        Err(e) => {
            log::error!("Failed to serve status probes at {}: {}", addr, e);
        }
    }
}

pub fn status_probe_reply(probe: &str, r: Result<(), CubeError>) -> Result<StatusCode, Infallible> {
    match r {
        Ok(()) => Ok(StatusCode::OK),
        Err(e) => {
            log::warn!("{} probe failed: {}", probe, e.display_with_backtrace());
            Ok(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[derive(Clone)]
struct RouterProbes {
    services: Arc<Injector>,
}

impl RouterProbes {
    pub fn try_new(config: &Config) -> Option<RouterProbes> {
        if !is_router(config.config_obj().as_ref()) {
            return None;
        }
        Some(RouterProbes {
            services: config.injector(),
        })
    }

    pub async fn is_live(&self) -> Result<(), CubeError> {
        if let Some(s) = self
            .services
            .try_get_service_typed::<dyn SqlService>()
            .await
        {
            s.exec_query("SELECT 1").await?;
        }
        Ok(())
    }

    pub async fn is_ready(&self) -> Result<(), CubeError> {
        if uses_remote_metastore(&self.services).await {
            return Ok(());
        }
        let m = match self.services.try_get_service_typed::<dyn MetaStore>().await {
            None => return Err(CubeError::internal("metastore is not ready".to_string())),
            Some(m) => m,
        };
        // Check metastore is not stalled.
        m.get_schemas().await?;
        // It is tempting to check worker connectivity on the router, but we cannot do this now.
        // Workers connect to the router for warmup, so router must be ready before workers are up.
        // TODO: warmup explicitly with router request instead?
        Ok(())
    }
}
