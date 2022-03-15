pub mod injection;
pub mod processing_loop;

use crate::config::injection::{DIService, Injector};
use crate::config::processing_loop::ProcessingLoop;
use crate::sql::{
    MySqlServer, PostgresServer, ServerManager, SessionManager, SqlAuthDefaultImpl, SqlAuthService,
};
use crate::telemetry::{start_track_event_loop, stop_track_event_loop};
use crate::transport::{HttpTransport, TransportService};
use crate::CubeError;
use futures::future::join_all;
use log::error;

use mockall::automock;

use std::env;

use std::sync::Arc;

use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct CubeServices {
    pub injector: Arc<Injector>,
}

impl CubeServices {
    pub async fn start_processing_loops(&self) -> Result<(), CubeError> {
        let futures = self.spawn_processing_loops().await?;
        tokio::spawn(async move {
            if let Err(e) = Self::wait_loops(futures).await {
                error!("Error in processing loop: {}", e);
            }
        });
        Ok(())
    }

    pub async fn wait_processing_loops(&self) -> Result<(), CubeError> {
        let processing_loops = self.spawn_processing_loops().await?;
        Self::wait_loops(processing_loops).await
    }

    pub async fn wait_loops(loops: Vec<LoopHandle>) -> Result<(), CubeError> {
        join_all(loops)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }

    pub async fn spawn_processing_loops(&self) -> Result<Vec<LoopHandle>, CubeError> {
        let mut futures = Vec::new();

        if self.injector.has_service_typed::<MySqlServer>().await {
            let mysql_server = self.injector.get_service_typed::<MySqlServer>().await;
            futures.push(tokio::spawn(async move {
                if let Err(e) = mysql_server.processing_loop().await {
                    error!("{}", e.to_string());
                };

                Ok(())
            }));
        }

        if self.injector.has_service_typed::<PostgresServer>().await {
            let mysql_server = self.injector.get_service_typed::<PostgresServer>().await;
            futures.push(tokio::spawn(async move {
                if let Err(e) = mysql_server.processing_loop().await {
                    error!("{}", e.to_string());
                };

                Ok(())
            }));
        }

        futures.push(tokio::spawn(async move {
            start_track_event_loop().await;
            Ok(())
        }));

        Ok(futures)
    }

    pub async fn stop_processing_loops(&self) -> Result<(), CubeError> {
        if self.injector.has_service_typed::<MySqlServer>().await {
            self.injector
                .get_service_typed::<MySqlServer>()
                .await
                .stop_processing()
                .await?;
        }

        if self.injector.has_service_typed::<PostgresServer>().await {
            self.injector
                .get_service_typed::<PostgresServer>()
                .await
                .stop_processing()
                .await?;
        }

        stop_track_event_loop().await;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Config {
    config_obj: Arc<ConfigObjImpl>,
    injector: Arc<Injector>,
}

#[automock]
pub trait ConfigObj: DIService {
    fn bind_address(&self) -> &Option<String>;

    fn postgres_bind_address(&self) -> &Option<String>;

    fn query_timeout(&self) -> u64;

    fn nonce(&self) -> &Option<Vec<u8>>;
}

#[derive(Debug, Clone)]
pub struct ConfigObjImpl {
    pub bind_address: Option<String>,
    pub postgres_bind_address: Option<String>,
    pub nonce: Option<Vec<u8>>,
    pub query_timeout: u64,
}

crate::di_service!(ConfigObjImpl, [ConfigObj]);
crate::di_service!(MockConfigObj, [ConfigObj]);

impl ConfigObj for ConfigObjImpl {
    fn bind_address(&self) -> &Option<String> {
        &self.bind_address
    }

    fn postgres_bind_address(&self) -> &Option<String> {
        &self.postgres_bind_address
    }

    fn nonce(&self) -> &Option<Vec<u8>> {
        &self.nonce
    }

    fn query_timeout(&self) -> u64 {
        self.query_timeout
    }
}

lazy_static! {
    pub static ref TEST_LOGGING_INITIALIZED: tokio::sync::RwLock<bool> =
        tokio::sync::RwLock::new(false);
}

impl Config {
    pub fn default() -> Config {
        let query_timeout = env::var("CUBESQL_QUERY_TIMEOUT")
            .ok()
            .map(|v| v.parse::<u64>().unwrap())
            .unwrap_or(120);
        Config {
            injector: Injector::new(),
            config_obj: Arc::new(ConfigObjImpl {
                bind_address: Some(env::var("CUBESQL_BIND_ADDR").ok().unwrap_or(
                    format!("0.0.0.0:{}", env::var("CUBESQL_PORT")
                            .ok()
                            .map(|v| v.parse::<u16>().unwrap())
                            .unwrap_or(3306u16)),
                )),
                postgres_bind_address: env::var("CUBESQL_PG_PORT")
                    .ok()
                    .map(|port| format!("0.0.0.0:{}", port.parse::<u16>().unwrap())),
                nonce: None,
                query_timeout,
            }),
        }
    }

    pub fn test(_name: &str) -> Config {
        let query_timeout = 15;
        Config {
            injector: Injector::new(),
            config_obj: Arc::new(ConfigObjImpl {
                bind_address: None,
                postgres_bind_address: None,
                nonce: None,
                query_timeout,
            }),
        }
    }

    pub fn update_config(
        &self,
        update_config: impl FnOnce(ConfigObjImpl) -> ConfigObjImpl,
    ) -> Config {
        let new_config = self.config_obj.as_ref().clone();
        Self {
            injector: self.injector.clone(),
            config_obj: Arc::new(update_config(new_config)),
        }
    }

    pub fn config_obj(&self) -> Arc<dyn ConfigObj> {
        self.config_obj.clone()
    }

    pub fn injector(&self) -> Arc<Injector> {
        self.injector.clone()
    }

    pub async fn configure_injector(&self) {
        let config_obj_to_register = self.config_obj.clone();
        self.injector
            .register_typed::<dyn ConfigObj, _, _, _>(async move |_| config_obj_to_register)
            .await;

        self.injector
            .register_typed::<dyn TransportService, _, _, _>(async move |_| Arc::new(HttpTransport))
            .await;

        self.injector
            .register_typed::<ServerManager, _, _, _>(async move |i| {
                let config = i.get_service_typed::<dyn ConfigObj>().await;
                Arc::new(ServerManager::new(
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    config.nonce().clone(),
                ))
            })
            .await;

        self.injector
            .register_typed::<SessionManager, _, _, _>(async move |i| {
                Arc::new(SessionManager::new(i.get_service_typed().await))
            })
            .await;

        self.injector
            .register_typed::<dyn SqlAuthService, _, _, _>(async move |_| {
                Arc::new(SqlAuthDefaultImpl)
            })
            .await;

        if self.config_obj.bind_address().is_some() {
            self.injector
                .register_typed::<MySqlServer, _, _, _>(async move |i| {
                    let config = i.get_service_typed::<dyn ConfigObj>().await;
                    MySqlServer::new(
                        config.bind_address().as_ref().unwrap().to_string(),
                        i.get_service_typed().await,
                    )
                })
                .await;
        }

        if self.config_obj.postgres_bind_address().is_some() {
            self.injector
                .register_typed::<PostgresServer, _, _, _>(async move |i| {
                    let config = i.get_service_typed::<dyn ConfigObj>().await;
                    PostgresServer::new(
                        config.postgres_bind_address().as_ref().unwrap().to_string(),
                        i.get_service_typed().await,
                    )
                })
                .await;
        }
    }

    pub async fn cube_services(&self) -> CubeServices {
        CubeServices {
            injector: self.injector.clone(),
        }
    }

    pub async fn configure(&self) -> CubeServices {
        self.configure_injector().await;
        self.cube_services().await
    }
}

type LoopHandle = JoinHandle<Result<(), CubeError>>;
