pub mod injection;
pub mod processing_loop;

use crate::{
    config::{
        injection::{DIService, Injector},
        processing_loop::ProcessingLoop,
    },
    sql::{
        MySqlServer, PostgresServer, ServerManager, SessionManager, SqlAuthDefaultImpl,
        SqlAuthService,
    },
    transport::{HttpTransport, TransportService},
    CubeError,
};
use futures::future::join_all;
use log::error;

use std::{
    env,
    fmt::{Debug, Display},
    str::FromStr,
};

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

        Ok(())
    }
}

#[derive(Clone)]
pub struct Config {
    config_obj: Arc<ConfigObjImpl>,
    injector: Arc<Injector>,
}

pub trait ConfigObj: DIService + Debug {
    fn bind_address(&self) -> &Option<String>;

    fn postgres_bind_address(&self) -> &Option<String>;

    fn query_timeout(&self) -> u64;

    fn nonce(&self) -> &Option<Vec<u8>>;

    fn disable_strict_agg_type_match(&self) -> bool;
}

#[derive(Debug, Clone)]
pub struct ConfigObjImpl {
    pub bind_address: Option<String>,
    pub postgres_bind_address: Option<String>,
    pub nonce: Option<Vec<u8>>,
    pub query_timeout: u64,
    pub timezone: Option<String>,
    pub disable_strict_agg_type_match: bool,
}

impl ConfigObjImpl {
    pub fn default() -> Self {
        let query_timeout = env::var("CUBESQL_QUERY_TIMEOUT")
            .ok()
            .map(|v| v.parse::<u64>().unwrap())
            .unwrap_or(120);
        Self {
            bind_address: env::var("CUBESQL_BIND_ADDR").ok().or_else(|| {
                env::var("CUBESQL_PORT")
                    .ok()
                    .map(|v| format!("0.0.0.0:{}", v.parse::<u16>().unwrap()))
            }),
            postgres_bind_address: env::var("CUBESQL_PG_PORT")
                .ok()
                .map(|port| format!("0.0.0.0:{}", port.parse::<u16>().unwrap())),
            nonce: None,
            query_timeout,
            timezone: Some("UTC".to_string()),
            disable_strict_agg_type_match: env_parse(
                "CUBESQL_DISABLE_STRICT_AGG_TYPE_MATCH",
                false,
            ),
        }
    }
}

crate::di_service!(ConfigObjImpl, [ConfigObj]);

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

    fn disable_strict_agg_type_match(&self) -> bool {
        self.disable_strict_agg_type_match
    }
}

lazy_static! {
    pub static ref TEST_LOGGING_INITIALIZED: tokio::sync::RwLock<bool> =
        tokio::sync::RwLock::new(false);
}

impl Config {
    pub fn default() -> Config {
        Config {
            injector: Injector::new(),
            config_obj: Arc::new(ConfigObjImpl::default()),
        }
    }

    pub fn test(_name: &str) -> Config {
        let query_timeout = 15;
        let timezone = Some("UTC".to_string());
        Config {
            injector: Injector::new(),
            config_obj: Arc::new(ConfigObjImpl {
                bind_address: None,
                postgres_bind_address: None,
                nonce: None,
                query_timeout,
                timezone,
                disable_strict_agg_type_match: false,
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
            .register_typed::<dyn TransportService, _, _, _>(async move |_| {
                Arc::new(HttpTransport::new())
            })
            .await;

        self.injector
            .register_typed::<ServerManager, _, _, _>(async move |i| {
                let config = i.get_service_typed::<dyn ConfigObj>().await;
                Arc::new(ServerManager::new(
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    config.nonce().clone(),
                    config.clone(),
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

    pub async fn configure(&self) {
        if let Some(timezone) = &self.config_obj.timezone {
            env::set_var("TZ", timezone.as_str());
        }

        self.configure_injector().await;
    }
}

pub fn env_parse<T>(name: &str, default: T) -> T
where
    T: FromStr,
    T::Err: Display,
{
    env_optparse(name).unwrap_or(default)
}

fn env_optparse<T>(name: &str) -> Option<T>
where
    T: FromStr,
    T::Err: Display,
{
    env::var(name).ok().map(|x| match x.parse::<T>() {
        Ok(v) => v,
        Err(e) => panic!(
            "Could not parse environment variable '{}' with '{}' value: {}",
            name, x, e
        ),
    })
}

type LoopHandle = JoinHandle<Result<(), CubeError>>;
