pub mod injection;
pub mod processing_loop;

use crate::{
    config::{
        injection::{DIService, Injector},
        processing_loop::{ProcessingLoop, ShutdownMode},
    },
    sql::{
        pg_auth_service::{PostgresAuthService, PostgresAuthServiceDefaultImpl},
        ArrowNativeServer, PostgresServer, ServerManager, SessionManager, SqlAuthDefaultImpl,
        SqlAuthService,
    },
    transport::{HybridTransport, TransportService},
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

use crate::sql::compiler_cache::{CompilerCache, CompilerCacheImpl};
use tokio::{sync::RwLock, task::JoinHandle};

pub struct CubeServices {
    pub injector: Arc<Injector>,
    pub processing_loop_handles: RwLock<Vec<LoopHandle>>,
}

impl CubeServices {
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

        if self.injector.has_service_typed::<PostgresServer>().await {
            let postgres_server = self.injector.get_service_typed::<PostgresServer>().await;
            futures.push(tokio::spawn(async move {
                if let Err(e) = postgres_server.processing_loop().await {
                    error!("{}", e.to_string());
                };

                Ok(())
            }));
        }

        if self.injector.has_service_typed::<ArrowNativeServer>().await {
            let arrow_server = self.injector.get_service_typed::<ArrowNativeServer>().await;
            futures.push(tokio::spawn(async move {
                if let Err(e) = arrow_server.processing_loop().await {
                    error!("{}", e.to_string());
                };

                Ok(())
            }));
        }

        Ok(futures)
    }

    pub async fn stop_processing_loops(
        &self,
        shutdown_mode: ShutdownMode,
    ) -> Result<(), CubeError> {
        if self.injector.has_service_typed::<PostgresServer>().await {
            self.injector
                .get_service_typed::<PostgresServer>()
                .await
                .stop_processing(shutdown_mode)
                .await?;
        }

        if self.injector.has_service_typed::<ArrowNativeServer>().await {
            self.injector
                .get_service_typed::<ArrowNativeServer>()
                .await
                .stop_processing(shutdown_mode)
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

    fn arrow_native_bind_address(&self) -> &Option<String>;

    fn query_timeout(&self) -> u64;

    fn nonce(&self) -> &Option<Vec<u8>>;

    fn disable_strict_agg_type_match(&self) -> bool;

    fn auth_expire_secs(&self) -> u64;

    fn compiler_cache_size(&self) -> usize;

    fn query_cache_size(&self) -> u64;

    fn enable_parameterized_rewrite_cache(&self) -> bool;

    fn enable_rewrite_cache(&self) -> bool;

    fn query_cache_time_to_idle_secs(&self) -> u64;

    fn push_down_pull_up_split(&self) -> bool;

    fn stream_mode(&self) -> bool;

    fn non_streaming_query_max_row_limit(&self) -> i32;

    fn max_sessions(&self) -> usize;

    fn no_implicit_order(&self) -> bool;
}

#[derive(Debug, Clone)]
pub struct ConfigObjImpl {
    pub bind_address: Option<String>,
    pub postgres_bind_address: Option<String>,
    pub arrow_native_bind_address: Option<String>,
    pub nonce: Option<Vec<u8>>,
    pub query_timeout: u64,
    pub auth_expire_secs: u64,
    pub timezone: Option<String>,
    pub disable_strict_agg_type_match: bool,
    pub compiler_cache_size: usize,
    pub query_cache_size: u64,
    pub query_cache_time_to_idle_secs: u64,
    pub enable_parameterized_rewrite_cache: bool,
    pub enable_rewrite_cache: bool,
    pub push_down_pull_up_split: bool,
    pub stream_mode: bool,
    pub non_streaming_query_max_row_limit: i32,
    pub max_sessions: usize,
    pub no_implicit_order: bool,
}

impl ConfigObjImpl {
    pub fn default() -> Self {
        let query_timeout = env::var("CUBESQL_QUERY_TIMEOUT")
            .ok()
            .map(|v| v.parse::<u64>().unwrap())
            .unwrap_or(120);
        let sql_push_down = env_parse("CUBESQL_SQL_PUSH_DOWN", true);
        Self {
            bind_address: env::var("CUBESQL_BIND_ADDR").ok().or_else(|| {
                env::var("CUBESQL_PORT")
                    .ok()
                    .map(|v| format!("0.0.0.0:{}", v.parse::<u16>().unwrap()))
            }),
            postgres_bind_address: env::var("CUBESQL_PG_PORT")
                .ok()
                .map(|port| format!("0.0.0.0:{}", port.parse::<u16>().unwrap())),
            arrow_native_bind_address: env::var("CUBEJS_ADBC_PORT")
                .ok()
                .map(|port| format!("0.0.0.0:{}", port.parse::<u16>().unwrap())),
            nonce: None,
            query_timeout,
            timezone: Some("UTC".to_string()),
            disable_strict_agg_type_match: env_parse(
                "CUBESQL_DISABLE_STRICT_AGG_TYPE_MATCH",
                false,
            ),
            auth_expire_secs: env_parse("CUBESQL_AUTH_EXPIRE_SECS", 300),
            compiler_cache_size: env_parse("CUBEJS_COMPILER_CACHE_SIZE", 100),
            query_cache_size: env_parse("CUBESQL_QUERY_CACHE_SIZE", 500),
            query_cache_time_to_idle_secs: env_parse_duration(
                "CUBESQL_QUERY_CACHE_TIME_TO_IDLE",
                60 * 60,
                Some(60 * 60 * 24),
                Some(60),
            ),
            enable_parameterized_rewrite_cache: env_optparse("CUBESQL_PARAMETERIZED_REWRITE_CACHE")
                .unwrap_or(sql_push_down),
            enable_rewrite_cache: env_optparse("CUBESQL_REWRITE_CACHE").unwrap_or(sql_push_down),
            push_down_pull_up_split: env_optparse("CUBESQL_PUSH_DOWN_PULL_UP_SPLIT")
                .unwrap_or(sql_push_down),
            stream_mode: env_parse("CUBESQL_STREAM_MODE", false),
            non_streaming_query_max_row_limit: env_parse("CUBEJS_DB_QUERY_LIMIT", 50000),
            max_sessions: env_parse("CUBEJS_MAX_SESSIONS", 1024),
            no_implicit_order: env_parse("CUBESQL_SQL_NO_IMPLICIT_ORDER", true),
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

    fn arrow_native_bind_address(&self) -> &Option<String> {
        &self.arrow_native_bind_address
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

    fn auth_expire_secs(&self) -> u64 {
        self.auth_expire_secs
    }

    fn compiler_cache_size(&self) -> usize {
        self.compiler_cache_size
    }

    fn query_cache_size(&self) -> u64 {
        self.query_cache_size
    }

    fn enable_parameterized_rewrite_cache(&self) -> bool {
        self.enable_parameterized_rewrite_cache
    }

    fn enable_rewrite_cache(&self) -> bool {
        self.enable_rewrite_cache
    }

    fn query_cache_time_to_idle_secs(&self) -> u64 {
        self.query_cache_time_to_idle_secs
    }

    fn push_down_pull_up_split(&self) -> bool {
        self.push_down_pull_up_split
    }

    fn stream_mode(&self) -> bool {
        self.stream_mode
    }

    fn non_streaming_query_max_row_limit(&self) -> i32 {
        self.non_streaming_query_max_row_limit
    }

    fn no_implicit_order(&self) -> bool {
        self.no_implicit_order
    }

    fn max_sessions(&self) -> usize {
        self.max_sessions
    }
}

impl Config {
    pub fn default() -> Config {
        Config {
            injector: Injector::new(),
            config_obj: Arc::new(ConfigObjImpl::default()),
        }
    }

    pub fn test() -> Config {
        let query_timeout = 15;
        let timezone = Some("UTC".to_string());
        Config {
            injector: Injector::new(),
            config_obj: Arc::new(ConfigObjImpl {
                bind_address: None,
                postgres_bind_address: None,
                arrow_native_bind_address: None,
                nonce: None,
                query_timeout,
                auth_expire_secs: 60,
                timezone,
                disable_strict_agg_type_match: false,
                compiler_cache_size: 100,
                query_cache_size: 500u64,
                enable_parameterized_rewrite_cache: false,
                enable_rewrite_cache: false,
                query_cache_time_to_idle_secs: 1800,
                push_down_pull_up_split: true,
                stream_mode: false,
                non_streaming_query_max_row_limit: 50000,
                max_sessions: 1024,
                no_implicit_order: true,
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
            .register_typed::<dyn ConfigObj, _, _, _>(|_| async move { config_obj_to_register })
            .await;

        // Register HybridTransport (intelligently routes between Http and CubeStore)
        self.injector
            .register_typed_with_default::<dyn TransportService, HybridTransport, _, _>(
                |_| async move {
                    Arc::new(HybridTransport::new().expect("Failed to initialize HybridTransport"))
                },
            )
            .await;

        self.injector
            .register_typed::<dyn PostgresAuthService, _, _, _>(|_| async move {
                Arc::new(PostgresAuthServiceDefaultImpl::new())
            })
            .await;

        self.injector
            .register_typed::<dyn CompilerCache, _, _, _>(|i| async move {
                let config = i.get_service_typed::<dyn ConfigObj>().await;
                Arc::new(CompilerCacheImpl::new(
                    config.clone(),
                    i.get_service_typed().await,
                ))
            })
            .await;

        self.injector
            .register_typed::<ServerManager, _, _, _>(|i| async move {
                let config = i.get_service_typed::<dyn ConfigObj>().await;
                Arc::new(ServerManager::new(
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    config.nonce().clone(),
                    config.clone(),
                ))
            })
            .await;

        self.injector
            .register_typed::<SessionManager, _, _, _>(|i| async move {
                Arc::new(SessionManager::new(i.get_service_typed().await))
            })
            .await;

        self.injector
            .register_typed::<dyn SqlAuthService, _, _, _>(|_| async move {
                Arc::new(SqlAuthDefaultImpl)
            })
            .await;

        if self.config_obj.postgres_bind_address().is_some() {
            self.injector
                .register_typed::<PostgresServer, _, _, _>(|i| async move {
                    let config = i.get_service_typed::<dyn ConfigObj>().await;
                    PostgresServer::new(
                        config.postgres_bind_address().as_ref().unwrap().to_string(),
                        i.get_service_typed().await,
                    )
                })
                .await;
        }

        if self.config_obj.arrow_native_bind_address().is_some() {
            self.injector
                .register_typed::<ArrowNativeServer, _, _, _>(|i| async move {
                    let config = i.get_service_typed::<dyn ConfigObj>().await;
                    ArrowNativeServer::new(
                        config
                            .arrow_native_bind_address()
                            .as_ref()
                            .unwrap()
                            .to_string(),
                        i.get_service_typed().await,
                        i.get_service_typed().await,
                    )
                })
                .await;
        }
    }

    pub async fn cube_services(&self) -> CubeServices {
        CubeServices {
            injector: self.injector.clone(),
            processing_loop_handles: RwLock::new(Vec::new()),
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

pub fn env_parse_duration<T>(name: &str, default: T, max: Option<T>, min: Option<T>) -> T
where
    T: FromStr + PartialOrd + Display,
    T::Err: Display,
{
    let v = match env::var(name).ok() {
        None => {
            return default;
        }
        Some(v) => v,
    };

    let n = match v.parse::<T>() {
        Ok(n) => n,
        Err(e) => panic!(
            "could not parse environment variable '{}' with '{}' value: {}",
            name, v, e
        ),
    };

    if let Some(max) = max {
        if n > max {
            panic!(
                "wrong configuration for environment variable '{}' with '{}' value: greater then max size {}",
                name, v,
                max
            )
        }
    };

    if let Some(min) = min {
        if n < min {
            panic!(
                "wrong configuration for environment variable '{}' with '{}' value: lower then min size {}",
                name, v,
                min
            )
        }
    };

    n
}

pub type LoopHandle = JoinHandle<Result<(), CubeError>>;
