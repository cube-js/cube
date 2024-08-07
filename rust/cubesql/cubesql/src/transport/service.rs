use async_trait::async_trait;
use cubeclient::{
    apis::{configuration::Configuration as ClientConfiguration, default_api as cube_api},
    models::{V1LoadRequest, V1LoadRequestQuery, V1LoadResponse},
};

use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use serde_derive::*;
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{mpsc::Receiver, RwLock as RwLockAsync},
    time::Instant,
};
use uuid::Uuid;

use crate::{
    compile::{
        engine::df::{scan::MemberField, wrapper::SqlQuery},
        MetaContext,
    },
    sql::{AuthContextRef, HttpAuthContext},
    CubeError, RWLockAsync,
};

#[derive(Debug, Clone, Serialize)]
pub struct LoadRequestMeta {
    protocol: String,
    #[serde(rename = "apiType")]
    api_type: String,
    #[serde(rename = "appName")]
    app_name: Option<String>,
    // Optional fields
    #[serde(rename = "changeUser", skip_serializing_if = "Option::is_none")]
    change_user: Option<String>,
}

impl LoadRequestMeta {
    #[must_use]
    pub fn new(protocol: String, api_type: String, app_name: Option<String>) -> Self {
        Self {
            protocol,
            api_type,
            app_name,
            change_user: None,
        }
    }

    pub fn change_user(&self) -> Option<String> {
        self.change_user.clone()
    }

    pub fn set_change_user(&mut self, change_user: Option<String>) {
        self.change_user = change_user;
    }
}

#[derive(Debug, Deserialize)]
pub struct SqlResponse {
    pub sql: SqlQuery,
}

#[derive(Debug)]
pub struct SpanId {
    pub span_id: String,
    pub query_key: serde_json::Value,
    span_start: SystemTime,
    is_data_query: RWLockAsync<bool>,
}

impl SpanId {
    pub fn new(span_id: String, query_key: serde_json::Value) -> Self {
        Self {
            span_id,
            query_key,
            span_start: SystemTime::now(),
            is_data_query: tokio::sync::RwLock::new(false),
        }
    }

    pub async fn set_is_data_query(&self, is_data_query: bool) {
        let mut write = self.is_data_query.write().await;
        *write = is_data_query;
    }

    pub async fn is_data_query(&self) -> bool {
        let read = self.is_data_query.read().await;
        *read
    }

    pub fn duration(&self) -> u64 {
        self.span_start
            .elapsed()
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_millis() as u64
    }
}

#[async_trait]
pub trait TransportService: Send + Sync + Debug {
    // Load meta information about cubes
    async fn meta(&self, ctx: AuthContextRef) -> Result<Arc<MetaContext>, CubeError>;

    async fn compiler_id(&self, ctx: AuthContextRef) -> Result<Uuid, CubeError> {
        let meta = self.meta(ctx).await?;
        Ok(meta.compiler_id)
    }

    // Get sql for query to be used in wrapped SQL query
    async fn sql(
        &self,
        span_id: Option<Arc<SpanId>>,
        query: V1LoadRequestQuery,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
        member_to_alias: Option<HashMap<String, String>>,
        expression_params: Option<Vec<Option<String>>>,
    ) -> Result<SqlResponse, CubeError>;

    // Execute load query
    async fn load(
        &self,
        span_id: Option<Arc<SpanId>>,
        query: V1LoadRequestQuery,
        sql_query: Option<SqlQuery>,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
    ) -> Result<V1LoadResponse, CubeError>;

    async fn load_stream(
        &self,
        span_id: Option<Arc<SpanId>>,
        query: V1LoadRequestQuery,
        sql_query: Option<SqlQuery>,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
        schema: SchemaRef,
        member_fields: Vec<MemberField>,
    ) -> Result<CubeStreamReceiver, CubeError>;

    async fn can_switch_user_for_session(
        &self,
        ctx: AuthContextRef,
        to_user: String,
    ) -> Result<bool, CubeError>;

    async fn log_load_state(
        &self,
        span_id: Option<Arc<SpanId>>,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
        event: String,
        properties: serde_json::Value,
    ) -> Result<(), CubeError>;
}

pub type CubeStreamReceiver = Receiver<Option<Result<RecordBatch, CubeError>>>;

#[derive(Debug)]
struct MetaCacheBucket {
    lifetime: Instant,
    value: Arc<MetaContext>,
}

/// This transports is used in standalone mode
#[derive(Debug)]
pub struct HttpTransport {
    /// We use simple cache to improve DX with standalone mode
    /// because currently we dont persist DF in the SessionState
    /// and it causes a lot of HTTP requests which slow down BI connections
    cache: RwLockAsync<Option<MetaCacheBucket>>,
}

const CACHE_LIFETIME_DURATION: Duration = Duration::from_secs(5);

impl HttpTransport {
    pub fn new() -> Self {
        Self {
            cache: RwLockAsync::new(None),
        }
    }

    fn get_client_config_for_ctx(&self, ctx: AuthContextRef) -> ClientConfiguration {
        let http_ctx = ctx
            .as_any()
            .downcast_ref::<HttpAuthContext>()
            .expect("Unable to cast AuthContext to HttpAuthContext");

        let mut cube_config = ClientConfiguration::default();
        cube_config.bearer_access_token = Some(http_ctx.access_token.clone());
        cube_config.base_path = http_ctx.base_path.clone();

        cube_config
    }
}

crate::di_service!(HttpTransport, [TransportService]);

#[async_trait]
impl TransportService for HttpTransport {
    async fn meta(&self, ctx: AuthContextRef) -> Result<Arc<MetaContext>, CubeError> {
        {
            let store = self.cache.read().await;
            if let Some(cache_bucket) = &*store {
                if cache_bucket.lifetime.elapsed() < CACHE_LIFETIME_DURATION {
                    return Ok(cache_bucket.value.clone());
                };
            };
        }

        let response = cube_api::meta_v1(&self.get_client_config_for_ctx(ctx), true).await?;

        let mut store = self.cache.write().await;
        if let Some(cache_bucket) = &*store {
            if cache_bucket.lifetime.elapsed() < CACHE_LIFETIME_DURATION {
                return Ok(cache_bucket.value.clone());
            }
        };

        // Not used -- doesn't make sense to implement
        let value = Arc::new(MetaContext::new(
            response.cubes.unwrap_or_else(Vec::new),
            HashMap::new(),
            HashMap::new(),
            Uuid::new_v4(),
        ));

        *store = Some(MetaCacheBucket {
            lifetime: Instant::now(),
            value: value.clone(),
        });

        Ok(value)
    }

    async fn sql(
        &self,
        _span_id: Option<Arc<SpanId>>,
        _query: V1LoadRequestQuery,
        _ctx: AuthContextRef,
        _meta_fields: LoadRequestMeta,
        _member_to_alias: Option<HashMap<String, String>>,
        _expression_params: Option<Vec<Option<String>>>,
    ) -> Result<SqlResponse, CubeError> {
        todo!()
    }

    async fn load(
        &self,
        _span_id: Option<Arc<SpanId>>,
        query: V1LoadRequestQuery,
        _sql_query: Option<SqlQuery>,
        ctx: AuthContextRef,
        meta: LoadRequestMeta,
    ) -> Result<V1LoadResponse, CubeError> {
        if meta.change_user().is_some() {
            return Err(CubeError::internal(
                "Changing security context (__user) is not supported in the standalone mode"
                    .to_string(),
            ));
        }

        // TODO: support meta_fields for HTTP
        let request = V1LoadRequest {
            query: Some(query),
            query_type: Some("multi".to_string()),
        };
        let response =
            cube_api::load_v1(&self.get_client_config_for_ctx(ctx), Some(request)).await?;

        Ok(response)
    }

    async fn load_stream(
        &self,
        _span_id: Option<Arc<SpanId>>,
        _query: V1LoadRequestQuery,
        _sql_query: Option<SqlQuery>,
        _ctx: AuthContextRef,
        _meta_fields: LoadRequestMeta,
        _schema: SchemaRef,
        _member_fields: Vec<MemberField>,
    ) -> Result<CubeStreamReceiver, CubeError> {
        panic!("Does not work for standalone mode yet");
    }

    async fn can_switch_user_for_session(
        &self,
        _ctx: AuthContextRef,
        _to_user: String,
    ) -> Result<bool, CubeError> {
        panic!("Does not work for standalone mode yet");
    }

    async fn log_load_state(
        &self,
        span_id: Option<Arc<SpanId>>,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
        event: String,
        properties: serde_json::Value,
    ) -> Result<(), CubeError> {
        println!(
            "Load state: {:?} {:?} {:?} {} {:?}",
            span_id, ctx, meta_fields, event, properties
        );
        Ok(())
    }
}
