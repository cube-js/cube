use async_trait::async_trait;
use cubeclient::{
    apis::{configuration::Configuration as ClientConfiguration, default_api as cube_api},
    models::{V1LoadRequest, V1LoadRequestQuery, V1LoadResponse},
};

use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    physical_plan::aggregates::AggregateFunction,
};
use serde_derive::*;
use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};
use tera::{Context, Tera};
use tokio::{
    sync::{mpsc::Receiver, RwLock as RwLockAsync},
    time::Instant,
};

use crate::{
    compile::{
        engine::df::{scan::MemberField, wrapper::SqlQuery},
        MetaContext,
    },
    sql::{AuthContextRef, HttpAuthContext},
    CubeError,
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

#[async_trait]
pub trait TransportService: Send + Sync + Debug {
    // Load meta information about cubes
    async fn meta(&self, ctx: AuthContextRef) -> Result<Arc<MetaContext>, CubeError>;

    // Get sql for query to be used in wrapped SQL query
    async fn sql(
        &self,
        query: V1LoadRequestQuery,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
    ) -> Result<SqlResponse, CubeError>;

    // Execute load query
    async fn load(
        &self,
        query: V1LoadRequestQuery,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
    ) -> Result<V1LoadResponse, CubeError>;

    async fn load_stream(
        &self,
        query: V1LoadRequestQuery,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
        schema: SchemaRef,
        member_fields: Vec<MemberField>,
    ) -> Result<CubeStreamReceiver, CubeError>;
}

#[async_trait]
pub trait SqlGenerator: Send + Sync + Debug {
    fn get_sql_templates(&self) -> Arc<SqlTemplates>;

    async fn call_template(
        &self,
        name: String,
        params: HashMap<String, String>,
    ) -> Result<String, CubeError>;
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
        ));

        *store = Some(MetaCacheBucket {
            lifetime: Instant::now(),
            value: value.clone(),
        });

        Ok(value)
    }

    async fn sql(
        &self,
        query: V1LoadRequestQuery,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
    ) -> Result<SqlResponse, CubeError> {
        todo!()
    }

    async fn load(
        &self,
        query: V1LoadRequestQuery,
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
        _query: V1LoadRequestQuery,
        _ctx: AuthContextRef,
        _meta_fields: LoadRequestMeta,
        _schema: SchemaRef,
        _member_fields: Vec<MemberField>,
    ) -> Result<CubeStreamReceiver, CubeError> {
        panic!("Does not work for standalone mode yet");
    }
}

#[derive(Debug)]
pub struct SqlTemplates {
    pub functions: HashMap<String, String>,
    pub statements: HashMap<String, String>,
    tera: Tera,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliasedColumn {
    pub expr: String,
    pub alias: String,
}

impl SqlTemplates {
    pub fn new(
        functions: HashMap<String, String>,
        statements: HashMap<String, String>,
    ) -> Result<Self, CubeError> {
        let mut tera = Tera::default();
        for (name, template) in functions.iter() {
            tera.add_raw_template(&format!("functions/{}", name), template)
                .map_err(|e| {
                    CubeError::internal(format!(
                        "Error parsing template {} '{}': {}",
                        name, template, e
                    ))
                })?;
        }

        for (name, template) in statements.iter() {
            tera.add_raw_template(&format!("statements/{}", name), template)
                .map_err(|e| {
                    CubeError::internal(format!(
                        "Error parsing template {} '{}': {}",
                        name, template, e
                    ))
                })?;
        }

        Ok(Self {
            functions,
            statements,
            tera,
        })
    }

    pub fn aggregate_function_name(
        &self,
        aggregate_function: AggregateFunction,
        distinct: bool,
    ) -> String {
        if aggregate_function == AggregateFunction::Count && distinct {
            return "COUNT_DISTINCT".to_string();
        }
        aggregate_function.to_string()
    }

    pub fn select(
        &self,
        from: String,
        group_by: Vec<AliasedColumn>,
        aggregate: Vec<AliasedColumn>,
        alias: String,
        filter: Option<String>,
        having: Option<String>,
        order_by: Vec<AliasedColumn>,
    ) -> Result<String, CubeError> {
        let mut context = Context::new();
        context.insert("from", &from);
        context.insert("group_by", &group_by);
        context.insert("aggregate", &aggregate);
        context.insert("from_alias", &alias);
        self.tera
            .render("statements/select", &context)
            .map_err(|e| CubeError::internal(format!("Error rendering select template: {}", e)))
    }

    pub fn aggregate_function(
        &self,
        aggregate_function: AggregateFunction,
        args: Vec<String>,
        distinct: bool,
    ) -> Result<String, CubeError> {
        let mut context = Context::new();
        context.insert("args", &args);
        context.insert("distinct", &distinct);
        let function = self.aggregate_function_name(aggregate_function, distinct);
        self.tera
            .render(&format!("functions/{}", function), &context)
            .map_err(|e| {
                CubeError::internal(format!(
                    "Error rendering aggregate template '{}': {}",
                    function, e
                ))
            })
    }
}
