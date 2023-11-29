use async_trait::async_trait;
use cubeclient::{
    apis::{configuration::Configuration as ClientConfiguration, default_api as cube_api},
    models::{V1LoadRequest, V1LoadRequestQuery, V1LoadResponse},
};

use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    logical_plan::window_frames::WindowFrame,
    physical_plan::{aggregates::AggregateFunction, window_functions::WindowFunction},
};
use minijinja::{context, value::Value, Environment};
use serde_derive::*;
use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};
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
        member_to_alias: Option<HashMap<String, String>>,
        expression_params: Option<Vec<Option<String>>>,
    ) -> Result<SqlResponse, CubeError>;

    // Execute load query
    async fn load(
        &self,
        query: V1LoadRequestQuery,
        sql_query: Option<SqlQuery>,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
    ) -> Result<V1LoadResponse, CubeError>;

    async fn load_stream(
        &self,
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
}

#[derive(Debug)]
pub struct SqlTemplates {
    pub templates: HashMap<String, String>,
    jinja: Environment<'static>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliasedColumn {
    pub expr: String,
    pub alias: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateColumn {
    pub expr: String,
    pub alias: String,
    pub aliased: String,
    pub index: usize,
}

impl SqlTemplates {
    pub fn new(templates: HashMap<String, String>) -> Result<Self, CubeError> {
        let mut jinja = Environment::new();
        for (name, template) in templates.iter() {
            jinja
                .add_template_owned(name.to_string(), template.to_string())
                .map_err(|e| {
                    CubeError::internal(format!(
                        "Error parsing template {} '{}': {}",
                        name, template, e
                    ))
                })?;
        }

        Ok(Self { templates, jinja })
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
        projection: Vec<AliasedColumn>,
        group_by: Vec<AliasedColumn>,
        aggregate: Vec<AliasedColumn>,
        alias: String,
        _filter: Option<String>,
        _having: Option<String>,
        order_by: Vec<AliasedColumn>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<String, CubeError> {
        let group_by = self.to_template_columns(group_by)?;
        let aggregate = self.to_template_columns(aggregate)?;
        let projection = self.to_template_columns(projection)?;
        let order_by = self.to_template_columns(order_by)?;
        let select_concat = group_by
            .iter()
            .chain(aggregate.iter())
            .chain(projection.iter())
            .map(|c| c.clone())
            .collect::<Vec<_>>();
        self.render_template(
            "statements/select",
            context! {
                from => from,
                select_concat => select_concat,
                group_by => group_by,
                aggregate => aggregate,
                projection => projection,
                order_by => order_by,
                from_alias => alias,
                limit => limit,
                offset => offset,
            },
        )
    }

    fn to_template_columns(
        &self,
        aliased_columns: Vec<AliasedColumn>,
    ) -> Result<Vec<TemplateColumn>, CubeError> {
        aliased_columns
            .into_iter()
            .enumerate()
            .map(|(i, c)| -> Result<_, CubeError> {
                Ok(TemplateColumn {
                    expr: c.expr.to_string(),
                    alias: c.alias.to_string(),
                    aliased: self.alias_expr(&c.expr, &c.alias)?,
                    index: i + 1,
                })
            })
            .collect::<Result<Vec<_>, _>>()
    }

    pub fn alias_expr(&self, expr: &str, alias: &str) -> Result<String, CubeError> {
        let quoted_alias = self.quote_identifier(alias)?;
        self.render_template(
            "expressions/column_aliased",
            context! { alias => alias, expr => expr, quoted_alias => quoted_alias },
        )
    }

    pub fn quote_identifier(&self, column_name: &str) -> Result<String, CubeError> {
        let quote = self
            .templates
            .get("quotes/identifiers")
            .ok_or_else(|| CubeError::user("quotes/identifiers template not found".to_string()))?;
        let escape = self
            .templates
            .get("quotes/escape")
            .ok_or_else(|| CubeError::user("quotes/escape template not found".to_string()))?;
        Ok(format!(
            "{}{}{}",
            quote,
            column_name.replace(quote, escape),
            quote
        ))
    }

    fn render_template(&self, name: &str, ctx: Value) -> Result<String, CubeError> {
        Ok(self
            .jinja
            .get_template(name)
            .map_err(|e| CubeError::internal(format!("Error getting {} template: {}", name, e)))?
            .render(ctx)
            .map_err(|e| {
                CubeError::internal(format!("Error rendering {} template: {}", name, e))
            })?)
    }

    pub fn aggregate_function(
        &self,
        aggregate_function: AggregateFunction,
        args: Vec<String>,
        distinct: bool,
    ) -> Result<String, CubeError> {
        let function = self.aggregate_function_name(aggregate_function, distinct);
        let args_concat = args.join(", ");
        self.render_template(
            &format!("functions/{}", function),
            context! { args_concat => args_concat, args => args, distinct => distinct },
        )
    }

    pub fn scalar_function(
        &self,
        scalar_function: String,
        args: Vec<String>,
        date_part: Option<String>,
        interval: Option<String>,
    ) -> Result<String, CubeError> {
        let function = scalar_function.to_string().to_uppercase();
        let args_concat = args.join(", ");
        self.render_template(
            &format!("functions/{}", function),
            context! {
                args_concat => args_concat,
                args => args,
                date_part => date_part,
                interval => interval,
            },
        )
    }

    pub fn window_function_name(&self, window_function: WindowFunction) -> String {
        match window_function {
            WindowFunction::AggregateFunction(aggregate_function) => {
                self.aggregate_function_name(aggregate_function, false)
            }
            WindowFunction::BuiltInWindowFunction(built_in_window_function) => {
                built_in_window_function.to_string()
            }
        }
    }

    pub fn window_function(
        &self,
        window_function: WindowFunction,
        args: Vec<String>,
    ) -> Result<String, CubeError> {
        let function = self.window_function_name(window_function);
        let args_concat = args.join(", ");
        self.render_template(
            &format!("functions/{}", function),
            context! { args_concat => args_concat, args => args },
        )
    }

    pub fn window_function_expr(
        &self,
        window_function: WindowFunction,
        args: Vec<String>,
        partition_by: Vec<String>,
        order_by: Vec<String>,
        _window_frame: Option<WindowFrame>,
    ) -> Result<String, CubeError> {
        let fun_call = self.window_function(window_function, args)?;
        let partition_by_concat = partition_by.join(", ");
        let order_by_concat = order_by.join(", ");
        // TODO window_frame
        self.render_template(
            "expressions/window_function",
            context! {
                fun_call => fun_call,
                partition_by => partition_by,
                partition_by_concat => partition_by_concat,
                order_by => order_by,
                order_by_concat => order_by_concat
            },
        )
    }

    pub fn case(
        &self,
        expr: Option<String>,
        when_then: Vec<(String, String)>,
        else_expr: Option<String>,
    ) -> Result<String, CubeError> {
        self.render_template(
            "expressions/case",
            context! { expr => expr, when_then => when_then, else_expr => else_expr },
        )
    }

    pub fn binary_expr(
        &self,
        left: String,
        op: String,
        right: String,
    ) -> Result<String, CubeError> {
        self.render_template(
            "expressions/binary",
            context! { left => left, op => op, right => right },
        )
    }

    pub fn is_null_expr(&self, expr: String, negate: bool) -> Result<String, CubeError> {
        self.render_template(
            "expressions/is_null",
            context! { expr => expr, negate => negate },
        )
    }

    pub fn negative_expr(&self, expr: String) -> Result<String, CubeError> {
        self.render_template("expressions/negative", context! { expr => expr })
    }

    pub fn not_expr(&self, expr: String) -> Result<String, CubeError> {
        self.render_template("expressions/not", context! { expr => expr })
    }

    pub fn sort_expr(
        &self,
        expr: String,
        asc: bool,
        nulls_first: bool,
    ) -> Result<String, CubeError> {
        self.render_template(
            "expressions/sort",
            context! { expr => expr, asc => asc, nulls_first => nulls_first },
        )
    }

    pub fn extract_expr(&self, date_part: String, expr: String) -> Result<String, CubeError> {
        self.render_template(
            "expressions/extract",
            context! { date_part => date_part, expr => expr },
        )
    }

    pub fn interval_expr(
        &self,
        interval: String,
        num: i64,
        date_part: String,
    ) -> Result<String, CubeError> {
        self.render_template(
            "expressions/interval",
            context! { interval => interval, num => num, date_part => date_part },
        )
    }

    pub fn cast_expr(&self, expr: String, data_type: String) -> Result<String, CubeError> {
        self.render_template(
            "expressions/cast",
            context! { expr => expr, data_type => data_type },
        )
    }

    pub fn in_list_expr(
        &self,
        expr: String,
        in_exprs: Vec<String>,
        negated: bool,
    ) -> Result<String, CubeError> {
        let in_exprs_concat = in_exprs.join(", ");
        self.render_template(
            "expressions/in_list",
            context! {
                expr => expr,
                in_exprs_concat => in_exprs_concat,
                in_exprs => in_exprs,
                negated => negated
            },
        )
    }

    pub fn param(&self, param_index: usize) -> Result<String, CubeError> {
        self.render_template("params/param", context! { param_index => param_index })
    }
}
