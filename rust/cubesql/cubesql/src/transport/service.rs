use async_trait::async_trait;
use cubeclient::apis::{
    configuration::Configuration as ClientConfiguration, default_api as cube_api,
};

use datafusion::{
    arrow::{
        datatypes::{DataType, SchemaRef},
        record_batch::RecordBatch,
    },
    logical_plan::window_frames::{WindowFrame, WindowFrameBound, WindowFrameUnits},
    physical_plan::{aggregates::AggregateFunction, windows::WindowFunction},
};
use minijinja::{context, value::Value, Environment};
use serde::{Deserialize, Serialize};
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
        engine::df::{
            scan::{convert_transport_response, MemberField},
            wrapper::{GroupingSetDesc, GroupingSetType, SqlQuery},
        },
        rewrite::LikeType,
    },
    sql::{AuthContextRef, HttpAuthContext},
    transport::{MetaContext, TransportLoadRequest, TransportLoadRequestQuery},
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
        query: TransportLoadRequestQuery,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
        member_to_alias: Option<HashMap<String, String>>,
        expression_params: Option<Vec<Option<String>>>,
    ) -> Result<SqlResponse, CubeError>;

    // Execute load query
    async fn load(
        &self,
        span_id: Option<Arc<SpanId>>,
        query: TransportLoadRequestQuery,
        sql_query: Option<SqlQuery>,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
        schema: SchemaRef,
        member_fields: Vec<MemberField>,
    ) -> Result<Vec<RecordBatch>, CubeError>;

    async fn load_stream(
        &self,
        span_id: Option<Arc<SpanId>>,
        query: TransportLoadRequestQuery,
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
        _query: TransportLoadRequestQuery,
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
        query: TransportLoadRequestQuery,
        _sql_query: Option<SqlQuery>,
        ctx: AuthContextRef,
        meta: LoadRequestMeta,
        schema: SchemaRef,
        member_fields: Vec<MemberField>,
    ) -> Result<Vec<RecordBatch>, CubeError> {
        if meta.change_user().is_some() {
            return Err(CubeError::internal(
                "Changing security context (__user) is not supported in the standalone mode"
                    .to_string(),
            ));
        }

        // TODO: support meta_fields for HTTP
        let request = TransportLoadRequest {
            query: Some(query),
            query_type: Some("multi".to_string()),
        };
        let response =
            cube_api::load_v1(&self.get_client_config_for_ctx(ctx), Some(request)).await?;

        convert_transport_response(response, schema, member_fields)
    }

    async fn load_stream(
        &self,
        _span_id: Option<Arc<SpanId>>,
        _query: TransportLoadRequestQuery,
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

#[derive(Debug)]
pub struct SqlTemplates {
    pub templates: HashMap<String, String>,
    pub reuse_params: bool,
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
    pub fn new(templates: HashMap<String, String>, reuse_params: bool) -> Result<Self, CubeError> {
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

        Ok(Self {
            templates,
            jinja,
            reuse_params,
        })
    }

    pub fn contains_template(&self, template_name: &str) -> bool {
        self.templates.contains_key(template_name)
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
        group_descs: Vec<Option<GroupingSetDesc>>,
        aggregate: Vec<AliasedColumn>,
        alias: String,
        filter: Option<String>,
        _having: Option<String>,
        order_by: Vec<AliasedColumn>,
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: bool,
    ) -> Result<String, CubeError> {
        let group_by = self.to_template_columns(group_by)?;
        let aggregate = self.to_template_columns(aggregate)?;
        let projection = self.to_template_columns(projection)?;
        let order_by = self.to_template_columns(order_by)?;
        let select_concat = group_by
            .iter()
            .chain(aggregate.iter())
            .chain(projection.iter())
            .cloned()
            .collect::<Vec<_>>();
        let quoted_from_alias = self.quote_identifier(&alias)?;
        let has_grouping_sets = group_descs.iter().any(|d| d.is_some());
        let group_by_expr = if has_grouping_sets {
            self.group_by_with_grouping_sets(&group_by, &group_descs)?
        } else {
            self.render_template(
                "statements/group_by_exprs",
                context! { group_by => group_by },
            )?
        };
        self.render_template(
            "statements/select",
            context! {
                from => from,
                select_concat => select_concat,
                group_by => group_by_expr,
                aggregate => aggregate,
                projection => projection,
                order_by => order_by,
                filter => filter,
                from_alias => quoted_from_alias,
                limit => limit,
                offset => offset,
                distinct => distinct,
            },
        )
    }

    fn group_by_with_grouping_sets(
        &self,
        group_by: &Vec<TemplateColumn>,
        group_descs: &Vec<Option<GroupingSetDesc>>,
    ) -> Result<String, CubeError> {
        let mut parts = Vec::new();
        let mut curr_set = Vec::new();
        let mut curr_set_desc = None;
        for (col, desc) in group_by.iter().zip(group_descs.iter()) {
            if desc != &curr_set_desc {
                if let Some(curr_desc) = &curr_set_desc {
                    let part_expr = match curr_desc.group_type {
                        GroupingSetType::Rollup => self.rollup_expr(curr_set)?,
                        GroupingSetType::Cube => self.cube_expr(curr_set)?,
                    };
                    parts.push(part_expr);
                }
                curr_set_desc = desc.clone();
                curr_set = Vec::new();
            }
            if desc.is_some() {
                curr_set.push(col.index.to_string());
            } else {
                parts.push(col.index.to_string())
            }
        }
        if let Some(curr_desc) = &curr_set_desc {
            let part_expr = match curr_desc.group_type {
                GroupingSetType::Rollup => self.rollup_expr(curr_set)?,
                GroupingSetType::Cube => self.cube_expr(curr_set)?,
            };
            parts.push(part_expr);
        }

        Ok(parts.join(", "))
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

    pub fn window_frame(&self, window_frame: Option<WindowFrame>) -> Result<String, CubeError> {
        let Some(window_frame) = window_frame else {
            return Ok("".to_string());
        };

        let type_template = match window_frame.units {
            WindowFrameUnits::Rows => "rows",
            WindowFrameUnits::Range => "range",
            WindowFrameUnits::Groups => "groups",
        };
        let frame_type = self.render_template(
            &format!("window_frame_types/{}", type_template),
            context! {},
        )?;

        let frame_start = self.window_frame_bound(&window_frame.start_bound)?;
        let frame_end = self.window_frame_bound(&window_frame.end_bound)?;

        self.render_template(
            "expressions/window_frame_bounds",
            context! {
                frame_type => frame_type,
                frame_start => frame_start,
                frame_end => frame_end
            },
        )
    }

    pub fn window_frame_bound(&self, frame_bound: &WindowFrameBound) -> Result<String, CubeError> {
        match frame_bound {
            WindowFrameBound::Preceding(n) => {
                self.render_template("window_frame_bounds/preceding", context! { n => n })
            }
            WindowFrameBound::CurrentRow => {
                self.render_template("window_frame_bounds/current_row", context! {})
            }
            WindowFrameBound::Following(n) => {
                self.render_template("window_frame_bounds/following", context! { n => n })
            }
        }
    }

    pub fn window_function_expr(
        &self,
        window_function: WindowFunction,
        args: Vec<String>,
        partition_by: Vec<String>,
        order_by: Vec<String>,
        window_frame: Option<WindowFrame>,
    ) -> Result<String, CubeError> {
        let fun_call = self.window_function(window_function, args)?;
        let partition_by_concat = partition_by.join(", ");
        let order_by_concat = order_by.join(", ");
        let window_frame = self.window_frame(window_frame)?;
        self.render_template(
            "expressions/window_function",
            context! {
                fun_call => fun_call,
                partition_by => partition_by,
                partition_by_concat => partition_by_concat,
                order_by => order_by,
                order_by_concat => order_by_concat,
                window_frame => window_frame
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

    pub fn interval_any_expr(
        &self,
        interval: String,
        num: i64,
        date_part: &'static str,
    ) -> Result<String, CubeError> {
        const INTERVAL_TEMPLATE: &str = "expressions/interval";
        const INTERVAL_SINGLE_TEMPLATE: &str = "expressions/interval_single_date_part";
        if self.contains_template(INTERVAL_TEMPLATE) {
            self.interval_expr(interval)
        } else if self.contains_template(INTERVAL_SINGLE_TEMPLATE) {
            self.interval_single_expr(num, date_part)
        } else {
            Err(CubeError::internal(
                "Interval template generation is not supported".to_string(),
            ))
        }
    }

    pub fn interval_expr(&self, interval: String) -> Result<String, CubeError> {
        self.render_template("expressions/interval", context! { interval => interval })
    }

    pub fn interval_single_expr(
        &self,
        num: i64,
        date_part: &'static str,
    ) -> Result<String, CubeError> {
        self.render_template(
            "expressions/interval_single_date_part",
            context! { num => num, date_part => date_part },
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

    pub fn rollup_expr(&self, exprs: Vec<String>) -> Result<String, CubeError> {
        let exprs_concat = exprs.join(", ");
        self.render_template(
            "expressions/rollup",
            context! {
                exprs_concat => exprs_concat,
            },
        )
    }

    pub fn cube_expr(&self, exprs: Vec<String>) -> Result<String, CubeError> {
        let exprs_concat = exprs.join(", ");
        self.render_template(
            "expressions/cube",
            context! {
                exprs_concat => exprs_concat,
            },
        )
    }

    pub fn subquery_expr(&self, subquery_expr: String) -> Result<String, CubeError> {
        self.render_template(
            "expressions/subquery",
            context! {
                expr => subquery_expr,
            },
        )
    }

    pub fn in_subquery_expr(
        &self,
        expr: String,
        subquery_expr: String,
        negated: bool,
    ) -> Result<String, CubeError> {
        self.render_template(
            "expressions/in_subquery",
            context! {
                expr => expr,
                subquery_expr => subquery_expr,
                negated => negated
            },
        )
    }

    pub fn literal_bool_expr(&self, value: bool) -> Result<String, CubeError> {
        match value {
            true => self.render_template("expressions/true", context! {}),
            false => self.render_template("expressions/false", context! {}),
        }
    }

    pub fn timestamp_literal_expr(&self, value: String) -> Result<String, CubeError> {
        self.render_template("expressions/timestamp_literal", context! { value => value })
    }

    pub fn like_expr(
        &self,
        like_type: LikeType,
        expr: String,
        negated: bool,
        pattern: String,
        escape_char: Option<String>,
    ) -> Result<String, CubeError> {
        let expression_name = match like_type {
            LikeType::Like => "like",
            LikeType::ILike => "ilike",
            _ => {
                return Err(CubeError::internal(format!(
                    "Error rendering template: like type {} is not supported",
                    like_type
                )))
            }
        };

        let rendered_like = self.render_template(
            &format!("expressions/{}", expression_name),
            context! { expr => expr, negated => negated, pattern => pattern },
        )?;

        let Some(escape_char) = escape_char else {
            return Ok(rendered_like);
        };
        self.render_template(
            "expressions/like_escape",
            context! { like_expr => rendered_like, escape_char => escape_char },
        )
    }

    pub fn param(&self, param_index: usize) -> Result<String, CubeError> {
        self.render_template("params/param", context! { param_index => param_index })
    }

    pub fn sql_type(&self, data_type: DataType) -> Result<String, CubeError> {
        let data_type = match data_type {
            DataType::Decimal(precision, scale) => {
                return self.render_template(
                    "types/decimal",
                    context! {
                        precision => precision,
                        scale => scale,
                    },
                )
            }
            // NULL is not a type in databases. In PostgreSQL, untyped NULL is TEXT
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Null => "string",
            DataType::Boolean => "boolean",
            DataType::Int8 | DataType::UInt8 => "tinyint",
            DataType::Int16 | DataType::UInt16 => "smallint",
            DataType::Int32 | DataType::UInt32 => "integer",
            DataType::Int64 | DataType::UInt64 => "bigint",
            DataType::Float16 | DataType::Float32 => "float",
            DataType::Float64 => "double",
            DataType::Timestamp(_, _) => "timestamp",
            DataType::Date32 | DataType::Date64 => "date",
            DataType::Time32(_) | DataType::Time64(_) => "time",
            DataType::Duration(_) | DataType::Interval(_) => "interval",
            DataType::Binary | DataType::FixedSizeBinary(_) | DataType::LargeBinary => "binary",
            dt => {
                return Err(CubeError::internal(format!(
                    "Can't generate SQL for type {:?}: not supported",
                    dt
                )))
            }
        };
        self.render_template(&format!("types/{}", data_type), context! {})
    }

    pub fn left_join(&self) -> Result<String, CubeError> {
        self.render_template("join_types/left", context! {})
    }

    pub fn inner_join(&self) -> Result<String, CubeError> {
        self.render_template("join_types/inner", context! {})
    }
}
