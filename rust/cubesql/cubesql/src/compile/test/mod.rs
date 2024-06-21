use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use cubeclient::models::{
    V1CubeMeta, V1CubeMetaDimension, V1CubeMetaJoin, V1CubeMetaMeasure, V1CubeMetaSegment,
    V1LoadRequestQuery, V1LoadResponse,
};
use datafusion::arrow::datatypes::SchemaRef;
use uuid::Uuid;

use crate::{
    compile::engine::df::{scan::MemberField, wrapper::SqlQuery},
    config::{ConfigObj, ConfigObjImpl},
    sql::{
        compiler_cache::CompilerCacheImpl, session::DatabaseProtocol, AuthContextRef,
        AuthenticateResponse, HttpAuthContext, ServerManager, Session, SessionManager,
        SqlAuthService,
    },
    transport::{
        CubeStreamReceiver, LoadRequestMeta, SpanId, SqlGenerator, SqlResponse, SqlTemplates,
        TransportService,
    },
    CubeError,
};

use super::MetaContext;

pub mod rewrite_engine;

pub fn get_test_meta() -> Vec<V1CubeMeta> {
    vec![
        V1CubeMeta {
            name: "KibanaSampleDataEcommerce".to_string(),
            title: None,
            dimensions: vec![
                V1CubeMetaDimension {
                    name: "KibanaSampleDataEcommerce.order_date".to_string(),
                    _type: "time".to_string(),
                },
                V1CubeMetaDimension {
                    name: "KibanaSampleDataEcommerce.last_mod".to_string(),
                    _type: "time".to_string(),
                },
                V1CubeMetaDimension {
                    name: "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    _type: "string".to_string(),
                },
                V1CubeMetaDimension {
                    name: "KibanaSampleDataEcommerce.notes".to_string(),
                    _type: "string".to_string(),
                },
                V1CubeMetaDimension {
                    name: "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    _type: "number".to_string(),
                },
                V1CubeMetaDimension {
                    name: "KibanaSampleDataEcommerce.has_subscription".to_string(),
                    _type: "boolean".to_string(),
                },
            ],
            measures: vec![
                V1CubeMetaMeasure {
                    name: "KibanaSampleDataEcommerce.count".to_string(),
                    title: None,
                    _type: "number".to_string(),
                    agg_type: Some("count".to_string()),
                },
                V1CubeMetaMeasure {
                    name: "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    title: None,
                    _type: "number".to_string(),
                    agg_type: Some("max".to_string()),
                },
                V1CubeMetaMeasure {
                    name: "KibanaSampleDataEcommerce.sumPrice".to_string(),
                    title: None,
                    _type: "number".to_string(),
                    agg_type: Some("sum".to_string()),
                },
                V1CubeMetaMeasure {
                    name: "KibanaSampleDataEcommerce.minPrice".to_string(),
                    title: None,
                    _type: "number".to_string(),
                    agg_type: Some("min".to_string()),
                },
                V1CubeMetaMeasure {
                    name: "KibanaSampleDataEcommerce.avgPrice".to_string(),
                    title: None,
                    _type: "number".to_string(),
                    agg_type: Some("avg".to_string()),
                },
                V1CubeMetaMeasure {
                    name: "KibanaSampleDataEcommerce.countDistinct".to_string(),
                    title: None,
                    _type: "number".to_string(),
                    agg_type: Some("countDistinct".to_string()),
                },
            ],
            segments: vec![
                V1CubeMetaSegment {
                    name: "KibanaSampleDataEcommerce.is_male".to_string(),
                    title: "Ecommerce Male".to_string(),
                    short_title: "Male".to_string(),
                },
                V1CubeMetaSegment {
                    name: "KibanaSampleDataEcommerce.is_female".to_string(),
                    title: "Ecommerce Female".to_string(),
                    short_title: "Female".to_string(),
                },
            ],
            joins: Some(vec![V1CubeMetaJoin {
                name: "Logs".to_string(),
                relationship: "belongsTo".to_string(),
            }]),
        },
        V1CubeMeta {
            name: "Logs".to_string(),
            title: None,
            dimensions: vec![
                V1CubeMetaDimension {
                    name: "Logs.id".to_string(),
                    _type: "number".to_string(),
                },
                V1CubeMetaDimension {
                    name: "Logs.read".to_string(),
                    _type: "boolean".to_string(),
                },
                V1CubeMetaDimension {
                    name: "Logs.content".to_string(),
                    _type: "string".to_string(),
                },
            ],
            measures: vec![
                V1CubeMetaMeasure {
                    name: "Logs.agentCount".to_string(),
                    title: None,
                    _type: "number".to_string(),
                    agg_type: Some("countDistinct".to_string()),
                },
                V1CubeMetaMeasure {
                    name: "Logs.agentCountApprox".to_string(),
                    title: None,
                    _type: "number".to_string(),
                    agg_type: Some("countDistinctApprox".to_string()),
                },
            ],
            segments: vec![],
            joins: Some(vec![V1CubeMetaJoin {
                name: "NumberCube".to_string(),
                relationship: "belongsTo".to_string(),
            }]),
        },
        V1CubeMeta {
            name: "NumberCube".to_string(),
            title: None,
            dimensions: vec![],
            measures: vec![V1CubeMetaMeasure {
                name: "NumberCube.someNumber".to_string(),
                title: None,
                _type: "number".to_string(),
                agg_type: Some("number".to_string()),
            }],
            segments: vec![],
            joins: None,
        },
        V1CubeMeta {
            name: "WideCube".to_string(),
            title: None,
            dimensions: (0..100)
                .map(|i| V1CubeMetaDimension {
                    name: format!("WideCube.dim{}", i),
                    _type: "number".to_string(),
                })
                .collect(),
            measures: (0..100)
                .map(|i| V1CubeMetaMeasure {
                    name: format!("WideCube.measure{}", i),
                    _type: "number".to_string(),
                    agg_type: Some("number".to_string()),
                    title: None,
                })
                .chain(
                    vec![
                        V1CubeMetaMeasure {
                            name: "KibanaSampleDataEcommerce.count".to_string(),
                            title: None,
                            _type: "number".to_string(),
                            agg_type: Some("count".to_string()),
                        },
                        V1CubeMetaMeasure {
                            name: "KibanaSampleDataEcommerce.maxPrice".to_string(),
                            title: None,
                            _type: "number".to_string(),
                            agg_type: Some("max".to_string()),
                        },
                        V1CubeMetaMeasure {
                            name: "KibanaSampleDataEcommerce.minPrice".to_string(),
                            title: None,
                            _type: "number".to_string(),
                            agg_type: Some("min".to_string()),
                        },
                        V1CubeMetaMeasure {
                            name: "KibanaSampleDataEcommerce.avgPrice".to_string(),
                            title: None,
                            _type: "number".to_string(),
                            agg_type: Some("avg".to_string()),
                        },
                        V1CubeMetaMeasure {
                            name: "KibanaSampleDataEcommerce.countDistinct".to_string(),
                            title: None,
                            _type: "number".to_string(),
                            agg_type: Some("countDistinct".to_string()),
                        },
                    ]
                    .into_iter(),
                )
                .collect(),
            segments: Vec::new(),
            joins: Some(Vec::new()),
        },
    ]
}

pub fn get_string_cube_meta() -> Vec<V1CubeMeta> {
    vec![V1CubeMeta {
        name: "StringCube".to_string(),
        title: None,
        dimensions: vec![],
        measures: vec![V1CubeMetaMeasure {
            name: "StringCube.someString".to_string(),
            title: None,
            _type: "string".to_string(),
            agg_type: Some("string".to_string()),
        }],
        segments: vec![],
        joins: None,
    }]
}

pub fn get_sixteen_char_member_cube() -> Vec<V1CubeMeta> {
    vec![V1CubeMeta {
        name: "SixteenChar".to_string(),
        title: None,
        dimensions: vec![],
        measures: vec![
            V1CubeMetaMeasure {
                name: "SixteenChar.sixteen_charchar".to_string(),
                title: None,
                _type: "number".to_string(),
                agg_type: Some("sum".to_string()),
            },
            V1CubeMetaMeasure {
                name: "SixteenChar.sixteen_charchar_foo".to_string(),
                title: None,
                _type: "number".to_string(),
                agg_type: Some("avg".to_string()),
            },
            V1CubeMetaMeasure {
                name: "SixteenChar.sixteen_charchar_bar".to_string(),
                title: None,
                _type: "number".to_string(),
                agg_type: Some("count".to_string()),
            },
        ],
        segments: vec![],
        joins: None,
    }]
}

#[derive(Debug)]
pub struct SqlGeneratorMock {
    pub sql_templates: Arc<SqlTemplates>,
}

#[async_trait]
impl SqlGenerator for SqlGeneratorMock {
    fn get_sql_templates(&self) -> Arc<SqlTemplates> {
        self.sql_templates.clone()
    }

    async fn call_template(
        &self,
        _name: String,
        _params: HashMap<String, String>,
    ) -> Result<String, CubeError> {
        todo!()
    }
}

pub fn get_test_tenant_ctx() -> Arc<MetaContext> {
    get_test_tenant_ctx_customized(Vec::new())
}

pub fn get_test_tenant_ctx_customized(custom_templates: Vec<(String, String)>) -> Arc<MetaContext> {
    Arc::new(MetaContext::new(
        get_test_meta(),
        vec![
            (
                "KibanaSampleDataEcommerce".to_string(),
                "default".to_string(),
            ),
            ("Logs".to_string(), "default".to_string()),
            ("NumberCube".to_string(), "default".to_string()),
            ("WideCube".to_string(), "default".to_string()),
        ]
        .into_iter()
        .collect(),
        vec![("default".to_string(), sql_generator(custom_templates))]
            .into_iter()
            .collect(),
        Uuid::new_v4(),
    ))
}

fn sql_generator(custom_templates: Vec<(String, String)>) -> Arc<dyn SqlGenerator + Send + Sync> {
    Arc::new(SqlGeneratorMock {
        sql_templates: Arc::new(
            SqlTemplates::new(
                vec![
                    ("functions/COALESCE".to_string(), "COALESCE({{ args_concat }})".to_string()),
                    ("functions/SUM".to_string(), "SUM({{ args_concat }})".to_string()),
                    ("functions/MIN".to_string(), "MIN({{ args_concat }})".to_string()),
                    ("functions/MAX".to_string(), "MAX({{ args_concat }})".to_string()),
                    ("functions/COUNT".to_string(), "COUNT({{ args_concat }})".to_string()),
                    (
                        "functions/COUNT_DISTINCT".to_string(),
                        "COUNT(DISTINCT {{ args_concat }})".to_string(),
                    ),
                    ("functions/AVG".to_string(), "AVG({{ args_concat }})".to_string()),
                    ("functions/APPROXDISTINCT".to_string(), "COUNTDISTINCTAPPROX({{ args_concat }})".to_string()),
                    ("functions/DATETRUNC".to_string(), "DATE_TRUNC({{ args_concat }})".to_string()),
                    ("functions/DATEPART".to_string(), "DATE_PART({{ args_concat }})".to_string()),
                    ("functions/FLOOR".to_string(), "FLOOR({{ args_concat }})".to_string()),
                    ("functions/CEIL".to_string(), "CEIL({{ args_concat }})".to_string()),
                    ("functions/TRUNC".to_string(), "TRUNC({{ args_concat }})".to_string()),
                    ("functions/LEAST".to_string(), "LEAST({{ args_concat }})".to_string()),
                    ("functions/DATEDIFF".to_string(), "DATEDIFF({{ date_part }}, {{ args[1] }}, {{ args[2] }})".to_string()),
                    ("functions/CURRENTDATE".to_string(), "CURRENT_DATE({{ args_concat }})".to_string()),
                    ("functions/DATE_ADD".to_string(), "DATE_ADD({{ args_concat }})".to_string()),
                    ("functions/CONCAT".to_string(), "CONCAT({{ args_concat }})".to_string()),
                    ("functions/DATE".to_string(), "DATE({{ args_concat }})".to_string()),
                    ("functions/LEFT".to_string(), "LEFT({{ args_concat }})".to_string()),
                    ("functions/RIGHT".to_string(), "RIGHT({{ args_concat }})".to_string()),
                    ("functions/LOWER".to_string(), "LOWER({{ args_concat }})".to_string()),
                    ("functions/UPPER".to_string(), "UPPER({{ args_concat }})".to_string()),
                    ("expressions/extract".to_string(), "EXTRACT({{ date_part }} FROM {{ expr }})".to_string()),
                    (
                        "statements/select".to_string(),
                        r#"SELECT {% if distinct %}DISTINCT {% endif %}
  {{ select_concat | map(attribute='aliased') | join(', ') }} 
  {% if from %} 
FROM (
  {{ from | indent(2) }}
) AS {{ from_alias }} {% endif %} {% if filter %}
WHERE {{ filter }}{% endif %}{% if group_by %}
GROUP BY {{ group_by }}{% endif %}{% if order_by %}
ORDER BY {{ order_by | map(attribute='expr') | join(', ') }}{% endif %}{% if limit %}
LIMIT {{ limit }}{% endif %}{% if offset %}
OFFSET {{ offset }}{% endif %}"#.to_string(),
                    ),
                    (
                        "statements/group_by_exprs".to_string(),
                        "{{ group_by | map(attribute='index') | join(', ') }}".to_string(),
                    ),
                    (
                        "expressions/column_aliased".to_string(),
                        "{{expr}} {{quoted_alias}}".to_string(),
                    ),
                    ("expressions/binary".to_string(), "({{ left }} {{ op }} {{ right }})".to_string()),
                    ("expressions/is_null".to_string(), "{{ expr }} IS {% if negate %}NOT {% endif %}NULL".to_string()),
                    ("expressions/case".to_string(), "CASE{% if expr %} {{ expr }}{% endif %}{% for when, then in when_then %} WHEN {{ when }} THEN {{ then }}{% endfor %}{% if else_expr %} ELSE {{ else_expr }}{% endif %} END".to_string()),
                    ("expressions/sort".to_string(), "{{ expr }} {% if asc %}ASC{% else %}DESC{% endif %}{% if nulls_first %} NULLS FIRST {% endif %}".to_string()),
                    ("expressions/cast".to_string(), "CAST({{ expr }} AS {{ data_type }})".to_string()),
                    ("expressions/interval".to_string(), "INTERVAL '{{ interval }}'".to_string()),
                    ("expressions/window_function".to_string(), "{{ fun_call }} OVER ({% if partition_by_concat %}PARTITION BY {{ partition_by_concat }}{% if order_by_concat or window_frame %} {% endif %}{% endif %}{% if order_by_concat %}ORDER BY {{ order_by_concat }}{% if window_frame %} {% endif %}{% endif %}{% if window_frame %}{{ window_frame }}{% endif %})".to_string()),
                    ("expressions/window_frame_bounds".to_string(), "{{ frame_type }} BETWEEN {{ frame_start }} AND {{ frame_end }}".to_string()),
                    ("expressions/in_list".to_string(), "{{ expr }} {% if negated %}NOT {% endif %}IN ({{ in_exprs_concat }})".to_string()),
                    ("expressions/subquery".to_string(), "({{ expr }})".to_string()),
                    ("expressions/in_subquery".to_string(), "{{ expr }} {% if negated %}NOT {% endif %}IN {{ subquery_expr }}".to_string()),
                    ("expressions/rollup".to_string(), "ROLLUP({{ exprs_concat }})".to_string()),
                    ("expressions/cube".to_string(), "CUBE({{ exprs_concat }})".to_string()),
                    ("expressions/negative".to_string(), "-({{ expr }})".to_string()),
                    ("expressions/not".to_string(), "NOT ({{ expr }})".to_string()),
                    ("expressions/true".to_string(), "TRUE".to_string()),
                    ("expressions/false".to_string(), "FALSE".to_string()),
                    ("expressions/timestamp_literal".to_string(), "timestamptz '{{ value }}'".to_string()),
                    ("quotes/identifiers".to_string(), "\"".to_string()),
                    ("quotes/escape".to_string(), "\"\"".to_string()),
                    ("params/param".to_string(), "${{ param_index + 1 }}".to_string()),
                    ("window_frame_types/rows".to_string(), "ROWS".to_string()),
                    ("window_frame_types/range".to_string(), "RANGE".to_string()),
                    ("window_frame_bounds/preceding".to_string(), "{% if n is not none %}{{ n }}{% else %}UNBOUNDED{% endif %} PRECEDING".to_string()),
                    ("window_frame_bounds/current_row".to_string(), "CURRENT ROW".to_string()),
                    ("window_frame_bounds/following".to_string(), "{% if n is not none %}{{ n }}{% else %}UNBOUNDED{% endif %} FOLLOWING".to_string()),
                ]
                    .into_iter().chain(custom_templates.into_iter())
                    .collect(),
                    false,
            )
                .unwrap(),
        ),
    })
}

pub fn get_test_tenant_ctx_with_meta(meta: Vec<V1CubeMeta>) -> Arc<MetaContext> {
    let cube_to_data_source = meta
        .iter()
        .map(|c| (c.name.clone(), "default".to_string()))
        .collect();
    Arc::new(MetaContext::new(
        meta,
        cube_to_data_source,
        vec![("default".to_string(), sql_generator(vec![]))]
            .into_iter()
            .collect(),
        Uuid::new_v4(),
    ))
}

pub async fn get_test_session(
    protocol: DatabaseProtocol,
    meta_context: Arc<MetaContext>,
) -> Arc<Session> {
    get_test_session_with_config(protocol, Arc::new(ConfigObjImpl::default()), meta_context).await
}

pub async fn get_test_session_with_config(
    protocol: DatabaseProtocol,
    config_obj: Arc<dyn ConfigObj>,
    meta_context: Arc<MetaContext>,
) -> Arc<Session> {
    let test_transport = get_test_transport(meta_context);
    let server = Arc::new(ServerManager::new(
        get_test_auth(),
        test_transport.clone(),
        Arc::new(CompilerCacheImpl::new(config_obj.clone(), test_transport)),
        None,
        config_obj,
    ));

    let db_name = match &protocol {
        DatabaseProtocol::MySQL => "db",
        DatabaseProtocol::PostgreSQL => "cubedb",
    };
    let session_manager = Arc::new(SessionManager::new(server.clone()));
    let session = session_manager
        .create_session(protocol, "127.0.0.1".to_string(), 1234)
        .await;

    // Populate like shims
    session.state.set_database(Some(db_name.to_string()));
    session.state.set_user(Some("ovr".to_string()));

    let auth_ctx = HttpAuthContext {
        access_token: "access_token".to_string(),
        base_path: "base_path".to_string(),
    };

    session.state.set_auth_context(Some(Arc::new(auth_ctx)));

    session
}

pub fn get_test_auth() -> Arc<dyn SqlAuthService> {
    #[derive(Debug)]
    struct TestSqlAuth {}

    #[async_trait]
    impl SqlAuthService for TestSqlAuth {
        async fn authenticate(
            &self,
            _user: Option<String>,
            password: Option<String>,
        ) -> Result<AuthenticateResponse, CubeError> {
            Ok(AuthenticateResponse {
                context: Arc::new(HttpAuthContext {
                    access_token: "fake".to_string(),
                    base_path: "fake".to_string(),
                }),
                password,
                skip_password_check: false,
            })
        }
    }

    Arc::new(TestSqlAuth {})
}

pub fn get_test_transport(meta_context: Arc<MetaContext>) -> Arc<dyn TransportService> {
    #[derive(Debug)]
    struct TestConnectionTransport {
        meta_context: Arc<MetaContext>,
    }

    #[async_trait]
    impl TransportService for TestConnectionTransport {
        // Load meta information about cubes
        async fn meta(&self, _ctx: AuthContextRef) -> Result<Arc<MetaContext>, CubeError> {
            Ok(self.meta_context.clone())
        }

        async fn sql(
            &self,
            _span_id: Option<Arc<SpanId>>,
            query: V1LoadRequestQuery,
            _ctx: AuthContextRef,
            _meta_fields: LoadRequestMeta,
            _member_to_alias: Option<HashMap<String, String>>,
            expression_params: Option<Vec<Option<String>>>,
        ) -> Result<SqlResponse, CubeError> {
            Ok(SqlResponse {
                sql: SqlQuery::new(
                    format!("SELECT * FROM {}", serde_json::to_string(&query).unwrap()),
                    expression_params.unwrap_or(Vec::new()),
                ),
            })
        }

        // Execute load query
        async fn load(
            &self,
            _span_id: Option<Arc<SpanId>>,
            _query: V1LoadRequestQuery,
            _sql_query: Option<SqlQuery>,
            _ctx: AuthContextRef,
            _meta_fields: LoadRequestMeta,
        ) -> Result<V1LoadResponse, CubeError> {
            panic!("It's a fake transport");
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
            panic!("It's a fake transport");
        }

        async fn can_switch_user_for_session(
            &self,
            _ctx: AuthContextRef,
            to_user: String,
        ) -> Result<bool, CubeError> {
            if to_user == "good_user" {
                Ok(true)
            } else {
                Ok(false)
            }
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

    Arc::new(TestConnectionTransport { meta_context })
}
