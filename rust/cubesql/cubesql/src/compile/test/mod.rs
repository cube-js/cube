use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use cubeclient::models::{
    V1CubeMeta, V1CubeMetaDimension, V1CubeMetaJoin, V1CubeMetaMeasure, V1CubeMetaSegment,
    V1LoadRequestQuery, V1LoadResponse,
};
use datafusion::arrow::datatypes::SchemaRef;

use crate::{
    compile::engine::df::scan::MemberField,
    sql::{
        session::DatabaseProtocol, AuthContextRef, AuthenticateResponse, HttpAuthContext,
        ServerManager, Session, SessionManager, SqlAuthService,
    },
    transport::{
        CubeStreamReceiver, LoadRequestMeta, SqlGenerator, SqlTemplates, TransportService,
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
        name: String,
        params: HashMap<String, String>,
    ) -> Result<String, CubeError> {
        todo!()
    }
}

pub fn get_test_tenant_ctx() -> Arc<MetaContext> {
    let sql_generator: Arc<dyn SqlGenerator + Send + Sync> = Arc::new(SqlGeneratorMock {
        sql_templates: Arc::new(SqlTemplates {
            functions: vec![
                ("SUM".to_string(), "SUM({{ column }})".to_string()),
                ("MIN".to_string(), "MIN({{ column }})".to_string()),
                ("MAX".to_string(), "MAX({{ column }})".to_string()),
                ("COUNT".to_string(), "COUNT({{ column }})".to_string()),
                (
                    "COUNT_DISTINCT".to_string(),
                    "COUNT(DISTINCT {{ column }})".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
        }),
    });
    Arc::new(MetaContext::new(
        get_test_meta(),
        vec![
            (
                "KibanaSampleDataEcommerce".to_string(),
                "default".to_string(),
            ),
            ("Logs".to_string(), "default".to_string()),
            ("NumberCube".to_string(), "default".to_string()),
        ]
        .into_iter()
        .collect(),
        vec![("default".to_string(), sql_generator)]
            .into_iter()
            .collect(),
    ))
}

pub fn get_test_tenant_ctx_with_meta(meta: Vec<V1CubeMeta>) -> Arc<MetaContext> {
    // TODO
    Arc::new(MetaContext::new(meta, HashMap::new(), HashMap::new()))
}

pub async fn get_test_session(protocol: DatabaseProtocol) -> Arc<Session> {
    let server = Arc::new(ServerManager::new(
        get_test_auth(),
        get_test_transport(),
        None,
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
        ) -> Result<AuthenticateResponse, CubeError> {
            Ok(AuthenticateResponse {
                context: Arc::new(HttpAuthContext {
                    access_token: "fake".to_string(),
                    base_path: "fake".to_string(),
                }),
                password: None,
            })
        }
    }

    Arc::new(TestSqlAuth {})
}

pub fn get_test_transport() -> Arc<dyn TransportService> {
    #[derive(Debug)]
    struct TestConnectionTransport {}

    #[async_trait]
    impl TransportService for TestConnectionTransport {
        // Load meta information about cubes
        async fn meta(&self, _ctx: AuthContextRef) -> Result<Arc<MetaContext>, CubeError> {
            panic!("It's a fake transport");
        }

        // Execute load query
        async fn load(
            &self,
            _query: V1LoadRequestQuery,
            _ctx: AuthContextRef,
            _meta_fields: LoadRequestMeta,
        ) -> Result<V1LoadResponse, CubeError> {
            panic!("It's a fake transport");
        }

        async fn load_stream(
            &self,
            _query: V1LoadRequestQuery,
            _ctx: AuthContextRef,
            _meta_fields: LoadRequestMeta,
            _schema: SchemaRef,
            _member_fields: Vec<MemberField>,
        ) -> Result<CubeStreamReceiver, CubeError> {
            panic!("It's a fake transport");
        }
    }

    Arc::new(TestConnectionTransport {})
}
