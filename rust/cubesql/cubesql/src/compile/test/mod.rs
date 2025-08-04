use std::{collections::HashMap, env, ops::Deref, sync::Arc};

use super::{convert_sql_to_cube_query, CompilationResult, QueryPlan};
use crate::{
    compile::{
        engine::df::{scan::MemberField, wrapper::SqlQuery},
        DatabaseProtocol, StatusFlags,
    },
    config::{ConfigObj, ConfigObjImpl},
    sql::{
        auth_service::SqlAuthServiceAuthenticateRequest, compiler_cache::CompilerCacheImpl,
        dataframe::batches_to_dataframe, pg_auth_service::PostgresAuthServiceDefaultImpl,
        AuthContextRef, AuthenticateResponse, HttpAuthContext, ServerManager, Session,
        SessionManager, SqlAuthService,
    },
    transport::{
        CubeMeta, CubeMetaDimension, CubeMetaJoin, CubeMetaMeasure, CubeMetaSegment,
        CubeStreamReceiver, LoadRequestMeta, MetaContext, SpanId, SqlGenerator, SqlResponse,
        SqlTemplates, TransportLoadRequestQuery, TransportLoadResponse, TransportService,
    },
    CubeError,
};
use async_trait::async_trait;
use cubeclient::models::V1CubeMetaType;
use datafusion::{arrow::datatypes::SchemaRef, dataframe::DataFrame as DFDataFrame};
use uuid::Uuid;

pub mod rewrite_engine;
#[cfg(test)]
pub mod test_bi_workarounds;
#[cfg(test)]
pub mod test_cube_join;
#[cfg(test)]
pub mod test_cube_join_grouped;
#[cfg(test)]
pub mod test_cube_scan;
#[cfg(test)]
pub mod test_df_execution;
#[cfg(test)]
pub mod test_filters;
#[cfg(test)]
pub mod test_introspection;
#[cfg(test)]
pub mod test_udfs;
#[cfg(test)]
pub mod test_user_change;
#[cfg(test)]
pub mod test_wrapper;
pub mod utils;
use crate::compile::{
    arrow::record_batch::RecordBatch, engine::df::scan::convert_transport_response,
};
pub use utils::*;

pub fn get_test_meta() -> Vec<CubeMeta> {
    vec![
        CubeMeta {
            name: "KibanaSampleDataEcommerce".to_string(),
            description: Some("Sample data for tracking eCommerce orders from Kibana".to_string()),
            title: None,
            r#type: V1CubeMetaType::Cube,
            dimensions: vec![
                CubeMetaDimension {
                    name: "KibanaSampleDataEcommerce.id".to_string(),
                    r#type: "number".to_string(),
                    ..CubeMetaDimension::default()
                },
                CubeMetaDimension {
                    name: "KibanaSampleDataEcommerce.order_date".to_string(),
                    r#type: "time".to_string(),
                    ..CubeMetaDimension::default()
                },
                CubeMetaDimension {
                    name: "KibanaSampleDataEcommerce.last_mod".to_string(),
                    r#type: "time".to_string(),
                    ..CubeMetaDimension::default()
                },
                CubeMetaDimension {
                    name: "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    description: Some("Customer gender".to_string()),
                    r#type: "string".to_string(),
                    ..CubeMetaDimension::default()
                },
                CubeMetaDimension {
                    name: "KibanaSampleDataEcommerce.notes".to_string(),
                    r#type: "string".to_string(),
                    ..CubeMetaDimension::default()
                },
                CubeMetaDimension {
                    name: "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    r#type: "number".to_string(),
                    ..CubeMetaDimension::default()
                },
                CubeMetaDimension {
                    name: "KibanaSampleDataEcommerce.has_subscription".to_string(),
                    r#type: "boolean".to_string(),
                    ..CubeMetaDimension::default()
                },
            ],
            measures: vec![
                CubeMetaMeasure {
                    name: "KibanaSampleDataEcommerce.count".to_string(),
                    title: None,
                    short_title: None,
                    description: Some("Events count".to_string()),
                    r#type: "number".to_string(),
                    agg_type: Some("count".to_string()),
                    meta: None,
                    alias_member: None,
                },
                CubeMetaMeasure {
                    name: "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    title: None,
                    short_title: None,
                    description: None,
                    r#type: "number".to_string(),
                    agg_type: Some("max".to_string()),
                    meta: None,
                    alias_member: None,
                },
                CubeMetaMeasure {
                    name: "KibanaSampleDataEcommerce.sumPrice".to_string(),
                    title: None,
                    short_title: None,
                    description: None,
                    r#type: "number".to_string(),
                    agg_type: Some("sum".to_string()),
                    meta: None,
                    alias_member: None,
                },
                CubeMetaMeasure {
                    name: "KibanaSampleDataEcommerce.minPrice".to_string(),
                    title: None,
                    short_title: None,
                    description: None,
                    r#type: "number".to_string(),
                    agg_type: Some("min".to_string()),
                    meta: None,
                    alias_member: None,
                },
                CubeMetaMeasure {
                    name: "KibanaSampleDataEcommerce.avgPrice".to_string(),
                    title: None,
                    short_title: None,
                    description: None,
                    r#type: "number".to_string(),
                    agg_type: Some("avg".to_string()),
                    meta: None,
                    alias_member: None,
                },
                CubeMetaMeasure {
                    name: "KibanaSampleDataEcommerce.countDistinct".to_string(),
                    title: None,
                    short_title: None,
                    description: None,
                    r#type: "number".to_string(),
                    agg_type: Some("countDistinct".to_string()),
                    meta: None,
                    alias_member: None,
                },
            ],
            segments: vec![
                CubeMetaSegment {
                    name: "KibanaSampleDataEcommerce.is_male".to_string(),
                    title: "Ecommerce Male".to_string(),
                    description: Some("Male users segment".to_string()),
                    short_title: "Male".to_string(),
                    meta: None,
                },
                CubeMetaSegment {
                    name: "KibanaSampleDataEcommerce.is_female".to_string(),
                    title: "Ecommerce Female".to_string(),
                    description: None,
                    short_title: "Female".to_string(),
                    meta: None,
                },
            ],
            joins: Some(vec![CubeMetaJoin {
                name: "Logs".to_string(),
                relationship: "belongsTo".to_string(),
            }]),
            folders: None,
            nested_folders: None,
            hierarchies: None,
            meta: None,
        },
        CubeMeta {
            name: "Logs".to_string(),
            description: None,
            title: None,
            r#type: V1CubeMetaType::Cube,
            dimensions: vec![
                CubeMetaDimension {
                    name: "Logs.id".to_string(),
                    r#type: "number".to_string(),
                    ..CubeMetaDimension::default()
                },
                CubeMetaDimension {
                    name: "Logs.read".to_string(),
                    r#type: "boolean".to_string(),
                    ..CubeMetaDimension::default()
                },
                CubeMetaDimension {
                    name: "Logs.content".to_string(),
                    r#type: "string".to_string(),
                    ..CubeMetaDimension::default()
                },
            ],
            measures: vec![
                CubeMetaMeasure {
                    name: "Logs.agentCount".to_string(),
                    title: None,
                    short_title: None,
                    description: None,
                    r#type: "number".to_string(),
                    agg_type: Some("countDistinct".to_string()),
                    meta: None,
                    alias_member: None,
                },
                CubeMetaMeasure {
                    name: "Logs.agentCountApprox".to_string(),
                    title: None,
                    short_title: None,
                    description: None,
                    r#type: "number".to_string(),
                    agg_type: Some("countDistinctApprox".to_string()),
                    meta: None,
                    alias_member: None,
                },
            ],
            segments: vec![],
            joins: Some(vec![CubeMetaJoin {
                name: "NumberCube".to_string(),
                relationship: "belongsTo".to_string(),
            }]),
            folders: None,
            nested_folders: None,
            hierarchies: None,
            meta: None,
        },
        CubeMeta {
            name: "NumberCube".to_string(),
            description: None,
            title: None,
            r#type: V1CubeMetaType::Cube,
            dimensions: vec![],
            measures: vec![CubeMetaMeasure {
                name: "NumberCube.someNumber".to_string(),
                title: None,
                short_title: None,
                description: None,
                r#type: "number".to_string(),
                agg_type: Some("number".to_string()),
                meta: None,
                alias_member: None,
            }],
            segments: vec![],
            joins: None,
            folders: None,
            nested_folders: None,
            hierarchies: None,
            meta: None,
        },
        CubeMeta {
            name: "WideCube".to_string(),
            description: None,
            title: None,
            r#type: V1CubeMetaType::Cube,
            dimensions: (0..100)
                .map(|i| CubeMetaDimension {
                    name: format!("WideCube.dim{}", i),
                    r#type: "number".to_string(),
                    ..CubeMetaDimension::default()
                })
                .collect(),
            measures: (0..100)
                .map(|i| CubeMetaMeasure {
                    name: format!("WideCube.measure{}", i),
                    r#type: "number".to_string(),
                    agg_type: Some("number".to_string()),
                    title: None,
                    short_title: None,
                    description: None,
                    meta: None,
                    alias_member: None,
                })
                .chain(
                    vec![
                        CubeMetaMeasure {
                            name: "WideCube.count".to_string(),
                            title: None,
                            short_title: None,
                            description: None,
                            r#type: "number".to_string(),
                            agg_type: Some("count".to_string()),
                            meta: None,
                            alias_member: None,
                        },
                        CubeMetaMeasure {
                            name: "WideCube.maxPrice".to_string(),
                            title: None,
                            short_title: None,
                            description: None,
                            r#type: "number".to_string(),
                            agg_type: Some("max".to_string()),
                            meta: None,
                            alias_member: None,
                        },
                        CubeMetaMeasure {
                            name: "WideCube.minPrice".to_string(),
                            title: None,
                            short_title: None,
                            description: None,
                            r#type: "number".to_string(),
                            agg_type: Some("min".to_string()),
                            meta: None,
                            alias_member: None,
                        },
                        CubeMetaMeasure {
                            name: "WideCube.avgPrice".to_string(),
                            title: None,
                            short_title: None,
                            description: None,
                            r#type: "number".to_string(),
                            agg_type: Some("avg".to_string()),
                            meta: None,
                            alias_member: None,
                        },
                        CubeMetaMeasure {
                            name: "WideCube.countDistinct".to_string(),
                            title: None,
                            short_title: None,
                            description: None,
                            r#type: "number".to_string(),
                            agg_type: Some("countDistinct".to_string()),
                            meta: None,
                            alias_member: None,
                        },
                    ]
                    .into_iter(),
                )
                .collect(),
            segments: Vec::new(),
            joins: Some(Vec::new()),
            folders: None,
            nested_folders: None,
            hierarchies: None,
            meta: None,
        },
        CubeMeta {
            name: "MultiTypeCube".to_string(),
            description: Some("Test cube with a little bit of everything".to_string()),
            title: None,
            r#type: V1CubeMetaType::Cube,
            dimensions: (0..10)
                .flat_map(|i| {
                    [
                        CubeMetaDimension {
                            name: format!("MultiTypeCube.dim_num{}", i),
                            description: Some(format!("Test numeric dimention {i}")),
                            r#type: "number".to_string(),
                            ..CubeMetaDimension::default()
                        },
                        CubeMetaDimension {
                            name: format!("MultiTypeCube.dim_str{}", i),
                            description: Some(format!("Test string dimention {i}")),
                            r#type: "string".to_string(),
                            ..CubeMetaDimension::default()
                        },
                        CubeMetaDimension {
                            name: format!("MultiTypeCube.dim_date{}", i),
                            description: Some(format!("Test time dimention {i}")),
                            r#type: "time".to_string(),
                            ..CubeMetaDimension::default()
                        },
                    ]
                })
                .collect(),
            measures: (0..10)
                .flat_map(|i| {
                    [
                        CubeMetaMeasure {
                            name: format!("MultiTypeCube.measure_num{}", i),
                            r#type: "number".to_string(),
                            agg_type: Some("number".to_string()),
                            title: None,
                            short_title: None,
                            description: Some(format!("Test number measure {i}")),
                            meta: None,
                            alias_member: None,
                        },
                        CubeMetaMeasure {
                            name: format!("MultiTypeCube.measure_str{}", i),
                            r#type: "string".to_string(),
                            agg_type: Some("max".to_string()),
                            title: None,
                            short_title: None,
                            description: Some(format!("Test max(string) measure {i}")),
                            meta: None,
                            alias_member: None,
                        },
                        CubeMetaMeasure {
                            name: format!("MultiTypeCube.measure_date{}", i),
                            r#type: "time".to_string(),
                            agg_type: Some("max".to_string()),
                            title: None,
                            short_title: None,
                            description: Some(format!("Test max(time) measure {i}")),
                            meta: None,
                            alias_member: None,
                        },
                    ]
                })
                .chain(
                    vec![
                        CubeMetaMeasure {
                            name: "MultiTypeCube.count".to_string(),
                            title: None,
                            short_title: None,
                            description: Some("Test count measure".to_string()),
                            r#type: "number".to_string(),
                            agg_type: Some("count".to_string()),
                            meta: None,
                            alias_member: None,
                        },
                        CubeMetaMeasure {
                            name: "MultiTypeCube.maxPrice".to_string(),
                            title: None,
                            short_title: None,
                            description: Some("Test maxPrice measure".to_string()),
                            r#type: "number".to_string(),
                            agg_type: Some("max".to_string()),
                            meta: None,
                            alias_member: None,
                        },
                        CubeMetaMeasure {
                            name: "MultiTypeCube.minPrice".to_string(),
                            title: None,
                            short_title: None,
                            description: Some("Test minPrice measure".to_string()),
                            r#type: "number".to_string(),
                            agg_type: Some("min".to_string()),
                            meta: None,
                            alias_member: None,
                        },
                        CubeMetaMeasure {
                            name: "MultiTypeCube.avgPrice".to_string(),
                            title: None,
                            short_title: None,
                            description: Some("Test avgPrice measure".to_string()),
                            r#type: "number".to_string(),
                            agg_type: Some("avg".to_string()),
                            meta: None,
                            alias_member: None,
                        },
                        CubeMetaMeasure {
                            name: "MultiTypeCube.countDistinct".to_string(),
                            title: None,
                            short_title: None,
                            description: Some("Test countDistinct measure".to_string()),
                            r#type: "number".to_string(),
                            agg_type: Some("countDistinct".to_string()),
                            meta: None,
                            alias_member: None,
                        },
                    ]
                    .into_iter(),
                )
                .collect(),
            segments: Vec::new(),
            joins: Some(Vec::new()),
            folders: None,
            nested_folders: None,
            hierarchies: None,
            meta: None,
        },
    ]
}

pub fn get_string_cube_meta() -> Vec<CubeMeta> {
    vec![CubeMeta {
        name: "StringCube".to_string(),
        description: None,
        title: None,
        r#type: V1CubeMetaType::Cube,
        dimensions: vec![],
        measures: vec![CubeMetaMeasure {
            name: "StringCube.someString".to_string(),
            title: None,
            short_title: None,
            description: None,
            r#type: "string".to_string(),
            agg_type: Some("string".to_string()),
            meta: None,
            alias_member: None,
        }],
        segments: vec![],
        joins: None,
        folders: None,
        nested_folders: None,
        hierarchies: None,
        meta: None,
    }]
}

pub fn get_sixteen_char_member_cube() -> Vec<CubeMeta> {
    vec![CubeMeta {
        name: "SixteenChar".to_string(),
        description: None,
        title: None,
        r#type: V1CubeMetaType::Cube,
        dimensions: vec![],
        measures: vec![
            CubeMetaMeasure {
                name: "SixteenChar.sixteen_charchar".to_string(),
                title: None,
                short_title: None,
                description: None,
                r#type: "number".to_string(),
                agg_type: Some("sum".to_string()),
                meta: None,
                alias_member: None,
            },
            CubeMetaMeasure {
                name: "SixteenChar.sixteen_charchar_foo".to_string(),
                title: None,
                short_title: None,
                description: None,
                r#type: "number".to_string(),
                agg_type: Some("avg".to_string()),
                meta: None,
                alias_member: None,
            },
            CubeMetaMeasure {
                name: "SixteenChar.sixteen_charchar_bar".to_string(),
                title: None,
                short_title: None,
                description: None,
                r#type: "number".to_string(),
                agg_type: Some("count".to_string()),
                meta: None,
                alias_member: None,
            },
        ],
        segments: vec![],
        joins: None,
        folders: None,
        nested_folders: None,
        hierarchies: None,
        meta: None,
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
    let meta = get_test_meta();
    get_test_tenant_ctx_with_meta_and_templates(meta, custom_templates)
}

pub fn sql_generator(
    custom_templates: Vec<(String, String)>,
) -> Arc<dyn SqlGenerator + Send + Sync> {
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
                    ("functions/NOW".to_string(), "NOW({{ args_concat }})".to_string()),
                    ("functions/DATE_ADD".to_string(), "DATE_ADD({{ args_concat }})".to_string()),
                    ("functions/CONCAT".to_string(), "CONCAT({{ args_concat }})".to_string()),
                    ("functions/DATE".to_string(), "DATE({{ args_concat }})".to_string()),
                    ("functions/LEFT".to_string(), "LEFT({{ args_concat }})".to_string()),
                    ("functions/RIGHT".to_string(), "RIGHT({{ args_concat }})".to_string()),
                    ("functions/LOWER".to_string(), "LOWER({{ args_concat }})".to_string()),
                    ("functions/UPPER".to_string(), "UPPER({{ args_concat }})".to_string()),
                    ("functions/PERCENTILECONT".to_string(), "PERCENTILE_CONT({{ args_concat }})".to_string()),
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
ORDER BY {{ order_by | map(attribute='expr') | join(', ') }}{% endif %}{% if limit is not none %}
LIMIT {{ limit }}{% endif %}{% if offset is not none %}
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
                    ("expressions/is_null".to_string(), "({{ expr }} IS {% if negate %}NOT {% endif %}NULL)".to_string()),
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
                    ("expressions/like".to_string(), "{{ expr }} {% if negated %}NOT {% endif %}LIKE {{ pattern }}".to_string()),
                    ("expressions/ilike".to_string(), "{{ expr }} {% if negated %}NOT {% endif %}ILIKE {{ pattern }}".to_string()),
                    ("expressions/like_escape".to_string(), "{{ like_expr }} ESCAPE {{ escape_char }}".to_string()),
                    ("expressions/within_group".to_string(), "{{ fun_sql }} WITHIN GROUP (ORDER BY {{ within_group_concat }})".to_string()),
                    ("expressions/between".to_string(), "{{ expr }} {% if negated %}NOT {% endif %}BETWEEN {{ low }} AND {{ high }}".to_string()),
                    ("join_types/inner".to_string(), "INNER".to_string()),
                    ("join_types/left".to_string(), "LEFT".to_string()),
                    ("quotes/identifiers".to_string(), "\"".to_string()),
                    ("quotes/escape".to_string(), "\"\"".to_string()),
                    ("params/param".to_string(), "${{ param_index + 1 }}".to_string()),
                    ("window_frame_types/rows".to_string(), "ROWS".to_string()),
                    ("window_frame_types/range".to_string(), "RANGE".to_string()),
                    ("window_frame_bounds/preceding".to_string(), "{% if n is not none %}{{ n }}{% else %}UNBOUNDED{% endif %} PRECEDING".to_string()),
                    ("window_frame_bounds/current_row".to_string(), "CURRENT ROW".to_string()),
                    ("window_frame_bounds/following".to_string(), "{% if n is not none %}{{ n }}{% else %}UNBOUNDED{% endif %} FOLLOWING".to_string()),
                    ("types/string".to_string(), "STRING".to_string()),
                    ("types/boolean".to_string(), "BOOLEAN".to_string()),
                    ("types/tinyint".to_string(), "TINYINT".to_string()),
                    ("types/smallint".to_string(), "SMALLINT".to_string()),
                    ("types/integer".to_string(), "INTEGER".to_string()),
                    ("types/bigint".to_string(), "BIGINT".to_string()),
                    ("types/float".to_string(), "FLOAT".to_string()),
                    ("types/double".to_string(), "DOUBLE".to_string()),
                    ("types/decimal".to_string(), "DECIMAL({{ precision }},{{ scale }})".to_string()),
                    ("types/timestamp".to_string(), "TIMESTAMP".to_string()),
                    ("types/date".to_string(), "DATE".to_string()),
                    ("types/time".to_string(), "TIME".to_string()),
                    ("types/interval".to_string(), "INTERVAL".to_string()),
                    ("types/binary".to_string(), "BINARY".to_string()),
                ]
                    .into_iter().chain(custom_templates)
                    .collect(),
                    false,
            )
                .unwrap(),
        ),
    })
}

fn get_test_tenant_ctx_with_meta_and_templates(
    meta: Vec<CubeMeta>,
    custom_templates: Vec<(String, String)>,
) -> Arc<MetaContext> {
    let member_to_data_source = meta
        .iter()
        .flat_map(|cube| {
            cube.dimensions
                .iter()
                .map(|d| &d.name)
                .chain(cube.measures.iter().map(|m| &m.name))
                .chain(cube.segments.iter().map(|s| &s.name))
        })
        .map(|member| (member.clone(), "default".to_string()))
        .collect();
    Arc::new(MetaContext::new(
        meta,
        member_to_data_source,
        vec![("default".to_string(), sql_generator(custom_templates))]
            .into_iter()
            .collect(),
        Uuid::new_v4(),
    ))
}

pub fn get_test_tenant_ctx_with_meta(meta: Vec<CubeMeta>) -> Arc<MetaContext> {
    get_test_tenant_ctx_with_meta_and_templates(meta, vec![])
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
    get_test_session_with_config_and_transport(
        protocol,
        config_obj,
        get_test_transport(meta_context),
    )
    .await
}

async fn get_test_session_with_config_and_transport(
    protocol: DatabaseProtocol,
    config_obj: Arc<dyn ConfigObj>,
    test_transport: Arc<dyn TransportService>,
) -> Arc<Session> {
    let server = Arc::new(ServerManager::new(
        get_test_auth(),
        test_transport.clone(),
        Arc::new(PostgresAuthServiceDefaultImpl::new()),
        Arc::new(CompilerCacheImpl::new(config_obj.clone(), test_transport)),
        None,
        config_obj,
    ));

    let db_name = match &protocol {
        DatabaseProtocol::MySQL => "db",
        _ => "cubedb",
    };
    let session_manager = Arc::new(SessionManager::new(server.clone()));
    let session = session_manager
        .create_session(protocol, "127.0.0.1".to_string(), 1234, None)
        .await
        .unwrap();

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
            _request: SqlAuthServiceAuthenticateRequest,
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

#[derive(Clone, Debug)]
pub struct TestTransportLoadCall {
    pub query: TransportLoadRequestQuery,
    pub sql_query: Option<SqlQuery>,
    pub ctx: AuthContextRef,
    pub meta: LoadRequestMeta,
}

#[derive(Debug)]
struct TestConnectionTransport {
    meta_context: Arc<MetaContext>,
    load_mocks: tokio::sync::Mutex<Vec<(TransportLoadRequestQuery, TransportLoadResponse)>>,
    load_calls: tokio::sync::Mutex<Vec<TestTransportLoadCall>>,
}

impl TestConnectionTransport {
    pub fn new(meta_context: Arc<MetaContext>) -> Self {
        Self {
            meta_context,
            load_mocks: tokio::sync::Mutex::new(vec![]),
            load_calls: tokio::sync::Mutex::new(vec![]),
        }
    }

    pub async fn load_calls(&self) -> Vec<TestTransportLoadCall> {
        self.load_calls.lock().await.clone()
    }

    pub async fn add_cube_load_mock(
        &self,
        req: TransportLoadRequestQuery,
        res: TransportLoadResponse,
    ) {
        self.load_mocks.lock().await.push((req, res));
    }
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
        query: TransportLoadRequestQuery,
        _ctx: AuthContextRef,
        meta: LoadRequestMeta,
        member_to_alias: Option<HashMap<String, String>>,
        expression_params: Option<Vec<Option<String>>>,
    ) -> Result<SqlResponse, CubeError> {
        let inputs = serde_json::json!({
            "query": query,
            "meta": meta,
            "member_to_alias": member_to_alias,
        });
        Ok(SqlResponse {
            sql: SqlQuery::new(
                format!(
                    "SELECT * FROM {}",
                    serde_json::to_string_pretty(&inputs).unwrap()
                ),
                expression_params.unwrap_or_default(),
            ),
        })
    }

    // Execute load query
    async fn load(
        &self,
        _span_id: Option<Arc<SpanId>>,
        query: TransportLoadRequestQuery,
        sql_query: Option<SqlQuery>,
        ctx: AuthContextRef,
        meta: LoadRequestMeta,
        schema: SchemaRef,
        member_fields: Vec<MemberField>,
    ) -> Result<Vec<RecordBatch>, CubeError> {
        {
            let mut calls = self.load_calls.lock().await;
            calls.push(TestTransportLoadCall {
                query: query.clone(),
                sql_query: sql_query.clone(),
                ctx: ctx.clone(),
                meta: meta.clone(),
            });
        }

        if let Some(sql_query) = sql_query {
            return Err(CubeError::internal(format!(
                "Test transport does not support load with SQL query: {sql_query:?}"
            )));
        }

        let mocks = self.load_mocks.lock().await;
        let Some(res) = mocks
            .iter()
            .find(|(req, _res)| req == &query)
            .map(|(_req, res)| {
                convert_transport_response(res.clone(), schema.clone(), member_fields)
            })
        else {
            return Err(CubeError::internal(format!(
                "Unexpected query in test transport: {query:?}"
            )));
        };

        res
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

fn get_test_transport_priv(meta_context: Arc<MetaContext>) -> Arc<TestConnectionTransport> {
    Arc::new(TestConnectionTransport::new(meta_context))
}

pub fn get_test_transport(meta_context: Arc<MetaContext>) -> Arc<dyn TransportService> {
    get_test_transport_priv(meta_context)
}

pub async fn execute_query_with_flags(
    query: String,
    db: DatabaseProtocol,
) -> Result<(String, StatusFlags), CubeError> {
    TestContext::new(db)
        .await
        .execute_query_with_flags(query)
        .await
}

pub async fn execute_queries_with_flags(
    queries: Vec<String>,
    db: DatabaseProtocol,
) -> Result<(String, StatusFlags), CubeError> {
    TestContext::new(db)
        .await
        .execute_queries_with_flags(queries)
        .await
}

pub async fn execute_query(query: String, db: DatabaseProtocol) -> Result<String, CubeError> {
    TestContext::new(db).await.execute_query(query).await
}

pub struct TestContext {
    meta: Arc<MetaContext>,
    transport: Arc<TestConnectionTransport>,
    config_obj: Arc<dyn ConfigObj>,
    session: Arc<Session>,
}

impl TestContext {
    pub async fn new(db: DatabaseProtocol) -> Self {
        Self::with_config(db, Arc::new(ConfigObjImpl::default())).await
    }

    pub async fn with_custom_templates(
        db: DatabaseProtocol,
        custom_templates: Vec<(String, String)>,
    ) -> Self {
        Self::with_config_and_custom_templates(
            db,
            Arc::new(ConfigObjImpl::default()),
            custom_templates,
        )
        .await
    }

    pub async fn with_config(db: DatabaseProtocol, config_obj: Arc<dyn ConfigObj>) -> Self {
        Self::with_config_and_custom_templates(db, config_obj, vec![]).await
    }

    pub async fn with_config_and_custom_templates(
        db: DatabaseProtocol,
        config_obj: Arc<dyn ConfigObj>,
        custom_templates: Vec<(String, String)>,
    ) -> Self {
        // TODO setenv is not thread-safe, remove this
        env::set_var("TZ", "UTC");

        let meta = get_test_tenant_ctx_customized(custom_templates);
        let transport = get_test_transport_priv(meta.clone());
        let session =
            get_test_session_with_config_and_transport(db, config_obj.clone(), transport.clone())
                .await;

        TestContext {
            meta,
            transport,
            config_obj,
            session,
        }
    }

    pub async fn add_cube_load_mock(
        &self,
        mut req: TransportLoadRequestQuery,
        res: TransportLoadResponse,
    ) {
        // Fill in default limit to simplify passing queries as they were in logical plan
        let config_limit = self.config_obj.non_streaming_query_max_row_limit();
        req.limit = req
            .limit
            .map(|req_limit| req_limit.min(config_limit))
            .or(Some(config_limit));
        self.transport.add_cube_load_mock(req, res).await
    }
    pub async fn load_calls(&self) -> Vec<TestTransportLoadCall> {
        self.transport.load_calls().await
    }

    pub async fn convert_sql_to_cube_query(&self, query: &str) -> CompilationResult<QueryPlan> {
        // TODO push to_string() deeper
        convert_sql_to_cube_query(&query.to_string(), self.meta.clone(), self.session.clone()).await
    }

    pub async fn execute_query_with_flags(
        &self,
        query: impl Deref<Target = str>,
    ) -> Result<(String, StatusFlags), CubeError> {
        self.execute_queries_with_flags([query]).await
    }

    pub async fn execute_queries_with_flags(
        &self,
        queries: impl IntoIterator<Item: Deref<Target = str>>,
    ) -> Result<(String, StatusFlags), CubeError> {
        let mut output: Vec<String> = Vec::new();
        let mut output_flags = StatusFlags::empty();

        for query in queries {
            let query = self
                .convert_sql_to_cube_query(&query)
                .await
                .map_err(|e| CubeError::internal(format!("Error during planning: {}", e)))?;
            match query {
                QueryPlan::DataFusionSelect(plan, ctx) => {
                    let df = DFDataFrame::new(ctx.state, &plan);
                    let batches = df.collect().await?;
                    let frame = batches_to_dataframe(&df.schema().into(), batches)?;

                    output.push(frame.print());
                }
                QueryPlan::MetaTabular(flags, frame) => {
                    output.push(frame.print());
                    output_flags = flags;
                }
                QueryPlan::CreateTempTable(_, _, _, _) => {
                    // nothing to do
                }
                QueryPlan::MetaOk(flags, _) => {
                    output_flags = flags;
                }
            }
        }

        Ok((output.join("\n").to_string(), output_flags))
    }

    pub async fn execute_query(
        &self,
        query: impl Deref<Target = str>,
    ) -> Result<String, CubeError> {
        Ok(self.execute_query_with_flags(query).await?.0)
    }
}

static TEST_LOGGING_INITIALIZED: std::sync::Once = std::sync::Once::new();

pub fn init_testing_logger() {
    TEST_LOGGING_INITIALIZED.call_once(|| {
        let log_level = log::Level::Trace;
        let logger = simple_logger::SimpleLogger::new()
            .with_level(log::Level::Error.to_level_filter())
            .with_module_level("cubeclient", log_level.to_level_filter())
            .with_module_level("cubesql", log_level.to_level_filter())
            .with_module_level("datafusion", log::Level::Warn.to_level_filter())
            .with_module_level("pg-srv", log::Level::Warn.to_level_filter());

        log::set_boxed_logger(Box::new(logger)).unwrap();
        log::set_max_level(log_level.to_level_filter());
    });
}

pub async fn convert_select_to_query_plan_customized(
    query: String,
    db: DatabaseProtocol,
    custom_templates: Vec<(String, String)>,
) -> QueryPlan {
    TestContext::with_custom_templates(db, custom_templates)
        .await
        .convert_sql_to_cube_query(&query)
        .await
        .unwrap()
}

pub async fn convert_select_to_query_plan(query: String, db: DatabaseProtocol) -> QueryPlan {
    convert_select_to_query_plan_customized(query, db, vec![]).await
}

pub async fn convert_select_to_query_plan_with_config(
    query: String,
    db: DatabaseProtocol,
    config_obj: Arc<dyn ConfigObj>,
) -> QueryPlan {
    TestContext::with_config(db, config_obj)
        .await
        .convert_sql_to_cube_query(&query)
        .await
        .unwrap()
}

pub async fn convert_select_to_query_plan_with_meta(
    query: String,
    meta: Vec<CubeMeta>,
) -> QueryPlan {
    env::set_var("TZ", "UTC");

    let meta_context = get_test_tenant_ctx_with_meta(meta);
    let query = convert_sql_to_cube_query(
        &query,
        meta_context.clone(),
        get_test_session(DatabaseProtocol::PostgreSQL, meta_context).await,
    )
    .await;

    query.unwrap()
}
