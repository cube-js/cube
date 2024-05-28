use crate::{
    compile::{
        engine::df::scan::{CubeScanNode, DataType, MemberField, WrappedSelectNode},
        rewrite::{extract_exprlist_from_groupping_set, WrappedSelectType},
    },
    config::ConfigObj,
    sql::AuthContextRef,
    transport::{
        AliasedColumn, LoadRequestMeta, MetaContext, SpanId, SqlGenerator, SqlTemplates,
        TransportService,
    },
    CubeError,
};
use chrono::{Days, NaiveDate, SecondsFormat, TimeZone, Utc};
use cubeclient::models::V1LoadRequestQuery;
use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{
        plan::Extension, replace_col, Column, DFSchema, DFSchemaRef, Expr, GroupingSet,
        LogicalPlan, UserDefinedLogicalNode,
    },
    physical_plan::{aggregates::AggregateFunction, functions::BuiltinScalarFunction},
    scalar::ScalarValue,
};
use itertools::Itertools;
use regex::{Captures, Regex};
use serde_derive::*;
use std::{
    any::Any, collections::HashMap, convert::TryInto, fmt, future::Future, iter, pin::Pin, result,
    sync::Arc,
};

#[derive(Debug, Clone, Deserialize)]
pub struct SqlQuery {
    pub sql: String,
    pub values: Vec<Option<String>>,
}

#[derive(Debug, Clone, Serialize)]
struct UngrouppedMemberDef {
    cube_name: String,
    alias: String,
    cube_params: Vec<String>,
    expr: String,
    grouping_set: Option<GroupingSetDesc>,
}

#[derive(Clone, Serialize, Debug, PartialEq, Eq)]
pub enum GroupingSetType {
    Rollup,
    Cube,
}

#[derive(Clone, Serialize, Debug, PartialEq, Eq)]
pub struct GroupingSetDesc {
    pub group_type: GroupingSetType,
    pub id: u64,
    pub sub_id: Option<u64>,
}

impl GroupingSetDesc {
    pub fn new(group_type: GroupingSetType, id: u64) -> Self {
        Self {
            group_type,
            id,
            sub_id: None,
        }
    }
}

fn extract_group_type_from_groupping_set(
    exprs: &Vec<Expr>,
) -> Result<Vec<Option<GroupingSetDesc>>> {
    let mut result = Vec::new();
    let mut id = 0;
    for expr in exprs {
        match expr {
            Expr::GroupingSet(groupping_set) => match groupping_set {
                GroupingSet::Rollup(exprs) => {
                    result.extend(
                        iter::repeat(Some(GroupingSetDesc::new(GroupingSetType::Rollup, id)))
                            .take(exprs.len()),
                    );
                    id += 1;
                }
                GroupingSet::Cube(exprs) => {
                    result.extend(
                        iter::repeat(Some(GroupingSetDesc::new(GroupingSetType::Cube, id)))
                            .take(exprs.len()),
                    );
                    id += 1;
                }
                GroupingSet::GroupingSets(_) => {
                    return Err(DataFusionError::Internal(format!(
                        "SQL generation for GroupingSet is not supported"
                    )))
                }
            },
            _ => result.push(None),
        }
    }
    Ok(result)
}

impl SqlQuery {
    pub fn new(sql: String, values: Vec<Option<String>>) -> Self {
        Self { sql, values }
    }

    pub fn add_value(&mut self, value: Option<String>) -> usize {
        if let Some(index) = self.values.iter().position(|v| v == &value) {
            return index;
        }
        let index = self.values.len();
        self.values.push(value);
        index
    }

    pub fn extend_values(&mut self, values: &Vec<Option<String>>) {
        self.values.extend(values.iter().cloned());
    }

    pub fn replace_sql(&mut self, sql: String) {
        self.sql = sql;
    }

    pub fn unpack(self) -> (String, Vec<Option<String>>) {
        (self.sql, self.values)
    }

    fn render_param(
        &self,
        sql_templates: Arc<SqlTemplates>,
        param_index: Option<&str>,
        rendered_params: &HashMap<usize, String>,
        new_param_index: usize,
    ) -> Result<(usize, String, bool)> {
        let param = param_index
            .ok_or_else(|| DataFusionError::Execution("Missing param match".to_string()))?
            .parse::<usize>()
            .map_err(|e| DataFusionError::Execution(format!("Can't parse param index: {}", e)))?;
        if sql_templates.reuse_params {
            if let Some(rendered_param) = rendered_params.get(&param) {
                return Ok((param, rendered_param.clone(), false));
            }
        }
        Ok((
            param,
            sql_templates
                .param(new_param_index)
                .map_err(|e| DataFusionError::Execution(format!("Can't render param: {}", e)))?,
            true,
        ))
    }

    pub fn finalize_query(&mut self, sql_templates: Arc<SqlTemplates>) -> Result<()> {
        let mut params = Vec::new();
        let mut rendered_params = HashMap::new();
        let regex = Regex::new(r"\$(\d+)\$")
            .map_err(|e| DataFusionError::Execution(format!("Can't parse regex: {}", e)))?;
        let mut res = Ok(());
        let replaced_sql = regex.replace_all(self.sql.as_str(), |c: &Captures<'_>| {
            let param = c.get(1).map(|x| x.as_str());
            match self.render_param(sql_templates.clone(), param, &rendered_params, params.len()) {
                Ok((param_index, param, push_param)) => {
                    if push_param {
                        params.push(self.values[param_index].clone());
                        rendered_params.insert(param_index, param.clone());
                    }
                    param
                }
                Err(e) => {
                    res = Err(e);
                    "".to_string()
                }
            }
        });

        match res {
            Ok(()) => {
                self.sql = replaced_sql.to_string();
                self.values = params;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CubeScanWrapperNode {
    pub wrapped_plan: Arc<LogicalPlan>,
    pub meta: Arc<MetaContext>,
    pub auth_context: AuthContextRef,
    pub wrapped_sql: Option<SqlQuery>,
    pub request: Option<V1LoadRequestQuery>,
    pub member_fields: Option<Vec<MemberField>>,
    pub span_id: Option<Arc<SpanId>>,
    pub config_obj: Arc<dyn ConfigObj>,
}

impl CubeScanWrapperNode {
    pub fn new(
        wrapped_plan: Arc<LogicalPlan>,
        meta: Arc<MetaContext>,
        auth_context: AuthContextRef,
        span_id: Option<Arc<SpanId>>,
        config_obj: Arc<dyn ConfigObj>,
    ) -> Self {
        Self {
            wrapped_plan,
            meta,
            auth_context,
            wrapped_sql: None,
            request: None,
            member_fields: None,
            span_id,
            config_obj,
        }
    }

    pub fn with_sql_and_request(
        &self,
        sql: SqlQuery,
        request: V1LoadRequestQuery,
        member_fields: Vec<MemberField>,
    ) -> Self {
        Self {
            wrapped_plan: self.wrapped_plan.clone(),
            meta: self.meta.clone(),
            auth_context: self.auth_context.clone(),
            wrapped_sql: Some(sql),
            request: Some(request),
            member_fields: Some(member_fields),
            span_id: self.span_id.clone(),
            config_obj: self.config_obj.clone(),
        }
    }
}

fn expr_name(e: &Expr, schema: &Arc<DFSchema>) -> Result<String> {
    match e {
        Expr::Column(col) => Ok(col.name.clone()),
        Expr::Sort { expr, .. } => expr_name(expr, schema),
        _ => e.name(schema),
    }
}

pub struct SqlGenerationResult {
    pub data_source: Option<String>,
    pub from_alias: Option<String>,
    pub column_remapping: Option<HashMap<Column, Column>>,
    pub sql: SqlQuery,
    pub request: V1LoadRequestQuery,
}

lazy_static! {
    static ref DATE_PART_REGEX: Regex = Regex::new("^[A-Za-z_ ]+$").unwrap();
}

macro_rules! generate_sql_for_timestamp {
    (@generic $value:ident, $value_block:expr, $sql_generator:expr, $sql_query:expr) => {
        if let Some($value) = $value {
            let value = $value_block.to_rfc3339_opts(SecondsFormat::Millis, true);
            (
                $sql_generator
                    .get_sql_templates()
                    .timestamp_literal_expr(value)
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Can't generate SQL for timestamp: {}",
                            e
                        ))
                    })?,
                $sql_query,
            )
        } else {
            ("NULL".to_string(), $sql_query)
        }
    };
    ($value:ident, timestamp, $sql_generator:expr, $sql_query:expr) => {
        generate_sql_for_timestamp!(
            @generic $value, { Utc.timestamp_opt($value as i64, 0).unwrap() }, $sql_generator, $sql_query
        )
    };
    ($value:ident, timestamp_millis_opt, $sql_generator:expr, $sql_query:expr) => {
        generate_sql_for_timestamp!(
            @generic $value, { Utc.timestamp_millis_opt($value as i64).unwrap() }, $sql_generator, $sql_query
        )
    };
    ($value:ident, timestamp_micros, $sql_generator:expr, $sql_query:expr) => {
        generate_sql_for_timestamp!(
            @generic $value, { Utc.timestamp_micros($value as i64).unwrap() }, $sql_generator, $sql_query
        )
    };
    ($value:ident, $method:ident, $sql_generator:expr, $sql_query:expr) => {
        generate_sql_for_timestamp!(
            @generic $value, { Utc.$method($value as i64) }, $sql_generator, $sql_query
        )
    };
}

impl CubeScanWrapperNode {
    pub async fn generate_sql(
        &self,
        transport: Arc<dyn TransportService>,
        load_request_meta: Arc<LoadRequestMeta>,
    ) -> result::Result<Self, CubeError> {
        let schema = self.schema();
        let wrapped_plan = self.wrapped_plan.clone();
        let (sql, request, member_fields) = Self::generate_sql_for_node(
            Arc::new(self.clone()),
            transport,
            load_request_meta,
            self.clone().set_max_limit_for_node(wrapped_plan),
            true,
            Vec::new(),
            None,
        )
        .await
        .and_then(|SqlGenerationResult { data_source, mut sql, request, column_remapping, .. }| -> result::Result<_, CubeError> {
            let member_fields = if let Some(column_remapping) = column_remapping {
                schema
                    .fields()
                    .iter()
                    .map(|f| MemberField::Member(column_remapping.get(&Column::from_name(f.name().to_string())).map(|x| x.name.to_string()).unwrap_or(f.name().to_string())))
                    .collect()
            } else {
                schema
                    .fields()
                    .iter()
                    .map(|f| MemberField::Member(f.name().to_string()))
                    .collect()
            };
            let data_source = data_source.ok_or_else(|| CubeError::internal(format!(
                "Can't generate SQL for wrapped select: no data source returned"
            )))?;
            let sql_templates = self
                .meta
                .data_source_to_sql_generator
                .get(&data_source)
                .ok_or_else(|| {
                    CubeError::internal(format!(
                        "Can't generate SQL for wrapped select: no sql generator for '{:?}' data source",
                        data_source
                    ))
                })?
                .get_sql_templates();
            sql.finalize_query(sql_templates).map_err(|e| CubeError::internal(e.to_string()))?;
            Ok((sql, request, member_fields))
        })?;
        Ok(self.with_sql_and_request(sql, request, member_fields))
    }

    pub fn set_max_limit_for_node(self, node: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        let stream_mode = self.config_obj.stream_mode();
        if stream_mode {
            return node;
        }

        let query_limit = self.config_obj.non_streaming_query_max_row_limit();
        match node.as_ref() {
            LogicalPlan::Extension(Extension {
                node: extension_node,
            }) => {
                let cube_scan_node = extension_node
                    .as_any()
                    .downcast_ref::<CubeScanNode>()
                    .cloned();
                let wrapped_select_node = extension_node
                    .as_any()
                    .downcast_ref::<WrappedSelectNode>()
                    .cloned();
                if let Some(node) = cube_scan_node {
                    let mut new_node = node.clone();
                    new_node.request.limit = Some(query_limit);
                    Arc::new(LogicalPlan::Extension(Extension {
                        node: Arc::new(new_node),
                    }))
                } else if let Some(node) = wrapped_select_node {
                    let mut new_node = node.clone();
                    new_node.limit = Some(query_limit as usize);
                    Arc::new(LogicalPlan::Extension(Extension {
                        node: Arc::new(new_node),
                    }))
                } else {
                    node.clone()
                }
            }
            _ => node.clone(),
        }
    }

    pub fn generate_sql_for_node(
        plan: Arc<Self>,
        transport: Arc<dyn TransportService>,
        load_request_meta: Arc<LoadRequestMeta>,
        node: Arc<LogicalPlan>,
        can_rename_columns: bool,
        values: Vec<Option<String>>,
        parent_data_source: Option<String>,
    ) -> Pin<Box<dyn Future<Output = result::Result<SqlGenerationResult, CubeError>> + Send>> {
        Box::pin(async move {
            match node.as_ref() {
                // LogicalPlan::Projection(_) => {}
                // LogicalPlan::Filter(_) => {}
                // LogicalPlan::Window(_) => {}
                // LogicalPlan::Sort(_) => {}
                // LogicalPlan::Join(_) => {}
                // LogicalPlan::CrossJoin(_) => {}
                // LogicalPlan::Repartition(_) => {}
                // LogicalPlan::Union(_) => {}
                // LogicalPlan::TableScan(_) => {}
                // LogicalPlan::EmptyRelation(_) => {}
                // LogicalPlan::Limit(_) => {}
                // LogicalPlan::Subquery(_) => {}
                // LogicalPlan::CreateExternalTable(_) => {}
                // LogicalPlan::CreateMemoryTable(_) => {}
                // LogicalPlan::CreateCatalogSchema(_) => {}
                // LogicalPlan::DropTable(_) => {}
                // LogicalPlan::Values(_) => {}
                // LogicalPlan::Explain(_) => {}
                // LogicalPlan::Analyze(_) => {}
                // LogicalPlan::TableUDFs(_) => {}
                LogicalPlan::Extension(Extension { node }) => {
                    // .cloned() to avoid borrowing Any to comply with Send + Sync
                    let cube_scan_node = node.as_any().downcast_ref::<CubeScanNode>().cloned();
                    let wrapped_select_node =
                        node.as_any().downcast_ref::<WrappedSelectNode>().cloned();
                    if let Some(node) = cube_scan_node {
                        let data_sources = node
                            .used_cubes
                            .iter()
                            .map(|c| plan.meta.cube_to_data_source.get(c).map(|c| c.to_string()))
                            .unique()
                            .collect::<Option<Vec<_>>>().ok_or_else(|| {
                            CubeError::internal(format!(
                                "Can't generate SQL for node due to sql generator can't be found: {:?}",
                                node
                            ))
                        })?;
                        if data_sources.len() != 1 {
                            return Err(CubeError::internal(format!(
                                "Can't generate SQL for node due to multiple data sources {}: {:?}",
                                data_sources.join(","),
                                node
                            )));
                        }
                        let sql = transport
                            .sql(
                                node.span_id.clone(),
                                node.request.clone(),
                                node.auth_context,
                                load_request_meta.as_ref().clone(),
                                Some(
                                    node.member_fields
                                        .iter()
                                        .zip(node.schema.fields().iter())
                                        .filter_map(|(m, field)| match m {
                                            MemberField::Member(f) => {
                                                Some((f.to_string(), field.name().to_string()))
                                            }
                                            _ => None,
                                        })
                                        .collect(),
                                ),
                                None,
                            )
                            .await?;
                        // TODO Add wrapper for reprojection and literal members handling
                        return Ok(SqlGenerationResult {
                            data_source: Some(data_sources[0].clone()),
                            from_alias: node
                                .schema
                                .fields()
                                .iter()
                                .next()
                                .and_then(|f| f.qualifier().cloned()),
                            sql: sql.sql,
                            column_remapping: None,
                            request: node.request.clone(),
                        });
                    } else if let Some(WrappedSelectNode {
                        schema,
                        select_type,
                        projection_expr,
                        subqueries,
                        group_expr,
                        aggr_expr,
                        window_expr,
                        from,
                        joins: _joins,
                        filter_expr,
                        having_expr: _having_expr,
                        limit,
                        offset,
                        order_expr,
                        alias,
                        distinct,
                        ungrouped,
                    }) = wrapped_select_node
                    {
                        // TODO support joins
                        let ungrouped_scan_node = if ungrouped {
                            if let LogicalPlan::Extension(Extension { node }) = from.as_ref() {
                                if let Some(cube_scan_node) =
                                    node.as_any().downcast_ref::<CubeScanNode>()
                                {
                                    Some(Arc::new(cube_scan_node.clone()))
                                } else {
                                    return Err(CubeError::internal(format!(
                                        "Expected CubeScan node but found: {:?}",
                                        plan
                                    )));
                                }
                            } else {
                                return Err(CubeError::internal(format!(
                                    "Expected CubeScan node but found: {:?}",
                                    plan
                                )));
                            }
                        } else {
                            None
                        };

                        let SqlGenerationResult {
                            data_source,
                            from_alias,
                            column_remapping,
                            mut sql,
                            request,
                        } = if let Some(ungrouped_scan_node) = ungrouped_scan_node.clone() {
                            let data_sources = ungrouped_scan_node
                                .used_cubes
                                .iter()
                                .map(|c| plan.meta.cube_to_data_source.get(c).map(|c| c.to_string()))
                                .unique()
                                .collect::<Option<Vec<_>>>().ok_or_else(|| {
                                CubeError::internal(format!(
                                    "Can't generate SQL for node due to sql generator can't be found: {:?}",
                                    ungrouped_scan_node
                                ))
                            })?;
                            if data_sources.len() != 1 {
                                return Err(CubeError::internal(format!(
                                    "Can't generate SQL for node due to multiple data sources {}: {:?}",
                                    data_sources.join(","),
                                    ungrouped_scan_node
                                )));
                            }
                            let sql = SqlQuery::new("".to_string(), values.clone());
                            SqlGenerationResult {
                                data_source: Some(data_sources[0].clone()),
                                from_alias: ungrouped_scan_node
                                    .schema
                                    .fields()
                                    .iter()
                                    .next()
                                    .and_then(|f| f.qualifier().cloned()),
                                column_remapping: None,
                                sql,
                                request: ungrouped_scan_node.request.clone(),
                            }
                        } else {
                            Self::generate_sql_for_node(
                                plan.clone(),
                                transport.clone(),
                                load_request_meta.clone(),
                                from.clone(),
                                true,
                                values.clone(),
                                parent_data_source.clone(),
                            )
                            .await?
                        };

                        let mut subqueries_sql = HashMap::new();
                        for subquery in subqueries.iter() {
                            let SqlGenerationResult {
                                data_source: _,
                                from_alias: _,
                                column_remapping: _,
                                sql: subquery_sql,
                                request: _,
                            } = Self::generate_sql_for_node(
                                plan.clone(),
                                transport.clone(),
                                load_request_meta.clone(),
                                subquery.clone(),
                                true,
                                sql.values.clone(),
                                data_source.clone(),
                            )
                            .await?;

                            let (sql_string, new_values) = subquery_sql.unpack();
                            sql.extend_values(&new_values);
                            let field = subquery.schema().field(0);
                            subqueries_sql.insert(field.qualified_name(), sql_string);
                        }
                        let subqueries_sql = Arc::new(subqueries_sql);
                        let mut next_remapping = HashMap::new();
                        let alias = alias.or(from_alias.clone());
                        if let Some(data_source) = data_source {
                            let generator = plan
                                .meta
                                .data_source_to_sql_generator
                                .get(&data_source)
                                .ok_or_else(|| {
                                    CubeError::internal(format!(
                                        "Can't generate SQL for wrapped select: no sql generator for {:?}",
                                        node
                                    ))
                                })?
                                .clone();
                            let (projection, sql) = Self::generate_column_expr(
                                plan.clone(),
                                schema.clone(),
                                projection_expr.clone(),
                                sql,
                                generator.clone(),
                                &column_remapping,
                                &mut next_remapping,
                                alias.clone(),
                                can_rename_columns,
                                ungrouped_scan_node.clone(),
                                subqueries_sql.clone(),
                            )
                            .await?;
                            let (group_by, sql) = Self::generate_column_expr(
                                plan.clone(),
                                schema.clone(),
                                extract_exprlist_from_groupping_set(&group_expr),
                                sql,
                                generator.clone(),
                                &column_remapping,
                                &mut next_remapping,
                                alias.clone(),
                                can_rename_columns,
                                ungrouped_scan_node.clone(),
                                subqueries_sql.clone(),
                            )
                            .await?;
                            let group_descs = extract_group_type_from_groupping_set(&group_expr)?;
                            let (aggregate, sql) = Self::generate_column_expr(
                                plan.clone(),
                                schema.clone(),
                                aggr_expr.clone(),
                                sql,
                                generator.clone(),
                                &column_remapping,
                                &mut next_remapping,
                                alias.clone(),
                                can_rename_columns,
                                ungrouped_scan_node.clone(),
                                subqueries_sql.clone(),
                            )
                            .await?;

                            let (filter, sql) = Self::generate_column_expr(
                                plan.clone(),
                                schema.clone(),
                                filter_expr.clone(),
                                sql,
                                generator.clone(),
                                &column_remapping,
                                &mut next_remapping,
                                alias.clone(),
                                can_rename_columns,
                                ungrouped_scan_node.clone(),
                                subqueries_sql.clone(),
                            )
                            .await?;

                            let (window, sql) = Self::generate_column_expr(
                                plan.clone(),
                                schema.clone(),
                                window_expr.clone(),
                                sql,
                                generator.clone(),
                                &column_remapping,
                                &mut next_remapping,
                                alias.clone(),
                                can_rename_columns,
                                ungrouped_scan_node.clone(),
                                subqueries_sql.clone(),
                            )
                            .await?;

                            let (order, mut sql) = Self::generate_column_expr(
                                plan.clone(),
                                schema.clone(),
                                order_expr.clone(),
                                sql,
                                generator.clone(),
                                &column_remapping,
                                &mut next_remapping,
                                alias.clone(),
                                can_rename_columns,
                                ungrouped_scan_node.clone(),
                                subqueries_sql.clone(),
                            )
                            .await?;
                            if let Some(ungrouped_scan_node) = ungrouped_scan_node.clone() {
                                let mut load_request = ungrouped_scan_node.request.clone();
                                load_request.measures = Some(
                                    aggregate
                                        .iter()
                                        .map(|m| {
                                            Self::ungrouped_member_def(
                                                m,
                                                &ungrouped_scan_node.used_cubes,
                                            )
                                        })
                                        .chain(
                                            // TODO understand type of projections
                                            projection.iter().map(|m| {
                                                Self::ungrouped_member_def(
                                                    m,
                                                    &ungrouped_scan_node.used_cubes,
                                                )
                                            }),
                                        )
                                        .chain(window.iter().map(|m| {
                                            Self::ungrouped_member_def(
                                                m,
                                                &ungrouped_scan_node.used_cubes,
                                            )
                                        }))
                                        .collect::<Result<_>>()?,
                                );
                                load_request.dimensions = Some(
                                    group_by
                                        .iter()
                                        .zip(group_descs.iter())
                                        .map(|(m, t)| {
                                            Self::dimension_member_def(
                                                m,
                                                &ungrouped_scan_node.used_cubes,
                                                t,
                                            )
                                        })
                                        .collect::<Result<_>>()?,
                                );
                                load_request.segments = Some(
                                    filter
                                        .iter()
                                        .map(|m| {
                                            Self::ungrouped_member_def(
                                                m,
                                                &ungrouped_scan_node.used_cubes,
                                            )
                                        })
                                        .collect::<Result<_>>()?,
                                );
                                if !order_expr.is_empty() {
                                    load_request.order = Some(
                                        order_expr
                                            .iter()
                                            .map(|o| -> Result<_> { match o {
                                                Expr::Sort {
                                                    expr,
                                                    asc,
                                                    ..
                                                } => {
                                                    let col_name = expr_name(&expr, &schema)?;
                                                    let aliased_column = aggr_expr
                                                        .iter()
                                                        .find_position(|e| {
                                                            expr_name(e, &schema).map(|n| &n == &col_name).unwrap_or(false)
                                                        })
                                                        .map(|(i, _)| aggregate[i].clone()).or_else(|| {
                                                            projection_expr
                                                                .iter()
                                                                .find_position(|e| {
                                                                    expr_name(e, &schema).map(|n| &n == &col_name).unwrap_or(false)
                                                                })
                                                                .map(|(i, _)| {
                                                                    projection[i].clone()
                                                                })
                                                        }).or_else(|| {
                                                            group_expr
                                                                .iter()
                                                                .find_position(|e| {
                                                                    expr_name(e, &schema).map(|n| &n == &col_name).unwrap_or(false)
                                                                })
                                                                .map(|(i, _)| group_by[i].clone())
                                                        }).ok_or_else(|| {
                                                            DataFusionError::Execution(format!(
                                                                "Can't find column {} in projection {:?} or aggregate {:?} or group {:?}",
                                                                col_name,
                                                                projection_expr,
                                                                aggr_expr,
                                                                group_expr
                                                            ))
                                                        })?;
                                                    Ok(vec![
                                                        aliased_column.alias.clone(),
                                                        if *asc { "asc".to_string() } else { "desc".to_string() },
                                                    ])
                                                }
                                                _ => Err(DataFusionError::Execution(format!(
                                                    "Expected sort expression, found {:?}",
                                                    o
                                                ))),
                                            }})
                                            .collect::<Result<Vec<_>>>()?,
                                    );
                                }
                                load_request.ungrouped =
                                    if let WrappedSelectType::Projection = select_type {
                                        load_request.ungrouped.clone()
                                    } else {
                                        None
                                    };

                                if let Some(limit) = limit {
                                    load_request.limit = Some(limit as i32);
                                }

                                if let Some(offset) = offset {
                                    load_request.offset = Some(offset as i32);
                                }
                                // TODO time dimensions, filters, segments

                                let sql_response = transport
                                    .sql(
                                        ungrouped_scan_node.span_id.clone(),
                                        load_request.clone(),
                                        ungrouped_scan_node.auth_context.clone(),
                                        load_request_meta.as_ref().clone(),
                                        // TODO use aliases or push everything through names?
                                        None,
                                        Some(sql.values.clone()),
                                    )
                                    .await?;

                                Ok(SqlGenerationResult {
                                    data_source: Some(data_source),
                                    from_alias: alias,
                                    sql: sql_response.sql,
                                    column_remapping: if next_remapping.len() > 0 {
                                        Some(next_remapping)
                                    } else {
                                        None
                                    },
                                    request: load_request.clone(),
                                })
                            } else {
                                let resulting_sql = generator
                                    .get_sql_templates()
                                    .select(
                                        sql.sql.to_string(),
                                        projection,
                                        group_by,
                                        group_descs,
                                        aggregate,
                                        // TODO
                                        from_alias.unwrap_or("".to_string()),
                                        if !filter.is_empty() {
                                            Some(
                                                filter
                                                    .iter()
                                                    .map(|f| f.expr.to_string())
                                                    .join(" AND "),
                                            )
                                        } else {
                                            None
                                        },
                                        None,
                                        order,
                                        limit,
                                        offset,
                                        distinct,
                                    )
                                    .map_err(|e| {
                                        DataFusionError::Internal(format!(
                                            "Can't generate SQL for wrapped select: {}",
                                            e
                                        ))
                                    })?;
                                sql.replace_sql(resulting_sql.clone());
                                Ok(SqlGenerationResult {
                                    data_source: Some(data_source),
                                    from_alias: alias,
                                    sql,
                                    column_remapping: if next_remapping.len() > 0 {
                                        Some(next_remapping)
                                    } else {
                                        None
                                    },
                                    request,
                                })
                            }
                        } else {
                            Err(CubeError::internal(format!(
                                "Can't generate SQL for wrapped select: no data source for {:?}",
                                node
                            )))
                        }
                    } else {
                        return Err(CubeError::internal(format!(
                            "Can't generate SQL for node: {:?}",
                            node
                        )));
                    }
                }
                LogicalPlan::EmptyRelation(_) => Ok(SqlGenerationResult {
                    data_source: parent_data_source,
                    from_alias: None,
                    sql: SqlQuery::new("".to_string(), values.clone()),
                    column_remapping: None,
                    request: V1LoadRequestQuery::new(),
                }),
                // LogicalPlan::Distinct(_) => {}
                x => {
                    return Err(CubeError::internal(format!(
                        "Can't generate SQL for node: {:?}",
                        x
                    )))
                }
            }
        })
    }

    async fn generate_column_expr(
        plan: Arc<Self>,
        schema: DFSchemaRef,
        exprs: Vec<Expr>,
        mut sql: SqlQuery,
        generator: Arc<dyn SqlGenerator>,
        column_remapping: &Option<HashMap<Column, Column>>,
        next_remapping: &mut HashMap<Column, Column>,
        from_alias: Option<String>,
        can_rename_columns: bool,
        ungrouped_scan_node: Option<Arc<CubeScanNode>>,
        subqueries: Arc<HashMap<String, String>>,
    ) -> result::Result<(Vec<AliasedColumn>, SqlQuery), CubeError> {
        let non_id_regex = Regex::new(r"[^a-zA-Z0-9_]")
            .map_err(|e| CubeError::internal(format!("Can't parse regex: {}", e)))?;
        let mut aliased_columns = Vec::new();
        for original_expr in exprs {
            let expr = if let Some(column_remapping) = column_remapping.as_ref() {
                let mut expr = replace_col(
                    original_expr.clone(),
                    &column_remapping.iter().map(|(k, v)| (k, v)).collect(),
                )
                .map_err(|_| {
                    CubeError::internal(format!(
                        "Can't rename columns for expr: {:?}",
                        original_expr
                    ))
                })?;
                if !can_rename_columns {
                    let original_alias = expr_name(&original_expr, &schema)?;
                    if original_alias != expr_name(&expr, &schema)? {
                        expr = Expr::Alias(Box::new(expr), original_alias.clone());
                    }
                }
                expr
            } else {
                original_expr.clone()
            };
            let (expr_sql, new_sql_query) = Self::generate_sql_for_expr(
                plan.clone(),
                sql,
                generator.clone(),
                expr.clone(),
                ungrouped_scan_node.clone(),
                subqueries.clone(),
            )
            .await?;
            let expr_sql =
                Self::escape_interpolation_quotes(expr_sql, ungrouped_scan_node.is_some());
            sql = new_sql_query;

            let original_alias = expr_name(&original_expr, &schema)?;
            let original_alias_key = Column::from_name(&original_alias);
            if let Some(alias_column) = next_remapping.get(&original_alias_key) {
                let alias = alias_column.name.clone();
                aliased_columns.push(AliasedColumn {
                    expr: expr_sql,
                    alias,
                });
                continue;
            }

            let alias = if can_rename_columns {
                let alias = expr_name(&expr, &schema)?;
                let mut truncated_alias = non_id_regex
                    .replace_all(&alias, "_")
                    .trim_start_matches("_")
                    .to_lowercase();
                truncated_alias.truncate(16);
                let mut alias = truncated_alias.clone();
                for i in 1..10000 {
                    if !next_remapping
                        .iter()
                        .any(|(_, v)| v == &Column::from_name(&alias))
                    {
                        break;
                    }
                    alias = format!("{}_{}", truncated_alias, i);
                }
                alias
            } else {
                original_alias.clone()
            };
            if !next_remapping.contains_key(&Column::from_name(&alias)) {
                next_remapping.insert(original_alias_key, Column::from_name(&alias));
                if let Some(from_alias) = &from_alias {
                    next_remapping.insert(
                        Column {
                            name: original_alias.clone(),
                            relation: Some(from_alias.clone()),
                        },
                        Column {
                            name: alias.clone(),
                            relation: Some(from_alias.clone()),
                        },
                    );
                    if let Expr::Column(column) = &original_expr {
                        if let Some(original_relation) = &column.relation {
                            if original_relation != from_alias {
                                next_remapping.insert(
                                    Column {
                                        name: original_alias.clone(),
                                        relation: Some(original_relation.clone()),
                                    },
                                    Column {
                                        name: alias.clone(),
                                        relation: Some(from_alias.clone()),
                                    },
                                );
                            }
                        }
                    }
                }
            } else {
                return Err(CubeError::internal(format!(
                    "Can't generate SQL for column expr: duplicate alias {}",
                    alias
                )));
            }

            aliased_columns.push(AliasedColumn {
                expr: expr_sql,
                alias,
            });
        }
        Ok((aliased_columns, sql))
    }

    fn make_member_def(
        column: &AliasedColumn,
        used_cubes: &Vec<String>,
    ) -> Result<UngrouppedMemberDef> {
        let res = UngrouppedMemberDef {
            cube_name: used_cubes
                .iter()
                .next()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Can't generate SQL for column without cubes: {:?}",
                        column
                    ))
                })?
                .to_string(),
            alias: column.alias.clone(),
            cube_params: used_cubes.clone(),
            expr: column.expr.clone(),
            grouping_set: None,
        };
        Ok(res)
    }

    fn ungrouped_member_def(column: &AliasedColumn, used_cubes: &Vec<String>) -> Result<String> {
        let res = Self::make_member_def(column, used_cubes)?;
        Ok(serde_json::json!(res).to_string())
    }

    fn dimension_member_def(
        column: &AliasedColumn,
        used_cubes: &Vec<String>,
        grouping_type: &Option<GroupingSetDesc>,
    ) -> Result<String> {
        let mut res = Self::make_member_def(column, used_cubes)?;
        res.grouping_set = grouping_type.clone();
        Ok(serde_json::json!(res).to_string())
    }

    pub fn generate_sql_for_expr(
        plan: Arc<Self>,
        mut sql_query: SqlQuery,
        sql_generator: Arc<dyn SqlGenerator>,
        expr: Expr,
        ungrouped_scan_node: Option<Arc<CubeScanNode>>,
        subqueries: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = Result<(String, SqlQuery)>> + Send>> {
        Box::pin(async move {
            match expr {
                Expr::Alias(expr, _) => {
                    let (expr, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        ungrouped_scan_node,
                        subqueries.clone(),
                    )
                    .await?;
                    Ok((expr, sql_query))
                }
                // Expr::OuterColumn(_, _) => {}
                Expr::Column(c) => {
                    if let Some(subquery) = subqueries.get(&c.flat_name()) {
                        Ok((
                            sql_generator
                                .get_sql_templates()
                                .subquery_expr(subquery.clone())
                                .map_err(|e| {
                                    DataFusionError::Internal(format!(
                                        "Can't generate SQL for subquery expr: {}",
                                        e
                                    ))
                                })?,
                            sql_query,
                        ))
                    } else if let Some(scan_node) = ungrouped_scan_node.as_ref() {
                        let field_index = scan_node
                            .schema
                            .fields()
                            .iter()
                            .find_position(|f| {
                                f.name() == &c.name
                                    && match c.relation.as_ref() {
                                        Some(r) => Some(r) == f.qualifier(),
                                        None => true,
                                    }
                            })
                            .ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "Can't find column {} in ungrouped scan node",
                                    c
                                ))
                            })?
                            .0;
                        let member = scan_node.member_fields.get(field_index).ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "Can't find member for column {} in ungrouped scan node",
                                c
                            ))
                        })?;
                        match member {
                            MemberField::Member(member) => {
                                Ok((format!("${{{}}}", member), sql_query))
                            }
                            MemberField::Literal(value) => {
                                Self::generate_sql_for_expr(
                                    plan.clone(),
                                    sql_query,
                                    sql_generator.clone(),
                                    Expr::Literal(value.clone()),
                                    ungrouped_scan_node.clone(),
                                    subqueries.clone(),
                                )
                                .await
                            }
                        }
                    } else {
                        Ok((
                            match c.relation.as_ref() {
                                Some(r) => format!(
                                    "{}.{}",
                                    sql_generator
                                        .get_sql_templates()
                                        .quote_identifier(&r)
                                        .map_err(|e| {
                                            DataFusionError::Internal(format!(
                                                "Can't generate SQL for column: {}",
                                                e
                                            ))
                                        })?,
                                    sql_generator
                                        .get_sql_templates()
                                        .quote_identifier(&c.name)
                                        .map_err(|e| {
                                            DataFusionError::Internal(format!(
                                                "Can't generate SQL for column: {}",
                                                e
                                            ))
                                        })?
                                ),
                                None => sql_generator
                                    .get_sql_templates()
                                    .quote_identifier(&c.name)
                                    .map_err(|e| {
                                        DataFusionError::Internal(format!(
                                            "Can't generate SQL for column: {}",
                                            e
                                        ))
                                    })?,
                            },
                            sql_query,
                        ))
                    }
                }
                // Expr::ScalarVariable(_, _) => {}
                Expr::BinaryExpr { left, op, right } => {
                    let (left, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *left,
                        ungrouped_scan_node.clone(),
                        subqueries.clone(),
                    )
                    .await?;
                    let (right, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *right,
                        ungrouped_scan_node.clone(),
                        subqueries.clone(),
                    )
                    .await?;
                    let resulting_sql = sql_generator
                        .get_sql_templates()
                        .binary_expr(left, op.to_string(), right)
                        .map_err(|e| {
                            DataFusionError::Internal(format!(
                                "Can't generate SQL for binary expr: {}",
                                e
                            ))
                        })?;
                    Ok((resulting_sql, sql_query))
                }
                // Expr::AnyExpr { .. } => {}
                // Expr::Like(_) => {}-=
                // Expr::ILike(_) => {}
                // Expr::SimilarTo(_) => {}
                Expr::Not(expr) => {
                    let (expr, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        ungrouped_scan_node.clone(),
                        subqueries.clone(),
                    )
                    .await?;
                    let resulting_sql =
                        sql_generator
                            .get_sql_templates()
                            .not_expr(expr)
                            .map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Can't generate SQL for not expr: {}",
                                    e
                                ))
                            })?;
                    Ok((resulting_sql, sql_query))
                }
                Expr::IsNotNull(expr) => {
                    let (expr, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        ungrouped_scan_node.clone(),
                        subqueries.clone(),
                    )
                    .await?;
                    let resulting_sql = sql_generator
                        .get_sql_templates()
                        .is_null_expr(expr, true)
                        .map_err(|e| {
                            DataFusionError::Internal(format!(
                                "Can't generate SQL for is not null expr: {}",
                                e
                            ))
                        })?;
                    Ok((resulting_sql, sql_query))
                }
                Expr::IsNull(expr) => {
                    let (expr, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        ungrouped_scan_node.clone(),
                        subqueries.clone(),
                    )
                    .await?;
                    let resulting_sql = sql_generator
                        .get_sql_templates()
                        .is_null_expr(expr, false)
                        .map_err(|e| {
                            DataFusionError::Internal(format!(
                                "Can't generate SQL for is null expr: {}",
                                e
                            ))
                        })?;
                    Ok((resulting_sql, sql_query))
                }
                Expr::Negative(expr) => {
                    let (expr, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        ungrouped_scan_node.clone(),
                        subqueries.clone(),
                    )
                    .await?;
                    let resulting_sql = sql_generator
                        .get_sql_templates()
                        .negative_expr(expr)
                        .map_err(|e| {
                            DataFusionError::Internal(format!(
                                "Can't generate SQL for not expr: {}",
                                e
                            ))
                        })?;
                    Ok((resulting_sql, sql_query))
                }
                // Expr::GetIndexedField { .. } => {}
                // Expr::Between { .. } => {}
                Expr::Case {
                    expr,
                    when_then_expr,
                    else_expr,
                } => {
                    let expr = if let Some(expr) = expr {
                        let (expr, sql_query_next) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            *expr,
                            ungrouped_scan_node.clone(),
                            subqueries.clone(),
                        )
                        .await?;
                        sql_query = sql_query_next;
                        Some(expr)
                    } else {
                        None
                    };
                    let mut when_then_expr_sql = Vec::new();
                    for (when, then) in when_then_expr {
                        let (when, sql_query_next) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            *when,
                            ungrouped_scan_node.clone(),
                            subqueries.clone(),
                        )
                        .await?;
                        let (then, sql_query_next) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query_next,
                            sql_generator.clone(),
                            *then,
                            ungrouped_scan_node.clone(),
                            subqueries.clone(),
                        )
                        .await?;
                        sql_query = sql_query_next;
                        when_then_expr_sql.push((when, then));
                    }
                    let else_expr = if let Some(else_expr) = else_expr {
                        let (else_expr, sql_query_next) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            *else_expr,
                            ungrouped_scan_node.clone(),
                            subqueries.clone(),
                        )
                        .await?;
                        sql_query = sql_query_next;
                        Some(else_expr)
                    } else {
                        None
                    };
                    let resulting_sql = sql_generator
                        .get_sql_templates()
                        .case(expr, when_then_expr_sql, else_expr)
                        .map_err(|e| {
                            DataFusionError::Internal(format!("Can't generate SQL for case: {}", e))
                        })?;
                    Ok((resulting_sql, sql_query))
                }
                Expr::Cast { expr, data_type } => {
                    let (expr, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        ungrouped_scan_node.clone(),
                        subqueries.clone(),
                    )
                    .await?;
                    let data_type = match data_type {
                        DataType::Null => "NULL".to_string(),
                        DataType::Boolean => "BOOLEAN".to_string(),
                        DataType::Int8 => "INTEGER".to_string(),
                        DataType::Int16 => "INTEGER".to_string(),
                        DataType::Int32 => "INTEGER".to_string(),
                        DataType::Int64 => "INTEGER".to_string(),
                        DataType::UInt8 => "INTEGER".to_string(),
                        DataType::UInt16 => "INTEGER".to_string(),
                        DataType::UInt32 => "INTEGER".to_string(),
                        DataType::UInt64 => "INTEGER".to_string(),
                        DataType::Float16 => "FLOAT".to_string(),
                        DataType::Float32 => "FLOAT".to_string(),
                        DataType::Float64 => "DOUBLE PRECISION".to_string(),
                        DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
                        DataType::Date32 => "DATE".to_string(),
                        DataType::Date64 => "DATE".to_string(),
                        DataType::Time32(_) => "TIME".to_string(),
                        DataType::Time64(_) => "TIME".to_string(),
                        DataType::Duration(_) => "INTERVAL".to_string(),
                        DataType::Interval(_) => "INTERVAL".to_string(),
                        DataType::Binary => "BYTEA".to_string(),
                        DataType::FixedSizeBinary(_) => "BYTEA".to_string(),
                        DataType::Utf8 => "TEXT".to_string(),
                        DataType::LargeUtf8 => "TEXT".to_string(),
                        DataType::Decimal(precision, scale) => {
                            format!("NUMERIC({}, {})", precision, scale)
                        }
                        x => {
                            return Err(DataFusionError::Execution(format!(
                                "Can't generate SQL for cast: type isn't supported: {:?}",
                                x
                            )));
                        }
                    };
                    let resulting_sql = sql_generator
                        .get_sql_templates()
                        .cast_expr(expr, data_type)
                        .map_err(|e| {
                            DataFusionError::Internal(format!("Can't generate SQL for cast: {}", e))
                        })?;
                    Ok((resulting_sql, sql_query))
                }
                // Expr::TryCast { .. } => {}
                Expr::Sort {
                    expr,
                    asc,
                    nulls_first,
                } => {
                    let (expr, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        ungrouped_scan_node.clone(),
                        subqueries.clone(),
                    )
                    .await?;
                    let resulting_sql = sql_generator
                        .get_sql_templates()
                        .sort_expr(expr, asc, nulls_first)
                        .map_err(|e| {
                            DataFusionError::Internal(format!(
                                "Can't generate SQL for sort expr: {}",
                                e
                            ))
                        })?;
                    Ok((resulting_sql, sql_query))
                }

                // Expr::TableUDF { .. } => {}
                Expr::Literal(literal) => {
                    Ok(match literal {
                        ScalarValue::Boolean(b) => (
                            b.map(|b| {
                                sql_generator
                                    .get_sql_templates()
                                    .literal_bool_expr(b)
                                    .map_err(|e| {
                                        DataFusionError::Internal(format!(
                                            "Can't generate SQL for literal bool: {}",
                                            e
                                        ))
                                    })
                            })
                            .unwrap_or(Ok("NULL".to_string()))?,
                            sql_query,
                        ),
                        ScalarValue::Float32(f) => (
                            f.map(|f| format!("{}", f)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        ScalarValue::Float64(f) => (
                            f.map(|f| format!("{}", f)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        // ScalarValue::Decimal128(_, _, _) => {}
                        ScalarValue::Int8(x) => (
                            x.map(|x| format!("{}", x)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        ScalarValue::Int16(x) => (
                            x.map(|x| format!("{}", x)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        ScalarValue::Int32(x) => (
                            x.map(|x| format!("{}", x)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        ScalarValue::Int64(x) => (
                            x.map(|x| format!("{}", x)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        ScalarValue::UInt8(x) => (
                            x.map(|x| format!("{}", x)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        ScalarValue::UInt16(x) => (
                            x.map(|x| format!("{}", x)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        ScalarValue::UInt32(x) => (
                            x.map(|x| format!("{}", x)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        ScalarValue::UInt64(x) => (
                            x.map(|x| format!("{}", x)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        ScalarValue::Utf8(x) => {
                            if x.is_some() {
                                let param_index = sql_query.add_value(x);
                                (format!("${}$", param_index), sql_query)
                            } else {
                                ("NULL".into(), sql_query)
                            }
                        }
                        // ScalarValue::LargeUtf8(_) => {}
                        // ScalarValue::Binary(_) => {}
                        // ScalarValue::LargeBinary(_) => {}
                        // ScalarValue::List(_, _) => {}
                        ScalarValue::Date32(x) => {
                            if let Some(x) = x {
                                let days = Days::new(x.abs().try_into().unwrap());
                                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                                let new_date = if x < 0 {
                                    epoch.checked_sub_days(days)
                                } else {
                                    epoch.checked_add_days(days)
                                };
                                let Some(new_date) = new_date else {
                                    return Err(DataFusionError::Internal(format!(
                                        "Can't generate SQL for date: day out of bounds ({})",
                                        x
                                    )));
                                };
                                let formatted_date = new_date.format("%Y-%m-%d").to_string();
                                (
                                    sql_generator
                                        .get_sql_templates()
                                        .scalar_function(
                                            "DATE".to_string(),
                                            vec![format!("'{}'", formatted_date)],
                                            None,
                                            None,
                                        )
                                        .map_err(|e| {
                                            DataFusionError::Internal(format!(
                                                "Can't generate SQL for date: {}",
                                                e
                                            ))
                                        })?,
                                    sql_query,
                                )
                            } else {
                                ("NULL".to_string(), sql_query)
                            }
                        }
                        // ScalarValue::Date64(_) => {}
                        ScalarValue::TimestampSecond(s, _) => {
                            generate_sql_for_timestamp!(s, timestamp, sql_generator, sql_query)
                        }
                        ScalarValue::TimestampMillisecond(ms, None) => {
                            generate_sql_for_timestamp!(
                                ms,
                                timestamp_millis_opt,
                                sql_generator,
                                sql_query
                            )
                        }
                        ScalarValue::TimestampMicrosecond(ms, None) => {
                            generate_sql_for_timestamp!(
                                ms,
                                timestamp_micros,
                                sql_generator,
                                sql_query
                            )
                        }
                        ScalarValue::TimestampNanosecond(nanoseconds, None) => {
                            generate_sql_for_timestamp!(
                                nanoseconds,
                                timestamp_nanos,
                                sql_generator,
                                sql_query
                            )
                        }
                        ScalarValue::IntervalYearMonth(x) => {
                            if let Some(x) = x {
                                let (num, date_part) = (x, "MONTH");
                                let interval = format!("{} {}", num, date_part);
                                (
                                    sql_generator
                                        .get_sql_templates()
                                        .interval_expr(interval, num.into(), date_part.to_string())
                                        .map_err(|e| {
                                            DataFusionError::Internal(format!(
                                                "Can't generate SQL for interval: {}",
                                                e
                                            ))
                                        })?,
                                    sql_query,
                                )
                            } else {
                                ("NULL".to_string(), sql_query)
                            }
                        }
                        ScalarValue::IntervalDayTime(x) => {
                            if let Some(x) = x {
                                let days = x >> 32;
                                let millis = x & 0xFFFFFFFF;
                                if days > 0 && millis > 0 {
                                    return Err(DataFusionError::Internal(format!(
                                        "Can't generate SQL for interval: mixed intervals aren't supported: {} days {} millis encoded as {}",
                                        days, millis, x
                                    )));
                                }
                                let (num, date_part) = if days > 0 {
                                    (days, "DAY")
                                } else {
                                    (millis, "MILLISECOND")
                                };
                                let interval = format!("{} {}", num, date_part);
                                (
                                    sql_generator
                                        .get_sql_templates()
                                        .interval_expr(interval, num, date_part.to_string())
                                        .map_err(|e| {
                                            DataFusionError::Internal(format!(
                                                "Can't generate SQL for interval: {}",
                                                e
                                            ))
                                        })?,
                                    sql_query,
                                )
                            } else {
                                ("NULL".to_string(), sql_query)
                            }
                        }
                        // ScalarValue::IntervalMonthDayNano(_) => {}
                        // ScalarValue::Struct(_, _) => {}
                        ScalarValue::Null => ("NULL".to_string(), sql_query),
                        x => {
                            return Err(DataFusionError::Internal(format!(
                                "Can't generate SQL for literal: {:?}",
                                x
                            )));
                        }
                    })
                }
                Expr::ScalarUDF { fun, args } => {
                    let date_part_err = |dp| {
                        DataFusionError::Internal(format!(
                        "Can't generate SQL for scalar function: date part '{}' is not supported",
                        dp
                    ))
                    };
                    let date_part = match fun.name.as_str() {
                        "datediff" | "dateadd" => match &args[0] {
                            Expr::Literal(ScalarValue::Utf8(Some(date_part))) => {
                                // Security check to prevent SQL injection
                                if DATE_PART_REGEX.is_match(date_part) {
                                    Ok(Some(date_part.to_string()))
                                } else {
                                    Err(date_part_err(date_part))
                                }
                            }
                            _ => Err(date_part_err(&args[0].to_string())),
                        },
                        _ => Ok(None),
                    }?;
                    let interval = match fun.name.as_str() {
                        "dateadd" => match &args[1] {
                            Expr::Literal(ScalarValue::Int64(Some(interval))) => {
                                Ok(Some(interval.to_string()))
                            }
                            _ => Err(DataFusionError::Internal(format!(
                                "Can't generate SQL for scalar function: interval must be Int64"
                            ))),
                        },
                        _ => Ok(None),
                    }?;
                    let mut sql_args = Vec::new();
                    for arg in args {
                        let (sql, query) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            arg,
                            ungrouped_scan_node.clone(),
                            subqueries.clone(),
                        )
                        .await?;
                        sql_query = query;
                        sql_args.push(sql);
                    }
                    Ok((
                        sql_generator
                            .get_sql_templates()
                            .scalar_function(fun.name.to_string(), sql_args, date_part, interval)
                            .map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Can't generate SQL for scalar function: {}",
                                    e
                                ))
                            })?,
                        sql_query,
                    ))
                }
                Expr::ScalarFunction { fun, args } => {
                    if let BuiltinScalarFunction::DatePart = &fun {
                        if args.len() >= 2 {
                            match &args[0] {
                                Expr::Literal(ScalarValue::Utf8(Some(date_part))) => {
                                    // Security check to prevent SQL injection
                                    if !DATE_PART_REGEX.is_match(date_part) {
                                        return Err(DataFusionError::Internal(format!(
                                            "Can't generate SQL for scalar function: date part '{}' is not supported",
                                            date_part
                                        )));
                                    }
                                    let (arg_sql, query) = Self::generate_sql_for_expr(
                                        plan.clone(),
                                        sql_query,
                                        sql_generator.clone(),
                                        args[1].clone(),
                                        ungrouped_scan_node.clone(),
                                        subqueries.clone(),
                                    )
                                    .await?;
                                    return Ok((
                                        sql_generator
                                            .get_sql_templates()
                                            .extract_expr(date_part.to_string(), arg_sql)
                                            .map_err(|e| {
                                                DataFusionError::Internal(format!(
                                                    "Can't generate SQL for scalar function: {}",
                                                    e
                                                ))
                                            })?,
                                        query,
                                    ));
                                }
                                _ => {}
                            }
                        }
                    }
                    let date_part = if let BuiltinScalarFunction::DateTrunc = &fun {
                        match &args[0] {
                            Expr::Literal(ScalarValue::Utf8(Some(date_part))) => {
                                // Security check to prevent SQL injection
                                if DATE_PART_REGEX.is_match(date_part) {
                                    Some(date_part.to_string())
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        }
                    } else {
                        None
                    };
                    let mut sql_args = Vec::new();
                    for arg in args {
                        let (sql, query) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            arg,
                            ungrouped_scan_node.clone(),
                            subqueries.clone(),
                        )
                        .await?;
                        sql_query = query;
                        sql_args.push(sql);
                    }
                    Ok((
                        sql_generator
                            .get_sql_templates()
                            .scalar_function(fun.to_string(), sql_args, date_part, None)
                            .map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Can't generate SQL for scalar function: {}",
                                    e
                                ))
                            })?,
                        sql_query,
                    ))
                }
                Expr::AggregateFunction {
                    fun,
                    args,
                    distinct,
                } => {
                    let mut sql_args = Vec::new();
                    for arg in args {
                        if let AggregateFunction::Count = fun {
                            if !distinct {
                                if let Expr::Literal(_) = arg {
                                    sql_args.push("*".to_string());
                                    break;
                                }
                            }
                        }
                        let (sql, query) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            arg,
                            ungrouped_scan_node.clone(),
                            subqueries.clone(),
                        )
                        .await?;
                        sql_query = query;
                        sql_args.push(sql);
                    }
                    Ok((
                        sql_generator
                            .get_sql_templates()
                            .aggregate_function(fun, sql_args, distinct)
                            .map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Can't generate SQL for aggregate function: {}",
                                    e
                                ))
                            })?,
                        sql_query,
                    ))
                }
                Expr::GroupingSet(grouping_set) => match grouping_set {
                    datafusion::logical_plan::GroupingSet::Rollup(exprs) => {
                        let mut sql_exprs = Vec::new();
                        for expr in exprs {
                            let (sql, query) = Self::generate_sql_for_expr(
                                plan.clone(),
                                sql_query,
                                sql_generator.clone(),
                                expr,
                                ungrouped_scan_node.clone(),
                                subqueries.clone(),
                            )
                            .await?;
                            sql_query = query;
                            sql_exprs.push(sql);
                        }
                        Ok((
                            sql_generator
                                .get_sql_templates()
                                .rollup_expr(sql_exprs)
                                .map_err(|e| {
                                    DataFusionError::Internal(format!(
                                        "Can't generate SQL for rollup expression: {}",
                                        e
                                    ))
                                })?,
                            sql_query,
                        ))
                    }
                    datafusion::logical_plan::GroupingSet::Cube(exprs) => {
                        let mut sql_exprs = Vec::new();
                        for expr in exprs {
                            let (sql, query) = Self::generate_sql_for_expr(
                                plan.clone(),
                                sql_query,
                                sql_generator.clone(),
                                expr,
                                ungrouped_scan_node.clone(),
                                subqueries.clone(),
                            )
                            .await?;
                            sql_query = query;
                            sql_exprs.push(sql);
                        }
                        Ok((
                            sql_generator
                                .get_sql_templates()
                                .cube_expr(sql_exprs)
                                .map_err(|e| {
                                    DataFusionError::Internal(format!(
                                        "Can't generate SQL for rollup expression: {}",
                                        e
                                    ))
                                })?,
                            sql_query,
                        ))
                    }
                    datafusion::logical_plan::GroupingSet::GroupingSets(_) => {
                        Err(DataFusionError::Internal(format!(
                            "SQL generation for GroupingSet is not supported"
                        )))
                    }
                },

                Expr::WindowFunction {
                    fun,
                    args,
                    partition_by,
                    order_by,
                    window_frame,
                } => {
                    let mut sql_args = Vec::new();
                    for arg in args {
                        let (sql, query) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            arg,
                            ungrouped_scan_node.clone(),
                            subqueries.clone(),
                        )
                        .await?;
                        sql_query = query;
                        sql_args.push(sql);
                    }
                    let mut sql_partition_by = Vec::new();
                    for arg in partition_by {
                        let (sql, query) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            arg,
                            ungrouped_scan_node.clone(),
                            subqueries.clone(),
                        )
                        .await?;
                        sql_query = query;
                        sql_partition_by.push(sql);
                    }
                    let mut sql_order_by = Vec::new();
                    for arg in order_by {
                        let (sql, query) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            arg,
                            ungrouped_scan_node.clone(),
                            subqueries.clone(),
                        )
                        .await?;
                        sql_query = query;
                        sql_order_by.push(sql);
                    }
                    let resulting_sql = sql_generator
                        .get_sql_templates()
                        .window_function_expr(
                            fun,
                            sql_args,
                            sql_partition_by,
                            sql_order_by,
                            window_frame,
                        )
                        .map_err(|e| {
                            DataFusionError::Internal(format!(
                                "Can't generate SQL for window function: {}",
                                e
                            ))
                        })?;
                    Ok((resulting_sql, sql_query))
                }
                // Expr::AggregateUDF { .. } => {}
                Expr::InList {
                    expr,
                    list,
                    negated,
                } => {
                    let mut sql_query = sql_query;
                    let (sql_expr, query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        ungrouped_scan_node.clone(),
                        subqueries.clone(),
                    )
                    .await?;
                    sql_query = query;
                    let mut sql_in_exprs = Vec::new();
                    for expr in list {
                        let (sql, query) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            expr,
                            ungrouped_scan_node.clone(),
                            subqueries.clone(),
                        )
                        .await?;
                        sql_query = query;
                        sql_in_exprs.push(sql);
                    }
                    Ok((
                        sql_generator
                            .get_sql_templates()
                            .in_list_expr(sql_expr, sql_in_exprs, negated)
                            .map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Can't generate SQL for in list expr: {}",
                                    e
                                ))
                            })?,
                        sql_query,
                    ))
                }
                Expr::InSubquery {
                    expr,
                    subquery,
                    negated,
                } => {
                    let mut sql_query = sql_query;
                    let (sql_expr, query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        ungrouped_scan_node.clone(),
                        subqueries.clone(),
                    )
                    .await?;
                    sql_query = query;
                    let (subquery_sql, query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *subquery,
                        ungrouped_scan_node.clone(),
                        subqueries.clone(),
                    )
                    .await?;
                    sql_query = query;

                    Ok((
                        sql_generator
                            .get_sql_templates()
                            .in_subquery_expr(sql_expr, subquery_sql, negated)
                            .map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Can't generate SQL for in subquery expr: {}",
                                    e
                                ))
                            })?,
                        sql_query,
                    ))
                }
                // Expr::Wildcard => {}
                // Expr::QualifiedWildcard { .. } => {}
                x => {
                    return Err(DataFusionError::Internal(format!(
                        "SQL generation for expression is not supported: {:?}",
                        x
                    )))
                }
            }
        })
    }

    fn escape_interpolation_quotes(s: String, ungrouped: bool) -> String {
        if ungrouped {
            s.replace("\\", "\\\\").replace("`", "\\`")
        } else {
            s
        }
    }
}

impl UserDefinedLogicalNode for CubeScanWrapperNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.wrapped_plan.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // TODO figure out nice plan for wrapped plan
        write!(f, "CubeScanWrapper")
    }

    fn from_template(
        &self,
        exprs: &[datafusion::logical_plan::Expr],
        inputs: &[datafusion::logical_plan::LogicalPlan],
    ) -> std::sync::Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        assert_eq!(inputs.len(), 0, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");

        Arc::new(CubeScanWrapperNode {
            wrapped_plan: self.wrapped_plan.clone(),
            meta: self.meta.clone(),
            auth_context: self.auth_context.clone(),
            wrapped_sql: self.wrapped_sql.clone(),
            request: self.request.clone(),
            member_fields: self.member_fields.clone(),
            span_id: self.span_id.clone(),
            config_obj: self.config_obj.clone(),
        })
    }
}
