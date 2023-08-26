use crate::{
    compile::engine::df::scan::{CubeScanNode, MemberField, WrappedSelectNode},
    sql::AuthContextRef,
    transport::{
        AliasedColumn, LoadRequestMeta, MetaContext, SqlGenerator, SqlTemplates, TransportService,
    },
    CubeError,
};
use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{
        plan::Extension, replace_col, Column, DFSchema, DFSchemaRef, Expr, LogicalPlan,
        UserDefinedLogicalNode,
    },
    physical_plan::aggregates::AggregateFunction,
    scalar::ScalarValue,
};
use itertools::Itertools;
use regex::{Captures, Regex};
use serde_derive::*;
use std::{any::Any, collections::HashMap, fmt, future::Future, pin::Pin, result, sync::Arc};

#[derive(Debug, Clone, Deserialize)]
pub struct SqlQuery {
    pub sql: String,
    pub values: Vec<Option<String>>,
}

impl SqlQuery {
    pub fn new(sql: String, values: Vec<Option<String>>) -> Self {
        Self { sql, values }
    }

    pub fn add_value(&mut self, value: Option<String>) -> usize {
        let index = self.values.len();
        self.values.push(value);
        index
    }

    pub fn replace_sql(&mut self, sql: String) {
        self.sql = sql;
    }

    fn render_param(
        &self,
        sql_templates: Arc<SqlTemplates>,
        param_index: Option<&str>,
        new_param_index: usize,
    ) -> Result<(usize, String)> {
        let param = param_index
            .ok_or_else(|| DataFusionError::Execution("Missing param match".to_string()))?
            .parse::<usize>()
            .map_err(|e| DataFusionError::Execution(format!("Can't parse param index: {}", e)))?;
        Ok((
            param,
            sql_templates
                .param(new_param_index)
                .map_err(|e| DataFusionError::Execution(format!("Can't render param: {}", e)))?,
        ))
    }

    pub fn finalize_query(&mut self, sql_templates: Arc<SqlTemplates>) -> Result<()> {
        let mut params = Vec::new();
        let regex = Regex::new(r"\$(\d+)\$")
            .map_err(|e| DataFusionError::Execution(format!("Can't parse regex: {}", e)))?;
        let mut res = Ok(());
        let replaced_sql = regex.replace_all(self.sql.as_str(), |c: &Captures<'_>| {
            let param = c.get(1).map(|x| x.as_str());
            match self.render_param(sql_templates.clone(), param, params.len()) {
                Ok((param_index, param)) => {
                    params.push(self.values[param_index].clone());
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
}

impl CubeScanWrapperNode {
    pub fn new(
        wrapped_plan: Arc<LogicalPlan>,
        meta: Arc<MetaContext>,
        auth_context: AuthContextRef,
    ) -> Self {
        Self {
            wrapped_plan,
            meta,
            auth_context,
            wrapped_sql: None,
        }
    }

    pub fn with_sql(&self, sql: SqlQuery) -> Self {
        Self {
            wrapped_plan: self.wrapped_plan.clone(),
            meta: self.meta.clone(),
            auth_context: self.auth_context.clone(),
            wrapped_sql: Some(sql),
        }
    }
}

fn expr_name(e: &Expr, schema: &Arc<DFSchema>) -> Result<String> {
    match e {
        Expr::Column(col) => Ok(col.name.clone()),
        _ => e.name(schema),
    }
}

pub struct SqlGenerationResult {
    pub data_source: Option<String>,
    pub from_alias: Option<String>,
    pub column_remapping: Option<HashMap<Column, Column>>,
    pub sql: SqlQuery,
}

impl CubeScanWrapperNode {
    pub async fn generate_sql(
        &self,
        transport: Arc<dyn TransportService>,
        load_request_meta: Arc<LoadRequestMeta>,
    ) -> result::Result<Self, CubeError> {
        let sql = Self::generate_sql_for_node(
            Arc::new(self.clone()),
            transport,
            load_request_meta,
            self.wrapped_plan.clone(),
            false,
        )
        .await
        .and_then(|SqlGenerationResult { data_source, mut sql, .. }| -> result::Result<_, CubeError> {
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
            Ok(sql)
        })?;
        Ok(self.with_sql(sql))
    }

    pub fn generate_sql_for_node(
        plan: Arc<Self>,
        transport: Arc<dyn TransportService>,
        load_request_meta: Arc<LoadRequestMeta>,
        node: Arc<LogicalPlan>,
        can_rename_columns: bool,
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
                                node.request,
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
                        });
                    } else if let Some(WrappedSelectNode {
                        schema,
                        select_type: _select_type,
                        projection_expr,
                        group_expr,
                        aggr_expr,
                        from,
                        joins: _joins,
                        filter_expr: _filter_expr,
                        having_expr: _having_expr,
                        limit,
                        offset,
                        order_expr: _order_expr,
                        alias,
                    }) = wrapped_select_node
                    {
                        // TODO support joins
                        let SqlGenerationResult {
                            data_source,
                            from_alias,
                            column_remapping,
                            sql,
                        } = Self::generate_sql_for_node(
                            plan.clone(),
                            transport.clone(),
                            load_request_meta.clone(),
                            from.clone(),
                            true,
                        )
                        .await?;
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
                                projection_expr,
                                sql,
                                generator.clone(),
                                &column_remapping,
                                &mut next_remapping,
                                alias.clone(),
                                can_rename_columns,
                            )
                            .await?;
                            let (group_by, sql) = Self::generate_column_expr(
                                plan.clone(),
                                schema.clone(),
                                group_expr,
                                sql,
                                generator.clone(),
                                &column_remapping,
                                &mut next_remapping,
                                alias.clone(),
                                can_rename_columns,
                            )
                            .await?;
                            let (aggregate, mut sql) = Self::generate_column_expr(
                                plan.clone(),
                                schema.clone(),
                                aggr_expr,
                                sql,
                                generator.clone(),
                                &column_remapping,
                                &mut next_remapping,
                                alias.clone(),
                                can_rename_columns,
                            )
                            .await?;
                            let resulting_sql = generator
                                .get_sql_templates()
                                .select(
                                    sql.sql.to_string(),
                                    projection,
                                    group_by,
                                    aggregate,
                                    // TODO
                                    from_alias.unwrap_or("".to_string()),
                                    None,
                                    None,
                                    Vec::new(),
                                    limit,
                                    offset,
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
                            })
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
            let (expr_sql, new_sql_query) =
                Self::generate_sql_for_expr(plan.clone(), sql, generator.clone(), expr.clone())
                    .await?;
            sql = new_sql_query;

            let original_alias = expr_name(&original_expr, &schema)?;
            let alias = if can_rename_columns {
                let alias = expr_name(&expr, &schema)?;
                let mut truncated_alias = non_id_regex.replace_all(&alias, "_").to_string();
                truncated_alias.truncate(16);
                let mut alias = truncated_alias.clone();
                for i in 1..10000 {
                    if !next_remapping.contains_key(&Column::from_name(&alias)) {
                        break;
                    }
                    alias = format!("{}_{}", truncated_alias, i);
                }
                alias
            } else {
                original_alias.clone()
            };
            if original_alias != alias {
                if !next_remapping.contains_key(&Column::from_name(&alias)) {
                    next_remapping.insert(
                        Column::from_name(&original_alias),
                        Column::from_name(&alias),
                    );
                    next_remapping.insert(
                        Column {
                            name: original_alias.clone(),
                            relation: from_alias.clone(),
                        },
                        Column {
                            name: alias.clone(),
                            relation: from_alias.clone(),
                        },
                    );
                } else {
                    return Err(CubeError::internal(format!(
                        "Can't generate SQL for column expr: duplicate alias {}",
                        alias
                    )));
                }
            }

            aliased_columns.push(AliasedColumn {
                expr: expr_sql,
                alias,
            });
        }
        Ok((aliased_columns, sql))
    }

    pub fn generate_sql_for_expr(
        plan: Arc<Self>,
        mut sql_query: SqlQuery,
        sql_generator: Arc<dyn SqlGenerator>,
        expr: Expr,
    ) -> Pin<Box<dyn Future<Output = Result<(String, SqlQuery)>> + Send>> {
        Box::pin(async move {
            match expr {
                Expr::Alias(expr, _) => {
                    let (expr, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                    )
                    .await?;
                    Ok((expr, sql_query))
                }
                // Expr::OuterColumn(_, _) => {}
                Expr::Column(c) => Ok((
                    match c.relation.as_ref() {
                        Some(r) => format!(
                            "{}.{}",
                            r,
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
                )),
                // Expr::ScalarVariable(_, _) => {}
                Expr::BinaryExpr { left, op, right } => {
                    let (left, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *left,
                    )
                    .await?;
                    let (right, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *right,
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
                // Expr::Not(_) => {}
                // Expr::IsNotNull(_) => {}
                // Expr::IsNull(_) => {}
                // Expr::Negative(_) => {}
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
                        )
                        .await?;
                        let (then, sql_query_next) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query_next,
                            sql_generator.clone(),
                            *then,
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
                // Expr::Cast { .. } => {}
                // Expr::TryCast { .. } => {}
                // Expr::Sort { .. } => {}
                // Expr::ScalarUDF { .. } => {}
                // Expr::TableUDF { .. } => {}
                Expr::Literal(literal) => {
                    Ok(match literal {
                        // ScalarValue::Boolean(b) => {}
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
                            let param_index = sql_query.add_value(x);
                            (format!("${}$", param_index), sql_query)
                        }
                        // ScalarValue::LargeUtf8(_) => {}
                        // ScalarValue::Binary(_) => {}
                        // ScalarValue::LargeBinary(_) => {}
                        // ScalarValue::List(_, _) => {}
                        // ScalarValue::Date32(_) => {}
                        // ScalarValue::Date64(_) => {}
                        // ScalarValue::TimestampSecond(_, _) => {}
                        // ScalarValue::TimestampMillisecond(_, _) => {}
                        // ScalarValue::TimestampMicrosecond(_, _) => {}
                        // ScalarValue::TimestampNanosecond(_, _) => {}
                        // ScalarValue::IntervalYearMonth(_) => {}
                        // ScalarValue::IntervalDayTime(_) => {}
                        // ScalarValue::IntervalMonthDayNano(_) => {}
                        // ScalarValue::Struct(_, _) => {}
                        x => {
                            return Err(DataFusionError::Internal(format!(
                                "Can't generate SQL for literal: {:?}",
                                x
                            )));
                        }
                    })
                }
                Expr::ScalarFunction { fun, args } => {
                    let mut sql_args = Vec::new();
                    for arg in args {
                        let (sql, query) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            arg,
                        )
                        .await?;
                        sql_query = query;
                        sql_args.push(sql);
                    }
                    Ok((
                        sql_generator
                            .get_sql_templates()
                            .scalar_function(fun, sql_args)
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
                // Expr::WindowFunction { .. } => {}
                // Expr::AggregateUDF { .. } => {}
                // Expr::InList { .. } => {}
                // Expr::Wildcard => {}
                // Expr::QualifiedWildcard { .. } => {}
                x => {
                    return Err(DataFusionError::Internal(format!(
                        "Can't generate SQL for expr: {:?}",
                        x
                    )))
                }
            }
        })
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
        })
    }
}
