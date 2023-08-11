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
        plan::Extension, DFSchema, DFSchemaRef, Expr, LogicalPlan, UserDefinedLogicalNode,
    },
    physical_plan::aggregates::AggregateFunction,
    scalar::ScalarValue,
};
use itertools::Itertools;
use regex::{Captures, Regex};
use serde_derive::*;
use std::{any::Any, fmt, future::Future, pin::Pin, result, sync::Arc};

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
        )
        .await
        .and_then(|(data_source, _, mut sql): (Option<String>, _, SqlQuery)| -> result::Result<_, CubeError> {
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
    ) -> Pin<
        Box<
            dyn Future<
                    Output = result::Result<(Option<String>, Option<String>, SqlQuery), CubeError>,
                > + Send,
        >,
    > {
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
                        return Ok((
                            Some(data_sources[0].clone()),
                            // TODO Implement more straightforward way to get alias name
                            node.schema
                                .fields()
                                .iter()
                                .next()
                                .and_then(|f| f.qualifier().cloned()),
                            sql.sql,
                        ));
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
                        limit: _limit,
                        offset: _offset,
                        order_expr: _order_expr,
                        alias,
                    }) = wrapped_select_node
                    {
                        // TODO support joins
                        let (data_source, from_alias, mut sql) = Self::generate_sql_for_node(
                            plan.clone(),
                            transport.clone(),
                            load_request_meta.clone(),
                            from.clone(),
                        )
                        .await?;
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
                            let mut group_by = Vec::new();
                            let mut projection = Vec::new();
                            for expr in projection_expr {
                                let (expr_sql, new_sql_query) = Self::generate_sql_for_expr(
                                    plan.clone(),
                                    sql,
                                    generator.clone(),
                                    expr.clone(),
                                )
                                .await?;
                                sql = new_sql_query;
                                projection.push(AliasedColumn {
                                    expr: expr_sql,
                                    alias: expr_name(&expr, &schema)?,
                                });
                            }
                            for expr in group_expr {
                                let (expr_sql, new_sql_query) = Self::generate_sql_for_expr(
                                    plan.clone(),
                                    sql,
                                    generator.clone(),
                                    expr.clone(),
                                )
                                .await?;
                                sql = new_sql_query;
                                group_by.push(AliasedColumn {
                                    expr: expr_sql,
                                    alias: expr_name(&expr, &schema)?,
                                });
                            }
                            let mut aggregate = Vec::new();
                            for expr in aggr_expr {
                                let (expr_sql, new_sql_query) = Self::generate_sql_for_expr(
                                    plan.clone(),
                                    sql,
                                    generator.clone(),
                                    expr.clone(),
                                )
                                .await?;
                                sql = new_sql_query;
                                aggregate.push(AliasedColumn {
                                    expr: expr_sql,
                                    alias: expr_name(&expr, &schema)?,
                                });
                            }
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
                                )
                                .map_err(|e| {
                                    DataFusionError::Internal(format!(
                                        "Can't generate SQL for wrapped select: {}",
                                        e
                                    ))
                                })?;
                            sql.replace_sql(resulting_sql.clone());
                            Ok((Some(data_source), alias, sql))
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
                        (*expr).clone(),
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
                // Expr::BinaryExpr { .. } => {}
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
                // Expr::Case { .. } => {}
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
