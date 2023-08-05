use crate::{
    compile::engine::df::scan::{CubeScanNode, MemberField, WrappedSelectNode},
    sql::AuthContextRef,
    transport::{AliasedColumn, LoadRequestMeta, MetaContext, SqlGenerator, TransportService},
    CubeError,
};
use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{
        plan::Extension, DFSchema, DFSchemaRef, Expr, LogicalPlan, UserDefinedLogicalNode,
    },
    physical_plan::aggregates::AggregateFunction,
};
use itertools::Itertools;
use serde_derive::*;
use std::{any::Any, fmt, future::Future, pin::Pin, result, sync::Arc};

#[derive(Debug, Clone, Deserialize)]
pub struct SqlQuery {
    pub sql: String,
    pub values: Vec<String>,
}

impl SqlQuery {
    pub fn new(sql: String, values: Vec<String>) -> Self {
        Self { sql, values }
    }

    pub fn add_value(&mut self, value: String) -> usize {
        self.values.push(value);
        self.values.len()
    }

    pub fn replace_sql(&mut self, sql: String) {
        self.sql = sql;
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
        .map(|(_, _, sql)| sql)?;
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
                                projection.push(AliasedColumn {
                                    expr: Self::generate_sql_for_expr(
                                        plan.clone(),
                                        generator.clone(),
                                        expr.clone(),
                                    )
                                    .await?,
                                    alias: expr_name(&expr, &schema)?,
                                });
                            }
                            for expr in group_expr {
                                group_by.push(AliasedColumn {
                                    expr: Self::generate_sql_for_expr(
                                        plan.clone(),
                                        generator.clone(),
                                        expr.clone(),
                                    )
                                    .await?,
                                    alias: expr_name(&expr, &schema)?,
                                });
                            }
                            let mut aggregate = Vec::new();
                            for expr in aggr_expr {
                                aggregate.push(AliasedColumn {
                                    expr: Self::generate_sql_for_expr(
                                        plan.clone(),
                                        generator.clone(),
                                        expr.clone(),
                                    )
                                    .await?,
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
        sql_generator: Arc<dyn SqlGenerator>,
        expr: Expr,
    ) -> Pin<Box<dyn Future<Output = Result<String>> + Send>> {
        Box::pin(async move {
            match expr {
                Expr::Alias(expr, _) => {
                    let expr = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_generator.clone(),
                        (*expr).clone(),
                    )
                    .await?;
                    Ok(expr)
                }
                // Expr::OuterColumn(_, _) => {}
                Expr::Column(c) => Ok(match c.relation.as_ref() {
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
                }),
                // Expr::ScalarVariable(_, _) => {}
                // Expr::Literal(_) => {}
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
                // Expr::ScalarFunction { .. } => {}
                // Expr::ScalarUDF { .. } => {}
                // Expr::TableUDF { .. } => {}
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
                        sql_args.push(
                            Self::generate_sql_for_expr(plan.clone(), sql_generator.clone(), arg)
                                .await?,
                        );
                    }
                    Ok(sql_generator
                        .get_sql_templates()
                        .aggregate_function(fun, sql_args, distinct)
                        .map_err(|e| {
                            DataFusionError::Internal(format!(
                                "Can't generate SQL for aggregate function: {}",
                                e
                            ))
                        })?)
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
