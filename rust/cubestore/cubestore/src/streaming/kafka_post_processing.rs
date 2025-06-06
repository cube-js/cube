use crate::metastore::Column;
use crate::queryplanner::metadata_cache::MetadataCacheFactory;
use crate::queryplanner::pretty_printers::{pp_plan_ext, PPOptions};
use crate::queryplanner::{sql_to_rel_options, try_make_memory_data_source, QueryPlannerImpl};
use crate::sql::MySqlDialectWithBackTicks;
use crate::streaming::topic_table_provider::TopicTableProvider;
use crate::CubeError;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr::{Alias, ScalarFunction};
use datafusion::logical_expr::{
    projection_schema, Expr, Filter, LogicalPlan, Projection, SubqueryAlias,
};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::planner::SqlToRel;
use sqlparser::ast::{Expr as SQExpr, FunctionArgExpr, FunctionArgumentList, FunctionArguments};
use sqlparser::ast::{FunctionArg, Ident, ObjectName, Query, SelectItem, SetExpr, Statement};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Span, Tokenizer};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct KafkaPostProcessPlan {
    metadata_cache_factory: Arc<dyn MetadataCacheFactory>,
    projection_plan: Arc<dyn ExecutionPlan>,
    filter_plan: Option<Arc<dyn ExecutionPlan>>,
    source_columns: Vec<Column>,
    source_unique_columns: Vec<Column>,
    source_seq_column_index: usize,
    source_schema: SchemaRef,
}

impl KafkaPostProcessPlan {
    pub fn new(
        projection_plan: Arc<dyn ExecutionPlan>,
        filter_plan: Option<Arc<dyn ExecutionPlan>>,
        source_columns: Vec<Column>,
        source_unique_columns: Vec<Column>,
        source_seq_column_index: usize,
        metadata_cache_factory: Arc<dyn MetadataCacheFactory>,
    ) -> Self {
        let source_schema = Arc::new(Schema::new(
            source_columns
                .iter()
                .map(|c| c.clone().into())
                .collect::<Vec<Field>>(),
        ));
        Self {
            projection_plan,
            filter_plan,
            source_columns,
            source_unique_columns,
            source_seq_column_index,
            source_schema,
            metadata_cache_factory,
        }
    }

    pub fn source_columns(&self) -> &Vec<Column> {
        &self.source_columns
    }

    pub fn source_seq_column_index(&self) -> usize {
        self.source_seq_column_index
    }

    pub fn source_unique_columns(&self) -> &Vec<Column> {
        &self.source_unique_columns
    }

    pub async fn apply(&self, data: Vec<ArrayRef>) -> Result<Vec<ArrayRef>, CubeError> {
        let batch = RecordBatch::try_new(self.source_schema.clone(), data)?;
        let input = try_make_memory_data_source(&[vec![batch]], self.source_schema.clone(), None)?;
        let filter_input = if let Some(filter_plan) = &self.filter_plan {
            filter_plan.clone().with_new_children(vec![input])?
        } else {
            input
        };

        let projection = self
            .projection_plan
            .clone()
            .with_new_children(vec![filter_input])?;

        let task_context = QueryPlannerImpl::make_execution_context(
            self.metadata_cache_factory.make_session_config(),
        )
        .task_ctx();

        let projection_schema: Arc<Schema> = projection.schema();
        let mut out_batches = collect(projection, task_context).await?;
        let res = if out_batches.len() == 1 {
            out_batches.pop().unwrap()
        } else {
            concat_batches(&projection_schema, &out_batches)?
        };

        Ok(res.columns().to_vec())
    }
}

pub struct KafkaPostProcessPlanner {
    topic: String,
    unique_key_columns: Vec<Column>,
    seq_column: Column,
    columns: Vec<Column>,
    source_columns: Vec<Column>,
    metadata_cache_factory: Arc<dyn MetadataCacheFactory>,
}

impl KafkaPostProcessPlanner {
    pub fn new(
        topic: String,
        unique_key_columns: Vec<Column>,
        seq_column: Column,
        columns: Vec<Column>,
        source_columns: Option<Vec<Column>>,
        metadata_cache_factory: Arc<dyn MetadataCacheFactory>,
    ) -> Self {
        let mut source_columns = source_columns.map_or_else(|| columns.clone(), |c| c);

        if !source_columns
            .iter()
            .any(|c| c.get_name() == seq_column.get_name())
        {
            source_columns.push(seq_column.replace_index(source_columns.len()));
        }

        Self {
            topic,
            unique_key_columns,
            seq_column,
            columns,
            source_columns,
            metadata_cache_factory,
        }
    }

    /// Compares schemas for equality, including metadata, except that physical_schema is allowed to
    /// have non-nullable versions of the target schema's field.  This function is defined this way
    /// (instead of some perhaps more generalizable way) because it conservatively replaces an
    /// equality comparison.
    fn is_compatible_schema(target_schema: &Schema, physical_schema: &Schema) -> bool {
        if target_schema.metadata != physical_schema.metadata
            || target_schema.fields.len() != physical_schema.fields.len()
        {
            return false;
        }
        for (target_field, physical_field) in target_schema
            .fields
            .iter()
            .zip(physical_schema.fields.iter())
        {
            // See the >= there on is_nullable.
            if !(target_field.name() == physical_field.name()
                && target_field.data_type() == physical_field.data_type()
                && target_field.is_nullable() >= physical_field.is_nullable()
                && target_field.metadata() == physical_field.metadata())
            {
                return false;
            }
        }
        return true;
    }

    pub async fn build(
        &self,
        select_statement: String,
        metadata_cache_factory: Arc<dyn MetadataCacheFactory>,
    ) -> Result<KafkaPostProcessPlan, CubeError> {
        let target_schema = Arc::new(Schema::new(
            self.columns
                .iter()
                .map(|c| c.clone().into())
                .collect::<Vec<Field>>(),
        ));
        let logical_plan: LogicalPlan = self.make_logical_plan(&select_statement)?;
        // Here we want to expand wildcards for extract_source_unique_columns.  Also, we run the
        // entire Analyzer pass, because make_projection_and_filter_physical_plans specifically
        // skips the Analyzer pass and LogicalPlan optimization steps performed by
        // SessionState::create_physical_plan.
        let logical_plan: LogicalPlan = datafusion::optimizer::Analyzer::new().execute_and_check(
            logical_plan,
            &ConfigOptions::default(),
            |_, _| {},
        )?;
        let source_unique_columns = self.extract_source_unique_columns(&logical_plan)?;

        let (projection_plan, filter_plan) = self
            .make_projection_and_filter_physical_plans(&logical_plan)
            .await?;
        if !Self::is_compatible_schema(target_schema.as_ref(), projection_plan.schema().as_ref()) {
            return Err(CubeError::user(format!(
                "Table schema: {:?} don't match select_statement result schema: {:?}",
                target_schema,
                projection_plan.schema()
            )));
        }
        let source_seq_column_index = self
            .source_columns
            .iter()
            .find(|c| c.get_name() == self.seq_column.get_name())
            .unwrap()
            .get_index();
        Ok(KafkaPostProcessPlan::new(
            projection_plan,
            filter_plan,
            self.source_columns.clone(),
            source_unique_columns,
            source_seq_column_index,
            metadata_cache_factory,
        ))
    }

    fn make_logical_plan(&self, select_statement: &str) -> Result<LogicalPlan, CubeError> {
        let dialect = &MySqlDialectWithBackTicks {};
        let mut tokenizer = Tokenizer::new(dialect, &select_statement);
        let tokens = tokenizer.tokenize().unwrap();
        let statement = Parser::new(dialect).with_tokens(tokens).parse_statement()?;
        let statement = self.rewrite_statement(statement);

        match &statement {
            Statement::Query(box Query {
                body: box SetExpr::Select(_),
                ..
            }) => {
                let provider = TopicTableProvider::new(self.topic.clone(), &self.source_columns);
                let query_planner = SqlToRel::new_with_options(&provider, sql_to_rel_options());
                let logical_plan = query_planner
                    .statement_to_plan(DFStatement::Statement(Box::new(statement.clone())))?;
                Ok(logical_plan)
            }
            _ => Err(CubeError::user(format!(
                "{} is not valid select query",
                select_statement
            ))),
        }
    }

    fn rewrite_statement(&self, statement: Statement) -> Statement {
        match statement {
            Statement::Query(box Query {
                body: box SetExpr::Select(mut s),
                with,
                order_by,
                limit,
                limit_by,
                offset,
                fetch,
                locks,
                for_clause,
                settings,
                format_clause,
            }) => {
                s.projection = s
                    .projection
                    .into_iter()
                    .map(|itm| match itm {
                        SelectItem::UnnamedExpr(e) => SelectItem::UnnamedExpr(self.rewrite_expr(e)),
                        SelectItem::ExprWithAlias { expr, alias } => SelectItem::ExprWithAlias {
                            expr: self.rewrite_expr(expr),
                            alias,
                        },
                        _ => itm,
                    })
                    .collect::<Vec<_>>();
                s.selection = s.selection.map(|e| self.rewrite_expr(e));
                //let select =
                Statement::Query(Box::new(Query {
                    with,
                    body: Box::new(SetExpr::Select(s)),
                    order_by,
                    limit,
                    limit_by,
                    offset,
                    fetch,
                    locks,
                    for_clause,
                    settings,
                    format_clause,
                }))
            }
            _ => statement,
        }
    }

    fn rewrite_expr(&self, expr: SQExpr) -> SQExpr {
        match expr {
            SQExpr::IsNull(e) => SQExpr::IsNull(Box::new(self.rewrite_expr(*e))),
            SQExpr::IsNotNull(e) => SQExpr::IsNotNull(Box::new(self.rewrite_expr(*e))),
            SQExpr::InList {
                expr,
                list,
                negated,
            } => SQExpr::InList {
                expr: Box::new(self.rewrite_expr(*expr)),
                list,
                negated,
            },
            SQExpr::Between {
                expr,
                negated,
                low,
                high,
            } => SQExpr::Between {
                expr: Box::new(self.rewrite_expr(*expr)),
                negated,
                low: Box::new(self.rewrite_expr(*low)),
                high: Box::new(self.rewrite_expr(*high)),
            },
            SQExpr::BinaryOp { left, op, right } => SQExpr::BinaryOp {
                left: Box::new(self.rewrite_expr(*left)),
                op,
                right: Box::new(self.rewrite_expr(*right)),
            },
            SQExpr::UnaryOp { op, expr } => SQExpr::UnaryOp {
                op,
                expr: Box::new(self.rewrite_expr(*expr)),
            },
            SQExpr::Cast {
                kind,
                expr,
                data_type,
                format,
            } => SQExpr::Cast {
                kind,
                expr: Box::new(self.rewrite_expr(*expr)),
                data_type,
                format,
            },
            SQExpr::Extract {
                field,
                syntax,
                expr,
            } => SQExpr::Extract {
                field,
                syntax,
                expr: Box::new(self.rewrite_expr(*expr)),
            },
            SQExpr::Substring {
                expr,
                substring_from,
                substring_for,
                special,
            } => SQExpr::Substring {
                expr: Box::new(self.rewrite_expr(*expr)),
                substring_from,
                substring_for,
                special,
            },
            SQExpr::Nested(e) => SQExpr::Nested(Box::new(self.rewrite_expr(*e))),
            SQExpr::Function(mut f) => {
                f.name = if f.name.0.len() == 1 && f.name.0[0].value.to_lowercase() == "convert_tz"
                {
                    ObjectName(vec![Ident {
                        value: "CONVERT_TZ_KSQL".to_string(),
                        quote_style: None,
                        span: Span::empty(),
                    }])
                } else {
                    f.name
                };
                f.args = match f.args {
                    FunctionArguments::None => FunctionArguments::None,
                    FunctionArguments::Subquery(s) => FunctionArguments::Subquery(s),
                    FunctionArguments::List(list) => {
                        FunctionArguments::List(FunctionArgumentList {
                            duplicate_treatment: list.duplicate_treatment,
                            args: list
                                .args
                                .into_iter()
                                .map(|a| match a {
                                    FunctionArg::Named {
                                        name,
                                        arg: FunctionArgExpr::Expr(e_arg),
                                        operator,
                                    } => FunctionArg::Named {
                                        name,
                                        arg: FunctionArgExpr::Expr(self.rewrite_expr(e_arg)),
                                        operator,
                                    },
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e_arg)) => {
                                        FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                            self.rewrite_expr(e_arg),
                                        ))
                                    }
                                    arg => arg,
                                })
                                .collect::<Vec<_>>(),
                            clauses: list.clauses,
                        })
                    }
                };
                SQExpr::Function(f)
            }
            SQExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let operand = operand.map(|o| Box::new(self.rewrite_expr(*o)));
                let conditions = conditions
                    .into_iter()
                    .map(|o| self.rewrite_expr(o))
                    .collect::<Vec<_>>();
                let results = results
                    .into_iter()
                    .map(|o| self.rewrite_expr(o))
                    .collect::<Vec<_>>();
                let else_result = else_result.map(|o| Box::new(self.rewrite_expr(*o)));

                SQExpr::Case {
                    operand,
                    conditions,
                    results,
                    else_result,
                }
            }
            _ => expr,
        }
    }

    fn extract_source_unique_columns(&self, plan: &LogicalPlan) -> Result<Vec<Column>, CubeError> {
        match plan {
            LogicalPlan::Projection(Projection { expr, .. }) => {
                let mut source_unique_columns = vec![];
                for e in expr.iter() {
                    let col_name = self.col_name_from_expr(e)?;
                    let is_unique_key_column = self
                        .unique_key_columns
                        .iter()
                        .any(|c| c.get_name() == &col_name);
                    if is_unique_key_column {
                        source_unique_columns.push(self.get_source_unique_column(e)?);
                    }
                }
                Ok(source_unique_columns)
            }
            _ => Ok(vec![]),
        }
    }

    /// Only Projection > [Filter] > TableScan plans are allowed
    async fn make_projection_and_filter_physical_plans(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(Arc<dyn ExecutionPlan>, Option<Arc<dyn ExecutionPlan>>), CubeError> {
        fn only_certain_plans_allowed_error(plan: &LogicalPlan) -> CubeError {
            CubeError::user(
                format!("Only Projection > [Filter] > TableScan plans are allowed for streaming; got plan {}", pp_plan_ext(plan, &PPOptions::show_all())),
            )
        }
        fn remove_subquery_alias_around_table_scan(plan: &LogicalPlan) -> &LogicalPlan {
            if let LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) = plan {
                if matches!(input.as_ref(), LogicalPlan::TableScan { .. }) {
                    return input.as_ref();
                }
            }
            return plan;
        }

        let source_schema = Arc::new(Schema::new(
            self.source_columns
                .iter()
                .map(|c| c.clone().into())
                .collect::<Vec<Field>>(),
        ));
        let empty_exec = Arc::new(EmptyExec::new(source_schema));
        match plan {
            LogicalPlan::Projection(Projection {
                input: projection_input,
                expr,
                schema,
                ..
            }) => match remove_subquery_alias_around_table_scan(projection_input.as_ref()) {
                filter_plan @ LogicalPlan::Filter(Filter { input, .. }) => {
                    match remove_subquery_alias_around_table_scan(input.as_ref()) {
                        LogicalPlan::TableScan { .. } => {
                            let projection_plan = self.make_projection_plan(
                                expr,
                                schema.clone(),
                                projection_input.clone(),
                            )?;

                            let plan_ctx = QueryPlannerImpl::make_execution_context(
                                self.metadata_cache_factory.make_session_config(),
                            );
                            let state = plan_ctx.state().with_physical_optimizer_rules(vec![]);

                            let projection_phys_plan_without_new_children = state
                                .query_planner()
                                .create_physical_plan(&projection_plan, &state)
                                .await?;
                            let projection_phys_plan = projection_phys_plan_without_new_children
                                .with_new_children(vec![empty_exec.clone()])?;

                            let filter_phys_plan = state
                                .query_planner()
                                .create_physical_plan(&filter_plan, &state)
                                .await?
                                .with_new_children(vec![empty_exec.clone()])?;

                            Ok((projection_phys_plan.clone(), Some(filter_phys_plan)))
                        }
                        _ => Err(only_certain_plans_allowed_error(plan)),
                    }
                }
                LogicalPlan::TableScan { .. } => {
                    let projection_plan =
                        self.make_projection_plan(expr, schema.clone(), projection_input.clone())?;

                    let plan_ctx = QueryPlannerImpl::make_execution_context(
                        self.metadata_cache_factory.make_session_config(),
                    );
                    let state = plan_ctx.state().with_physical_optimizer_rules(vec![]);

                    let projection_phys_plan = state
                        .query_planner()
                        .create_physical_plan(&projection_plan, &state)
                        .await?
                        .with_new_children(vec![empty_exec.clone()])?;
                    Ok((projection_phys_plan, None))
                }
                _ => Err(only_certain_plans_allowed_error(plan)),
            },
            _ => Err(only_certain_plans_allowed_error(plan)),
        }
    }

    fn make_projection_plan(
        &self,
        exprs: &Vec<Expr>,
        schema: DFSchemaRef,
        input: Arc<LogicalPlan>,
    ) -> Result<LogicalPlan, CubeError> {
        let mut need_add_seq_col = true;
        let mut res = vec![];
        for expr in exprs.iter() {
            let col_name = self.col_name_from_expr(expr)?;
            if &col_name == self.seq_column.get_name() {
                need_add_seq_col = false;
            }
            res.push(expr.clone());
        }

        let result_schema = if need_add_seq_col {
            res.push(Expr::Column(common::Column::from_name(
                self.seq_column.get_name(),
            )));
            Arc::new(schema.join(&DFSchema::new_with_metadata(
                vec![(
                    None,
                    Arc::new(Field::new(
                        self.seq_column.get_name(),
                        datafusion::arrow::datatypes::DataType::Int64,
                        true,
                    )),
                )],
                HashMap::new(),
            )?)?)
        } else {
            schema.clone()
        };

        Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
            res,
            input,
            result_schema,
        )?))
    }

    fn col_name_from_expr(&self, expr: &Expr) -> Result<String, CubeError> {
        match expr {
            Expr::Column(c) => Ok(c.name.clone()),
            Expr::Alias(Alias { name, .. }) => Ok(name.clone()),
            _ => Err(CubeError::user(format!(
                "All expressions must have aliases in kafka streaming queries, expression is {:?}",
                expr
            ))),
        }
    }

    fn get_source_unique_column(&self, expr: &Expr) -> Result<Column, CubeError> {
        fn find_column_name(expr: &Expr) -> Result<Option<String>, CubeError> {
            match expr {
                Expr::Column(c) => Ok(Some(c.name.clone())),
                Expr::Alias(Alias {
                    expr: e,
                    relation: _,
                    name: _,
                }) => find_column_name(&**e),
                Expr::ScalarFunction(ScalarFunction { func: _, args }) => {
                    let mut column_name: Option<String> = None;
                    for arg in args {
                        if let Some(name) = find_column_name(arg)? {
                            if let Some(existing_name) = &column_name {
                                if existing_name != &name {
                                    return Err(CubeError::user(
                                        format!("Scalar function can only use a single column, expression: {:?}", expr),
                                    ));
                                }
                            } else {
                                column_name = Some(name);
                            }
                        }
                    }
                    Ok(column_name)
                }
                _ => Ok(None),
            }
        }

        let source_name = match expr {
            Expr::Column(c) => Ok(c.name.clone()),
            Expr::Alias(Alias { expr, .. }) => match &**expr {
                Expr::Column(c) => Ok(c.name.clone()),
                Expr::ScalarFunction(_) => find_column_name(expr)?.ok_or_else(|| {
                    CubeError::user(format!("Scalar function must contain at least one column, expression: {:?}", expr))
                }),
                _ => Err(CubeError::user(format!(
                    "Unique key can't be an expression in kafka streaming queries, expression: {:?}",
                    expr
                ))),
            },
            _ => Err(CubeError::user(
                format!("All expressions must have aliases in kafka streaming queries, expression: {:?}", expr),
            )),
        }?;

        self.source_columns
            .iter()
            .find(|c| c.get_name() == &source_name)
            .ok_or_else(|| {
                CubeError::user(format!("Column {} not found in source table", source_name))
            })
            .map(|c| c.clone())
    }
}
