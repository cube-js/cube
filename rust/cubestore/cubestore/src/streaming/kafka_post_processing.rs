use crate::metastore::Column;
use crate::sql::MySqlDialectWithBackTicks;
use crate::streaming::topic_table_provider::TopicTableProvider;
use crate::CubeError;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_plan::{
    Column as DFColumn, DFField, DFSchema, DFSchemaRef, Expr, LogicalPlan,
};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::parquet::MetadataCacheFactory;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::{ExecutionConfig, ExecutionContext};
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::planner::SqlToRel;
use sqlparser::ast::Expr as SQExpr;
use sqlparser::ast::{FunctionArg, Ident, ObjectName, Query, SelectItem, SetExpr, Statement};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;
use std::sync::Arc;

#[derive(Clone)]
pub struct KafkaPostProcessPlan {
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
    ) -> Self {
        let source_schema = Arc::new(Schema::new(
            source_columns
                .iter()
                .map(|c| c.clone().into())
                .collect::<Vec<_>>(),
        ));
        Self {
            projection_plan,
            filter_plan,
            source_columns,
            source_unique_columns,
            source_seq_column_index,
            source_schema,
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
        let input = Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            self.source_schema.clone(),
            None,
        )?);
        let filter_input = if let Some(filter_plan) = &self.filter_plan {
            filter_plan.with_new_children(vec![input])?
        } else {
            input
        };

        let projection = self.projection_plan.with_new_children(vec![filter_input])?;

        let mut out_batches = collect(projection).await?;
        let res = if out_batches.len() == 1 {
            out_batches.pop().unwrap()
        } else {
            RecordBatch::concat(&self.source_schema, &out_batches)?
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
}

impl KafkaPostProcessPlanner {
    pub fn new(
        topic: String,
        unique_key_columns: Vec<Column>,
        seq_column: Column,
        columns: Vec<Column>,
        source_columns: Option<Vec<Column>>,
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
        }
    }

    pub fn build(
        &self,
        select_statement: String,
        metadata_cache_factory: Arc<dyn MetadataCacheFactory>,
    ) -> Result<KafkaPostProcessPlan, CubeError> {
        let target_schema = Arc::new(Schema::new(
            self.columns
                .iter()
                .map(|c| c.clone().into())
                .collect::<Vec<_>>(),
        ));
        let logical_plan = self.make_logical_plan(&select_statement)?;
        let source_unique_columns = self.extract_source_unique_columns(&logical_plan)?;

        let (projection_plan, filter_plan) =
            self.make_projection_and_filter_physical_plans(&logical_plan, metadata_cache_factory)?;
        if target_schema != projection_plan.schema() {
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
        ))
    }

    fn make_logical_plan(&self, select_statement: &str) -> Result<LogicalPlan, CubeError> {
        let dialect = &MySqlDialectWithBackTicks {};
        let mut tokenizer = Tokenizer::new(dialect, &select_statement);
        let tokens = tokenizer.tokenize().unwrap();
        let statement = Parser::new(tokens, dialect).parse_statement()?;
        let statement = self.rewrite_statement(statement);

        match &statement {
            Statement::Query(box Query {
                body: SetExpr::Select(_),
                ..
            }) => {
                let provider = TopicTableProvider::new(self.topic.clone(), &self.source_columns);
                let query_planner = SqlToRel::new(&provider);
                let logical_plan =
                    query_planner.statement_to_plan(&DFStatement::Statement(statement.clone()))?;
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
                body: SetExpr::Select(mut s),
                with,
                order_by,
                limit,
                offset,
                fetch,
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
                    body: SetExpr::Select(s),
                    order_by,
                    limit,
                    offset,
                    fetch,
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
            SQExpr::Cast { expr, data_type } => SQExpr::Cast {
                expr: Box::new(self.rewrite_expr(*expr)),
                data_type,
            },
            SQExpr::TryCast { expr, data_type } => SQExpr::TryCast {
                expr: Box::new(self.rewrite_expr(*expr)),
                data_type,
            },
            SQExpr::Extract { field, expr } => SQExpr::Extract {
                field,
                expr: Box::new(self.rewrite_expr(*expr)),
            },
            SQExpr::Substring {
                expr,
                substring_from,
                substring_for,
            } => SQExpr::Substring {
                expr: Box::new(self.rewrite_expr(*expr)),
                substring_from,
                substring_for,
            },
            SQExpr::Nested(e) => SQExpr::Nested(Box::new(self.rewrite_expr(*e))),
            SQExpr::Function(mut f) => {
                f.name = if f.name.0.len() == 1 && f.name.0[0].value.to_lowercase() == "convert_tz"
                {
                    ObjectName(vec![Ident {
                        value: "CONVERT_TZ_KSQL".to_string(),
                        quote_style: None,
                    }])
                } else {
                    f.name
                };
                f.args = f
                    .args
                    .into_iter()
                    .map(|a| match a {
                        FunctionArg::Named { name, arg } => FunctionArg::Named {
                            name,
                            arg: self.rewrite_expr(arg),
                        },
                        FunctionArg::Unnamed(expr) => FunctionArg::Unnamed(self.rewrite_expr(expr)),
                    })
                    .collect::<Vec<_>>();
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
            LogicalPlan::Projection { expr, .. } => {
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
    fn make_projection_and_filter_physical_plans(
        &self,
        plan: &LogicalPlan,
        metadata_cache_factory: Arc<dyn MetadataCacheFactory>,
    ) -> Result<(Arc<dyn ExecutionPlan>, Option<Arc<dyn ExecutionPlan>>), CubeError> {
        let source_schema = Arc::new(Schema::new(
            self.source_columns
                .iter()
                .map(|c| c.clone().into())
                .collect::<Vec<_>>(),
        ));
        let empty_exec = Arc::new(EmptyExec::new(false, source_schema));
        match plan {
            LogicalPlan::Projection {
                input: projection_input,
                expr,
                schema,
            } => match projection_input.as_ref() {
                filter_plan @ LogicalPlan::Filter { input, .. } => match input.as_ref() {
                    LogicalPlan::TableScan { .. } => {
                        let projection_plan = self.make_projection_plan(
                            expr,
                            schema.clone(),
                            projection_input.clone(),
                        )?;
                        let plan_ctx = Arc::new(ExecutionContext::with_config(
                            ExecutionConfig::new()
                                .with_metadata_cache_factory(metadata_cache_factory),
                        ));

                        let projection_phys_plan = plan_ctx
                            .create_physical_plan(&projection_plan)?
                            .with_new_children(vec![empty_exec.clone()])?;

                        let filter_phys_plan = plan_ctx
                            .create_physical_plan(&filter_plan)?
                            .with_new_children(vec![empty_exec.clone()])?;

                        Ok((projection_phys_plan.clone(), Some(filter_phys_plan)))
                    }
                    _ => Err(CubeError::user(
                        "Only Projection > [Filter] > TableScan plans are allowed for streaming"
                            .to_string(),
                    )),
                },
                LogicalPlan::TableScan { .. } => {
                    let projection_plan =
                        self.make_projection_plan(expr, schema.clone(), projection_input.clone())?;
                    let plan_ctx = Arc::new(ExecutionContext::with_config(
                        ExecutionConfig::new().with_metadata_cache_factory(metadata_cache_factory),
                    ));
                    let projection_phys_plan = plan_ctx
                        .create_physical_plan(&projection_plan)?
                        .with_new_children(vec![empty_exec.clone()])?;
                    Ok((projection_phys_plan, None))
                }
                _ => Err(CubeError::user(
                    "Only Projection > [Filter] > TableScan plans are allowed for streaming"
                        .to_string(),
                )),
            },
            _ => Err(CubeError::user(
                "Only Projection > [Filter] > TableScan plans are allowed for streaming"
                    .to_string(),
            )),
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
            res.push(Expr::Column(DFColumn::from_name(
                self.seq_column.get_name(),
            )));
            Arc::new(schema.join(&DFSchema::new(vec![DFField::new(
                None,
                self.seq_column.get_name(),
                datafusion::arrow::datatypes::DataType::Int64,
                true,
            )])?)?)
        } else {
            schema.clone()
        };

        Ok(LogicalPlan::Projection {
            expr: res,
            input,
            schema: result_schema,
        })
    }

    fn col_name_from_expr(&self, expr: &Expr) -> Result<String, CubeError> {
        match expr {
            Expr::Column(c) => Ok(c.name.clone()),
            Expr::Alias(_, name) => Ok(name.clone()),
            _ => Err(CubeError::user(
                "All expressions must have aliases in kafka streaming queries".to_string(),
            )),
        }
    }

    fn get_source_unique_column(&self, expr: &Expr) -> Result<Column, CubeError> {
        fn find_column_name(expr: &Expr) -> Result<Option<String>, CubeError> {
            match expr {
                Expr::Column(c) => Ok(Some(c.name.clone())),
                Expr::Alias(e, _) => find_column_name(&**e),
                Expr::ScalarUDF { args, .. } => {
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
            Expr::Alias(e, _) => match &**e {
                Expr::Column(c) => Ok(c.name.clone()),
                Expr::ScalarUDF { .. } => find_column_name(expr)?.ok_or_else(|| {
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
