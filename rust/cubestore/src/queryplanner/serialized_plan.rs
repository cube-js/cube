use crate::metastore::table::{Table, TablePath};
use crate::metastore::{Chunk, IdRow, Index, MetaStore, Partition};
use crate::queryplanner::query_executor::CubeTable;
use crate::CubeError;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::logical_plan::{Expr, JoinType, LogicalPlan, Operator, TableSource};
use datafusion::physical_plan::{aggregates, functions};
use datafusion::scalar::ScalarValue;
use futures::future::BoxFuture;
use futures::FutureExt;
use itertools::Itertools;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SerializedPlan {
    logical_plan: Arc<SerializedLogicalPlan>,
    schema_snapshot: Arc<SchemaSnapshot>,
    partition_ids_to_execute: HashSet<u64>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SchemaSnapshot {
    index_snapshots: Vec<IndexSnapshot>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct IndexSnapshot {
    table_path: TablePath,
    index: IdRow<Index>,
    partitions: Vec<PartitionSnapshot>,
}

impl IndexSnapshot {
    pub fn table_name(&self) -> String {
        self.table_path.table_name()
    }

    pub fn table(&self) -> &IdRow<Table> {
        &self.table_path.table
    }

    pub fn index(&self) -> &IdRow<Index> {
        &self.index
    }

    pub fn partitions(&self) -> &Vec<PartitionSnapshot> {
        &self.partitions
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PartitionSnapshot {
    partition: IdRow<Partition>,
    chunks: Vec<IdRow<Chunk>>,
}

impl PartitionSnapshot {
    pub fn partition(&self) -> &IdRow<Partition> {
        &self.partition
    }

    pub fn chunks(&self) -> &Vec<IdRow<Chunk>> {
        &self.chunks
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum SerializedLogicalPlan {
    Projection {
        expr: Vec<SerializedExpr>,
        input: Arc<SerializedLogicalPlan>,
        schema: SchemaRef,
    },
    Filter {
        predicate: SerializedExpr,
        input: Arc<SerializedLogicalPlan>,
    },
    Aggregate {
        input: Arc<SerializedLogicalPlan>,
        group_expr: Vec<SerializedExpr>,
        aggr_expr: Vec<SerializedExpr>,
        schema: SchemaRef,
    },
    Sort {
        expr: Vec<SerializedExpr>,
        input: Arc<SerializedLogicalPlan>,
    },
    Union {
        inputs: Vec<Arc<SerializedLogicalPlan>>,
        schema: SchemaRef,
        alias: Option<String>,
    },
    Join {
        left: Arc<SerializedLogicalPlan>,
        right: Arc<SerializedLogicalPlan>,
        on: Vec<(String, String)>,
        join_type: JoinType,
        schema: SchemaRef,
    },
    TableScan {
        schema_name: String,
        source: SerializedTableSource,
        table_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        projected_schema: SchemaRef,
        alias: Option<String>,
    },
    EmptyRelation {
        produce_one_row: bool,
        schema: SchemaRef,
    },
    Limit {
        n: usize,
        input: Arc<SerializedLogicalPlan>,
    },
}

impl SerializedLogicalPlan {
    fn logical_plan(&self) -> LogicalPlan {
        match self {
            SerializedLogicalPlan::Projection {
                expr,
                input,
                schema,
            } => LogicalPlan::Projection {
                expr: expr.iter().map(|e| e.expr()).collect(),
                input: Arc::new(input.logical_plan()),
                schema: schema.clone(),
            },
            SerializedLogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
                predicate: predicate.expr(),
                input: Arc::new(input.logical_plan()),
            },
            SerializedLogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
            } => LogicalPlan::Aggregate {
                group_expr: group_expr.iter().map(|e| e.expr()).collect(),
                aggr_expr: aggr_expr.iter().map(|e| e.expr()).collect(),
                input: Arc::new(input.logical_plan()),
                schema: schema.clone(),
            },
            SerializedLogicalPlan::Sort { expr, input } => LogicalPlan::Sort {
                expr: expr.iter().map(|e| e.expr()).collect(),
                input: Arc::new(input.logical_plan()),
            },
            SerializedLogicalPlan::Union {
                inputs,
                schema,
                alias,
            } => LogicalPlan::Union {
                inputs: inputs.iter().map(|p| Arc::new(p.logical_plan())).collect(),
                schema: schema.clone(),
                alias: alias.clone(),
            },
            SerializedLogicalPlan::TableScan {
                schema_name,
                source,
                table_schema,
                projection,
                projected_schema,
                alias,
            } => LogicalPlan::TableScan {
                schema_name: schema_name.clone(),
                source: match source {
                    SerializedTableSource::FromContext(v) => TableSource::FromContext(v.clone()),
                },
                table_schema: table_schema.clone(),
                projection: projection.clone(),
                projected_schema: projected_schema.clone(),
                alias: alias.clone(),
            },
            SerializedLogicalPlan::EmptyRelation {
                produce_one_row,
                schema,
            } => LogicalPlan::EmptyRelation {
                produce_one_row: *produce_one_row,
                schema: schema.clone(),
            },
            SerializedLogicalPlan::Limit { n, input } => LogicalPlan::Limit {
                n: *n,
                input: Arc::new(input.logical_plan()),
            },
            SerializedLogicalPlan::Join {
                left,
                right,
                on,
                join_type,
                schema,
            } => LogicalPlan::Join {
                left: Arc::new(left.logical_plan()),
                right: Arc::new(right.logical_plan()),
                on: on.clone(),
                join_type: join_type.clone(),
                schema: schema.clone(),
            },
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum SerializedExpr {
    Alias(Box<SerializedExpr>, String),
    Column(String, Option<String>),
    ScalarVariable(Vec<String>),
    Literal(ScalarValue),
    BinaryExpr {
        left: Box<SerializedExpr>,
        op: Operator,
        right: Box<SerializedExpr>,
    },
    Not(Box<SerializedExpr>),
    IsNotNull(Box<SerializedExpr>),
    IsNull(Box<SerializedExpr>),
    Case {
        /// Optional base expression that can be compared to literal values in the "when" expressions
        expr: Option<Box<SerializedExpr>>,
        /// One or more when/then expressions
        when_then_expr: Vec<(Box<SerializedExpr>, Box<SerializedExpr>)>,
        /// Optional "else" expression
        else_expr: Option<Box<SerializedExpr>>,
    },
    Cast {
        expr: Box<SerializedExpr>,
        data_type: DataType,
    },
    Sort {
        expr: Box<SerializedExpr>,
        asc: bool,
        nulls_first: bool,
    },
    ScalarFunction {
        fun: functions::BuiltinScalarFunction,
        args: Vec<SerializedExpr>,
    },
    AggregateFunction {
        fun: aggregates::AggregateFunction,
        args: Vec<SerializedExpr>,
        distinct: bool,
    },
    Wildcard,
}

impl SerializedExpr {
    fn expr(&self) -> Expr {
        match self {
            SerializedExpr::Alias(e, a) => Expr::Alias(Box::new(e.expr()), a.to_string()),
            SerializedExpr::Column(c, a) => Expr::Column(c.clone(), a.clone()),
            SerializedExpr::ScalarVariable(v) => Expr::ScalarVariable(v.clone()),
            SerializedExpr::Literal(v) => Expr::Literal(v.clone()),
            SerializedExpr::BinaryExpr { left, op, right } => Expr::BinaryExpr {
                left: Box::new(left.expr()),
                op: op.clone(),
                right: Box::new(right.expr()),
            },
            SerializedExpr::Not(e) => Expr::Not(Box::new(e.expr())),
            SerializedExpr::IsNotNull(e) => Expr::IsNotNull(Box::new(e.expr())),
            SerializedExpr::IsNull(e) => Expr::IsNull(Box::new(e.expr())),
            SerializedExpr::Cast { expr, data_type } => Expr::Cast {
                expr: Box::new(expr.expr()),
                data_type: data_type.clone(),
            },
            SerializedExpr::Sort {
                expr,
                asc,
                nulls_first,
            } => Expr::Sort {
                expr: Box::new(expr.expr()),
                asc: *asc,
                nulls_first: *nulls_first,
            },
            SerializedExpr::ScalarFunction { fun, args } => Expr::ScalarFunction {
                fun: fun.clone(),
                args: args.iter().map(|e| e.expr()).collect(),
            },
            SerializedExpr::AggregateFunction {
                fun,
                args,
                distinct,
            } => Expr::AggregateFunction {
                fun: fun.clone(),
                args: args.iter().map(|e| e.expr()).collect(),
                distinct: *distinct,
            },
            SerializedExpr::Case {
                expr,
                else_expr,
                when_then_expr,
            } => Expr::Case {
                expr: expr.as_ref().map(|e| Box::new(e.expr())),
                else_expr: else_expr.as_ref().map(|e| Box::new(e.expr())),
                when_then_expr: when_then_expr
                    .iter()
                    .map(|(w, t)| (Box::new(w.expr()), Box::new(t.expr())))
                    .collect(),
            },
            SerializedExpr::Wildcard => Expr::Wildcard,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum SerializedTableSource {
    FromContext(String),
}

impl SerializedPlan {
    pub async fn try_new(
        plan: LogicalPlan,
        meta_store: Arc<dyn MetaStore>,
    ) -> Result<Self, CubeError> {
        let serialized_logical_plan = Self::serialized_logical_plan(&plan);
        let index_snapshots =
            Self::index_snapshots_from_plan(Arc::new(plan), meta_store, Vec::new(), None).await?;
        Ok(SerializedPlan {
            logical_plan: Arc::new(serialized_logical_plan),
            schema_snapshot: Arc::new(SchemaSnapshot { index_snapshots }),
            partition_ids_to_execute: HashSet::new(),
        })
    }

    pub fn with_partition_id_to_execute(&self, partition_ids_to_execute: HashSet<u64>) -> Self {
        Self {
            logical_plan: self.logical_plan.clone(),
            schema_snapshot: self.schema_snapshot.clone(),
            partition_ids_to_execute,
        }
    }

    pub fn partition_ids_to_execute(&self) -> HashSet<u64> {
        self.partition_ids_to_execute.clone()
    }

    pub fn logical_plan(&self) -> LogicalPlan {
        self.logical_plan.logical_plan()
    }

    pub fn index_snapshots(&self) -> &Vec<IndexSnapshot> {
        &self.schema_snapshot.index_snapshots
    }

    pub fn files_to_download(&self) -> Vec<String> {
        let indexes = self.index_snapshots();

        let mut files = Vec::new();

        for index in indexes.iter() {
            for partition in index.partitions() {
                if let Some(file) = partition
                    .partition
                    .get_row()
                    .get_full_name(partition.partition.get_id())
                {
                    files.push(file);
                }

                for chunk in partition.chunks() {
                    files.push(chunk.get_row().get_full_name(chunk.get_id()))
                }
            }
        }

        files
    }

    fn index_snapshots_from_plan_boxed(
        plan: Arc<LogicalPlan>,
        meta_store: Arc<dyn MetaStore>,
        index_snapshots: Vec<IndexSnapshot>,
        join_on: Option<Vec<String>>
    ) -> BoxFuture<'static, Result<Vec<IndexSnapshot>, CubeError>> {
        async move { Self::index_snapshots_from_plan(plan, meta_store, index_snapshots, join_on).await }
            .boxed()
    }

    async fn index_snapshots_from_plan(
        plan: Arc<LogicalPlan>,
        meta_store: Arc<dyn MetaStore>,
        mut index_snapshots: Vec<IndexSnapshot>,
        join_on: Option<Vec<String>>
    ) -> Result<Vec<IndexSnapshot>, CubeError> {
        match plan.as_ref() {
            LogicalPlan::EmptyRelation { .. } => Ok(index_snapshots),
            LogicalPlan::InMemoryScan { .. } => Ok(index_snapshots),
            LogicalPlan::CsvScan { .. } => Ok(index_snapshots),
            LogicalPlan::ParquetScan { .. } => Ok(index_snapshots),
            LogicalPlan::TableScan {
                source, projection, ..
            } => {
                let name_split = match source {
                    TableSource::FromContext(name) => name.split(".").collect::<Vec<_>>(),
                    TableSource::FromProvider(_) => unimplemented!(),
                };
                let table = meta_store
                    .get_table(name_split[0].to_string(), name_split[1].to_string())
                    .await?;
                let schema = meta_store
                    .get_schema_by_id(table.get_row().get_schema_id())
                    .await?;
                let default_index = meta_store.get_default_index(table.get_id()).await?;
                let index = if let Some(projection_column_indices) = projection {
                    let projection_columns =
                        CubeTable::project_to_table(&table, &projection_column_indices);
                    let indexes = meta_store.get_table_indexes(table.get_id()).await?;
                    if let Some((index, _)) = indexes
                        .into_iter()
                        .filter_map(|i| {
                            if let Some(join_on_columns) = join_on.as_ref() {
                                let join_columns_in_index = join_on_columns.iter().map(
                                    |c| i.get_row().get_columns().iter().find(|ic| ic.get_name().as_str() == c.as_str()).clone()
                                ).collect::<Vec<_>>();
                                if join_columns_in_index.iter().any(|c| c.is_none()) {
                                    return None;
                                }
                                let join_columns_indices =
                                    CubeTable::project_to_index_positions(
                                        &join_columns_in_index.into_iter().map(|c| c.unwrap().clone()).collect(),
                                        &i
                                    );
                                if (0..join_columns_indices.len()).map(|i| Some(i)).collect::<HashSet<_>>() !=
                                    join_columns_indices.into_iter().collect::<HashSet<_>>() {
                                    return None;
                                }
                            }
                            let projected_index_positions =
                                CubeTable::project_to_index_positions(&projection_columns, &i);
                            let score = projected_index_positions
                                .into_iter()
                                .fold_options(0, |a, b| a + b);
                            score.map(|s| (i, s))
                        })
                        .min_by_key(|(_, s)| *s)
                    {
                        index
                    } else {
                        if let Some(join_on_columns) = join_on {
                            return Err(CubeError::user(format!(
                                "Can't find index to join table {} on {}. Consider creating index: CREATE INDEX {}_{} ON {} ({})",
                                name_split.join("."),
                                join_on_columns.join(", "),
                                &name_split[1],
                                join_on_columns.join("_"),
                                name_split.join("."),
                                join_on_columns.join(", ")
                            )))
                        }
                        default_index
                    }
                } else {
                    if let Some(join_on_columns) = join_on {
                        return Err(CubeError::internal(format!(
                            "Can't find index to join table {} on {} and projection push down optimization has been disabled. Invalid state.",
                            name_split.join("."),
                            join_on_columns.join(", ")
                        )))
                    }
                    default_index
                };

                let partitions = meta_store
                    .get_active_partitions_by_index_id(index.get_id())
                    .await?;

                let meta_store_to_move = meta_store.clone();

                let mut partition_snapshots = Vec::new();

                for partition in partitions.into_iter() {
                    partition_snapshots.push(PartitionSnapshot {
                        chunks: meta_store_to_move
                            .clone()
                            .get_chunks_by_partition_with_non_repartitioned(partition.get_id())
                            .await?,
                        partition,
                    });
                }

                index_snapshots.push(IndexSnapshot {
                    index,
                    partitions: partition_snapshots,
                    table_path: TablePath {
                        table,
                        schema: Arc::new(schema),
                    },
                });

                Ok(index_snapshots)
            }
            LogicalPlan::Projection { input, .. } => {
                Self::index_snapshots_from_plan_boxed(input.clone(), meta_store, index_snapshots, join_on)
                    .await
            }
            LogicalPlan::Filter { input, .. } => {
                Self::index_snapshots_from_plan_boxed(input.clone(), meta_store, index_snapshots, join_on)
                    .await
            }
            LogicalPlan::Aggregate { input, .. } => {
                Self::index_snapshots_from_plan_boxed(input.clone(), meta_store, index_snapshots, join_on)
                    .await
            }
            LogicalPlan::Sort { input, .. } => {
                Self::index_snapshots_from_plan_boxed(input.clone(), meta_store, index_snapshots, join_on)
                    .await
            }
            LogicalPlan::Limit { input, .. } => {
                Self::index_snapshots_from_plan_boxed(input.clone(), meta_store, index_snapshots, join_on)
                    .await
            }
            LogicalPlan::CreateExternalTable { .. } => Ok(index_snapshots),
            LogicalPlan::Explain { .. } => Ok(index_snapshots),
            LogicalPlan::Extension { .. } => Ok(index_snapshots),
            LogicalPlan::Union { inputs, .. } => {
                let mut snapshots = index_snapshots;
                for i in inputs.iter() {
                    snapshots = Self::index_snapshots_from_plan_boxed(
                        i.clone(),
                        meta_store.clone(),
                        snapshots,
                        join_on.clone()
                    )
                    .await?;
                }
                Ok(snapshots)
            }
            LogicalPlan::Join { left, right, on, .. } => {
                let mut snapshots = index_snapshots;
                snapshots = Self::index_snapshots_from_plan_boxed(
                    left.clone(),
                    meta_store.clone(),
                    snapshots,
                    Some(
                        join_on.as_ref().unwrap_or(&Vec::new()).iter().map(|c| c.to_string())
                        .chain(on.iter().map(|(l, _)| l.split(".").last().unwrap().to_string())).collect()
                    )
                ).await?;
                snapshots = Self::index_snapshots_from_plan_boxed(
                    right.clone(),
                    meta_store.clone(),
                    snapshots,
                    Some(
                        join_on.as_ref().unwrap_or(&Vec::new()).iter().map(|c| c.to_string())
                            .chain(on.iter().map(|(_, r)| r.split(".").last().unwrap().to_string())).collect()
                    )
                ).await?;
                Ok(snapshots)
            }
        }
    }

    pub fn is_data_select_query(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::EmptyRelation { .. } => false,
            LogicalPlan::InMemoryScan { .. } => false,
            LogicalPlan::CsvScan { .. } => false,
            LogicalPlan::ParquetScan { .. } => false,
            LogicalPlan::TableScan { source, .. } => {
                let name_split = match source {
                    TableSource::FromContext(name) => name.split(".").collect::<Vec<_>>(),
                    TableSource::FromProvider(_) => unimplemented!(),
                };
                name_split[0].to_string() != "information_schema"
            }
            LogicalPlan::Projection { input, .. } => Self::is_data_select_query(input),
            LogicalPlan::Filter { input, .. } => Self::is_data_select_query(input),
            LogicalPlan::Aggregate { input, .. } => Self::is_data_select_query(input),
            LogicalPlan::Sort { input, .. } => Self::is_data_select_query(input),
            LogicalPlan::Limit { input, .. } => Self::is_data_select_query(input),
            LogicalPlan::CreateExternalTable { .. } => false,
            LogicalPlan::Explain { .. } => false,
            LogicalPlan::Extension { .. } => false,
            LogicalPlan::Union { inputs, .. } => {
                let mut snapshots = false;
                for i in inputs.iter() {
                    snapshots = snapshots || Self::is_data_select_query(i);
                }
                snapshots
            }
            LogicalPlan::Join { left, right, .. } => {
                Self::is_data_select_query(left) || Self::is_data_select_query(right)
            }
        }
    }

    fn serialized_logical_plan(plan: &LogicalPlan) -> SerializedLogicalPlan {
        match plan {
            LogicalPlan::EmptyRelation {
                produce_one_row,
                schema,
            } => SerializedLogicalPlan::EmptyRelation {
                produce_one_row: *produce_one_row,
                schema: schema.clone(),
            },
            LogicalPlan::InMemoryScan { .. } => unimplemented!(),
            LogicalPlan::CsvScan { .. } => unimplemented!(),
            LogicalPlan::ParquetScan { .. } => unimplemented!(),
            LogicalPlan::TableScan {
                schema_name,
                source,
                alias,
                projected_schema,
                table_schema,
                projection,
            } => SerializedLogicalPlan::TableScan {
                schema_name: schema_name.clone(),
                source: match source {
                    TableSource::FromContext(name) => {
                        SerializedTableSource::FromContext(name.to_string())
                    }
                    TableSource::FromProvider(_) => unimplemented!(),
                },
                alias: alias.clone(),
                projected_schema: projected_schema.clone(),
                table_schema: table_schema.clone(),
                projection: projection.clone(),
            },
            LogicalPlan::Projection {
                input,
                expr,
                schema,
            } => SerializedLogicalPlan::Projection {
                input: Arc::new(Self::serialized_logical_plan(input)),
                expr: expr.iter().map(|e| Self::serialized_expr(e)).collect(),
                schema: schema.clone(),
            },
            LogicalPlan::Filter { predicate, input } => SerializedLogicalPlan::Filter {
                input: Arc::new(Self::serialized_logical_plan(input)),
                predicate: Self::serialized_expr(predicate),
            },
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
            } => SerializedLogicalPlan::Aggregate {
                input: Arc::new(Self::serialized_logical_plan(input)),
                group_expr: group_expr
                    .iter()
                    .map(|e| Self::serialized_expr(e))
                    .collect(),
                aggr_expr: aggr_expr.iter().map(|e| Self::serialized_expr(e)).collect(),
                schema: schema.clone(),
            },
            LogicalPlan::Sort { expr, input } => SerializedLogicalPlan::Sort {
                input: Arc::new(Self::serialized_logical_plan(input)),
                expr: expr.iter().map(|e| Self::serialized_expr(e)).collect(),
            },
            LogicalPlan::Limit { n, input } => SerializedLogicalPlan::Limit {
                input: Arc::new(Self::serialized_logical_plan(input)),
                n: *n,
            },
            LogicalPlan::CreateExternalTable { .. } => unimplemented!(),
            LogicalPlan::Explain { .. } => unimplemented!(),
            LogicalPlan::Extension { .. } => unimplemented!(),
            LogicalPlan::Union {
                inputs,
                schema,
                alias,
            } => SerializedLogicalPlan::Union {
                inputs: inputs
                    .iter()
                    .map(|input| Arc::new(Self::serialized_logical_plan(&input)))
                    .collect::<Vec<_>>(),
                schema: schema.clone(),
                alias: alias.clone(),
            },
            LogicalPlan::Join {
                left,
                right,
                on,
                join_type,
                schema,
            } => SerializedLogicalPlan::Join {
                left: Arc::new(Self::serialized_logical_plan(&left)),
                right: Arc::new(Self::serialized_logical_plan(&right)),
                on: on.clone(),
                join_type: join_type.clone(),
                schema: schema.clone(),
            },
        }
    }

    fn serialized_expr(expr: &Expr) -> SerializedExpr {
        match expr {
            Expr::Alias(expr, alias) => {
                SerializedExpr::Alias(Box::new(Self::serialized_expr(expr)), alias.to_string())
            }
            Expr::Column(c, a) => SerializedExpr::Column(c.to_string(), a.clone()),
            Expr::ScalarVariable(v) => SerializedExpr::ScalarVariable(v.clone()),
            Expr::Literal(v) => SerializedExpr::Literal(v.clone()),
            Expr::BinaryExpr { left, op, right } => SerializedExpr::BinaryExpr {
                left: Box::new(Self::serialized_expr(left)),
                op: op.clone(),
                right: Box::new(Self::serialized_expr(right)),
            },
            Expr::Not(e) => SerializedExpr::Not(Box::new(Self::serialized_expr(&e))),
            Expr::IsNotNull(e) => SerializedExpr::IsNotNull(Box::new(Self::serialized_expr(&e))),
            Expr::IsNull(e) => SerializedExpr::IsNull(Box::new(Self::serialized_expr(&e))),
            Expr::Cast { expr, data_type } => SerializedExpr::Cast {
                expr: Box::new(Self::serialized_expr(&expr)),
                data_type: data_type.clone(),
            },
            Expr::Sort {
                expr,
                asc,
                nulls_first,
            } => SerializedExpr::Sort {
                expr: Box::new(Self::serialized_expr(&expr)),
                asc: *asc,
                nulls_first: *nulls_first,
            },
            Expr::ScalarFunction { fun, args } => SerializedExpr::ScalarFunction {
                fun: fun.clone(),
                args: args.iter().map(|e| Self::serialized_expr(&e)).collect(),
            },
            Expr::ScalarUDF { .. } => unimplemented!(),
            Expr::AggregateFunction {
                fun,
                args,
                distinct,
            } => SerializedExpr::AggregateFunction {
                fun: fun.clone(),
                args: args.iter().map(|e| Self::serialized_expr(&e)).collect(),
                distinct: *distinct,
            },
            Expr::AggregateUDF { .. } => unimplemented!(),
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
            } => SerializedExpr::Case {
                expr: expr.as_ref().map(|e| Box::new(Self::serialized_expr(&e))),
                else_expr: else_expr
                    .as_ref()
                    .map(|e| Box::new(Self::serialized_expr(&e))),
                when_then_expr: when_then_expr
                    .iter()
                    .map(|(w, t)| {
                        (
                            Box::new(Self::serialized_expr(&w)),
                            Box::new(Self::serialized_expr(&t)),
                        )
                    })
                    .collect(),
            },
            Expr::Wildcard => SerializedExpr::Wildcard,
        }
    }
}
