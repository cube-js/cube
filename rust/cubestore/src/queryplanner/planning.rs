use crate::metastore::table::TablePath;
use crate::metastore::{IdRow, Index, MetaStore};
use crate::queryplanner::optimizations::rewrite_plan::{rewrite_plan, PlanRewriter};
use crate::queryplanner::partition_filter::PartitionFilter;
use crate::queryplanner::query_executor::CubeTable;
use crate::queryplanner::serialized_plan::{IndexSnapshot, PartitionSnapshot};
use crate::queryplanner::CubeTableLogical;
use arrow::datatypes::Field;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::logical_plan::{Expr, LogicalPlan};
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub async fn choose_index(
    p: &LogicalPlan,
    metastore: &dyn MetaStore,
) -> Result<(LogicalPlan, Vec<IndexSnapshot>), DataFusionError> {
    let mut r = ChooseIndex {
        metastore,
        collected_snapshots: Vec::new(),
    };
    let plan = rewrite_plan(p, &None, &mut r).await?;
    Ok((plan, r.collected_snapshots))
}

struct ChooseIndex<'a> {
    metastore: &'a dyn MetaStore,
    collected_snapshots: Vec<IndexSnapshot>,
}

struct SortColumns {
    sort_on: Vec<String>,
    required: bool,
}

#[async_trait]
impl PlanRewriter for ChooseIndex<'_> {
    type Context = Option<SortColumns>;

    async fn rewrite(
        &mut self,
        n: LogicalPlan,
        c: &Self::Context,
    ) -> Result<LogicalPlan, DataFusionError> {
        self.choose_table_index(n, c.as_ref().map(|sc| (&sc.sort_on, sc.required)))
            .await
    }

    fn enter_node(
        &mut self,
        n: &LogicalPlan,
        _: &Option<SortColumns>,
    ) -> Option<Option<SortColumns>> {
        fn column_name(expr: &Expr) -> Option<String> {
            match expr {
                Expr::Alias(e, _) => column_name(e),
                Expr::Column(col, _) => Some(col.to_string()), // TODO use alias
                _ => None,
            }
        }
        match n {
            LogicalPlan::Aggregate { group_expr, .. } => {
                let sort_on = group_expr.iter().map(column_name).collect::<Vec<_>>();
                if !sort_on.is_empty() && sort_on.iter().all(|c| c.is_some()) {
                    Some(Some(SortColumns {
                        sort_on: sort_on.into_iter().map(|c| c.unwrap()).collect(),
                        required: false,
                    }))
                } else {
                    Some(None)
                }
            }
            _ => None,
        }
    }

    fn enter_join_left(
        &mut self,
        join: &LogicalPlan,
        _: &Option<SortColumns>,
    ) -> Option<Option<SortColumns>> {
        let join_on;
        if let LogicalPlan::Join { on, .. } = join {
            join_on = on;
        } else {
            panic!("expected join node");
        }
        Some(Some(SortColumns {
            sort_on: join_on
                .iter()
                .map(|(l, _)| l.split(".").last().unwrap().to_string())
                .collect(),
            required: true,
        }))
    }

    fn enter_join_right(
        &mut self,
        join: &LogicalPlan,
        _c: &Self::Context,
    ) -> Option<Self::Context> {
        let join_on;
        if let LogicalPlan::Join { on, .. } = join {
            join_on = on;
        } else {
            panic!("expected join node");
        }
        Some(Some(SortColumns {
            sort_on: join_on
                .iter()
                .map(|(_, r)| r.split(".").last().unwrap().to_string())
                .collect(),
            required: true,
        }))
    }
}

impl ChooseIndex<'_> {
    async fn choose_table_index(
        &mut self,
        mut p: LogicalPlan,
        sort_on: Option<(&Vec<String>, bool)>,
    ) -> Result<LogicalPlan, DataFusionError> {
        let meta_store = self.metastore;
        match &mut p {
            LogicalPlan::TableScan {
                table_name,
                projection,
                filters,
                source,
                ..
            } => {
                let name_split = table_name.split(".").collect::<Vec<_>>();
                let table = meta_store
                    .get_table(name_split[0].to_string(), name_split[1].to_string())
                    .await?;
                let schema = meta_store
                    .get_schema_by_id(table.get_row().get_schema_id())
                    .await?;
                let default_index = meta_store.get_default_index(table.get_id()).await?;
                let (index, sort_on) = if let Some(projection_column_indices) = projection {
                    let projection_columns =
                        CubeTable::project_to_table(&table, &projection_column_indices);
                    let indexes = meta_store.get_table_indexes(table.get_id()).await?;
                    if let Some((index, _)) = indexes
                        .into_iter()
                        .filter_map(|i| {
                            if let Some((join_on_columns, _)) = sort_on.as_ref() {
                                let join_columns_in_index = join_on_columns
                                    .iter()
                                    .map(|c| {
                                        i.get_row()
                                            .get_columns()
                                            .iter()
                                            .find(|ic| ic.get_name().as_str() == c.as_str())
                                            .clone()
                                    })
                                    .collect::<Vec<_>>();
                                if join_columns_in_index.iter().any(|c| c.is_none()) {
                                    return None;
                                }
                                let join_columns_indices = CubeTable::project_to_index_positions(
                                    &join_columns_in_index
                                        .into_iter()
                                        .map(|c| c.unwrap().clone())
                                        .collect(),
                                    &i,
                                );
                                if (0..join_columns_indices.len())
                                    .map(|i| Some(i))
                                    .collect::<HashSet<_>>()
                                    != join_columns_indices.into_iter().collect::<HashSet<_>>()
                                {
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
                        (index, sort_on)
                    } else {
                        if let Some((join_on_columns, true)) = sort_on.as_ref() {
                            return Err(DataFusionError::Plan(format!(
                                "Can't find index to join table {} on {}. Consider creating index: CREATE INDEX {}_{} ON {} ({})",
                                name_split.join("."),
                                join_on_columns.join(", "),
                                &name_split[1],
                                join_on_columns.join("_"),
                                name_split.join("."),
                                join_on_columns.join(", ")
                            )));
                        }
                        (default_index, None)
                    }
                } else {
                    if let Some((join_on_columns, _)) = sort_on {
                        return Err(DataFusionError::Plan(format!(
                            "Can't find index to join table {} on {} and projection push down optimization has been disabled. Invalid state.",
                            name_split.join("."),
                            join_on_columns.join(", ")
                        )));
                    }
                    (default_index, None)
                };

                let partitions = meta_store
                    .get_active_partitions_and_chunks_by_index_id_for_select(index.get_id())
                    .await?;

                let partition_filter =
                    PartitionFilter::extract(&partition_filter_schema(&index), filters);
                log::trace!("Extracted partition filter is {:?}", partition_filter);
                let candidate_partitions = partitions.len();
                let mut pruned_partitions = 0;

                let mut partition_snapshots = Vec::new();
                for (partition, chunks) in partitions.into_iter() {
                    let min_row = partition
                        .get_row()
                        .get_min_val()
                        .as_ref()
                        .map(|r| r.values().as_slice());
                    let max_row = partition
                        .get_row()
                        .get_max_val()
                        .as_ref()
                        .map(|r| r.values().as_slice());

                    if !partition_filter.can_match(min_row, max_row) {
                        pruned_partitions += 1;
                        continue;
                    }

                    partition_snapshots.push(PartitionSnapshot { chunks, partition });
                }
                log::trace!(
                    "Pruned {} of {} partitions",
                    pruned_partitions,
                    candidate_partitions
                );

                assert!(source.as_any().is::<CubeTableLogical>());
                let snapshot = IndexSnapshot {
                    index,
                    partitions: partition_snapshots,
                    table_path: TablePath {
                        table,
                        schema: Arc::new(schema),
                    },
                    sort_on: sort_on.map(|(cols, _)| cols.clone()),
                };
                self.collected_snapshots.push(snapshot.clone());
                *source = Arc::new(CubeTable::try_new(
                    snapshot,
                    // These get filled on the workers.
                    HashMap::new(),
                    HashSet::new(),
                )?);
            }
            _ => {}
        }

        Ok(p)
    }
}

fn partition_filter_schema(index: &IdRow<Index>) -> arrow::datatypes::Schema {
    let schema_fields: Vec<Field>;
    schema_fields = index
        .get_row()
        .columns()
        .iter()
        .map(|c| c.clone().into())
        .take(index.get_row().sort_key_size() as usize)
        .collect();
    arrow::datatypes::Schema::new(schema_fields)
}
