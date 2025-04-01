use datafusion::logical_expr::Operator;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::expressions::{BinaryExpr, CastExpr, Column, Literal, TryCastExpr};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{ExecutionPlan, InputOrderMode, PhysicalExpr};
use serde::Serialize;
use serde_json::{json, Value};

use crate::queryplanner::query_executor::CubeTableExec;

#[derive(Serialize, Debug)]
pub struct PhysicalPlanFlags {
    pub merge_sort_plan: bool,
    pub predicate_sorted: Option<bool>,
}

impl PhysicalPlanFlags {
    pub fn is_suboptimal_query(&self) -> bool {
        !self.merge_sort_plan || self.predicate_sorted == Some(false)
    }

    pub fn to_json(&self) -> Value {
        json!(self)
    }

    pub fn with_execution_plan(p: &dyn ExecutionPlan) -> Self {
        let mut flags = PhysicalPlanFlags {
            merge_sort_plan: false,
            predicate_sorted: None,
        };
        PhysicalPlanFlags::physical_plan_flags_fill(p, &mut flags);
        flags
    }

    fn physical_plan_flags_fill(p: &dyn ExecutionPlan, flags: &mut PhysicalPlanFlags) {
        let a = p.as_any();
        if let Some(agg) = a.downcast_ref::<AggregateExec>() {
            let is_final_hash_agg_without_groups = agg.mode() == &AggregateMode::Final
                && agg.input_order_mode() == &InputOrderMode::Linear
                && agg.group_expr().expr().len() == 0;

            let is_full_inplace_agg = agg.mode() == &AggregateMode::Single
                && agg.input_order_mode() == &InputOrderMode::Sorted;

            let is_final_inplace_agg = agg.mode() == &AggregateMode::Final
                && agg.input_order_mode() == &InputOrderMode::Sorted;

            if is_final_hash_agg_without_groups || is_full_inplace_agg || is_final_inplace_agg {
                flags.merge_sort_plan = true;
            }

            // Stop the recursion if we have an optimal plan with groups, otherwise continue to check the children, filters for example
            if agg.group_expr().expr().len() > 0 && flags.merge_sort_plan {
                return;
            }
        } else if let Some(f) = a.downcast_ref::<FilterExec>() {
            // Stop the recursion if we found a filter and if plan already suboptimal or predicate flag is already set
            if flags.merge_sort_plan == false || flags.predicate_sorted.is_some() {
                return;
            }

            let predicate = f.predicate();
            let predicate_column_groups = extract_columns_with_operators(predicate.as_ref());
            let input = f.input();
            let input_as_any = input.as_any();

            let maybe_input_exec = input_as_any
                .downcast_ref::<CoalescePartitionsExec>()
                .map(|exec| exec.input().as_any())
                .or_else(|| {
                    input
                        .as_any()
                        .downcast_ref::<SortPreservingMergeExec>()
                        .map(|exec| exec.input().as_any())
                });

            // Left "if true" in DF upgrade branch to keep indentation and reduce conflicts.
            if true {
                let input_exec_any = maybe_input_exec.unwrap_or(input_as_any);
                if let Some(cte) = input_exec_any.downcast_ref::<CubeTableExec>() {
                    let sort_key_size = cte.index_snapshot.index.row.sort_key_size() as usize;
                    let index_columns =
                        cte.index_snapshot.index.row.columns()[..sort_key_size].to_vec();
                    flags.predicate_sorted = Some(check_predicate_order(
                        predicate_column_groups,
                        &index_columns,
                    ));
                }
            }
        }

        for child in p.children() {
            PhysicalPlanFlags::physical_plan_flags_fill(child.as_ref(), flags);
        }
    }
}

fn check_predicate_order(
    predicate_column_groups: Vec<Vec<(&Column, &Operator)>>,
    index_columns: &Vec<crate::metastore::Column>,
) -> bool {
    let index_column_names: Vec<String> = index_columns
        .into_iter()
        .map(|c| c.get_name().clone())
        .collect();

    'group_loop: for group in predicate_column_groups.iter() {
        if group.len() == 0 {
            // No columns in the group means a non-binary expression (InListExpr, IsNullExpr etc.)
            // Which is suboptimal for now
            return false;
        }

        let eq_column_names: Vec<String> = group
            .iter()
            .filter_map(|(col, op)| {
                if matches!(op, Operator::Eq) {
                    Some(col.name().to_string())
                } else {
                    None
                }
            })
            .collect();

        let mut checked_length = 0;
        for index_name in &index_column_names {
            if eq_column_names.contains(index_name) {
                checked_length += 1;
            }
        }

        if index_column_names.len() > checked_length {
            return false;
        }
        continue 'group_loop;
    }

    true
}

fn extract_columns_with_operators(predicate: &dyn PhysicalExpr) -> Vec<Vec<(&Column, &Operator)>> {
    let mut columns = Vec::new();
    extract_columns_with_operators_impl(predicate, &mut columns, true);
    columns
}

fn extract_columns_with_operators_impl<'a>(
    predicate: &'a dyn PhysicalExpr,
    out: &mut Vec<Vec<(&'a Column, &'a Operator)>>,
    is_root: bool,
) {
    let is_constant = |mut e: &dyn PhysicalExpr| loop {
        if e.as_any().is::<Literal>() {
            return true;
        } else if let Some(c) = e.as_any().downcast_ref::<CastExpr>() {
            e = c.expr().as_ref();
        } else if let Some(c) = e.as_any().downcast_ref::<TryCastExpr>() {
            e = c.expr().as_ref();
        } else {
            return false;
        }
    };

    let predicate = predicate.as_any();
    if let Some(binary) = predicate.downcast_ref::<BinaryExpr>() {
        match binary.op() {
            Operator::Or => {
                let conditions = [binary.left().as_ref(), binary.right().as_ref()]
                    .iter()
                    .flat_map(|&expr| {
                        let mut sub_conditions = Vec::new();
                        extract_columns_with_operators_impl(expr, &mut sub_conditions, false);
                        sub_conditions
                    })
                    .collect::<Vec<_>>();
                if !conditions.is_empty() {
                    out.extend(conditions);
                }
            }
            Operator::And => {
                extract_columns_with_operators_impl(binary.left().as_ref(), out, false);
                extract_columns_with_operators_impl(binary.right().as_ref(), out, false);
            }
            _ => {
                let mut left = binary.left();
                let mut right = binary.right();
                if !left.as_any().is::<Column>() {
                    std::mem::swap(&mut left, &mut right);
                }
                let left = left.as_any().downcast_ref::<Column>();
                if let Some(column) = left {
                    if is_constant(right.as_ref()) {
                        if out.is_empty() || is_root {
                            out.push(Vec::new());
                        }
                        out.last_mut().unwrap().push((column, binary.op()));
                    }
                }
            }
        }
    } else if is_root && out.is_empty() {
        out.push(Vec::new());
    }
}
