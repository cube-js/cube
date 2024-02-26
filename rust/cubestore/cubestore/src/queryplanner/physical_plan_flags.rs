use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::hash_aggregate::{
    AggregateMode, AggregateStrategy, HashAggregateExec,
};
use datafusion::physical_plan::ExecutionPlan;

use serde::Serialize;
use serde_json::{json, Value};

#[derive(Serialize, Debug)]
pub struct PhysicalPlanFlags {
    pub merge_sort_plan: bool,
}

impl PhysicalPlanFlags {
    pub fn is_suboptimal_query(&self) -> bool {
        !self.merge_sort_plan
    }

    pub fn to_json(&self) -> Value {
        json!(self)
    }

    pub fn with_execution_plan(p: &dyn ExecutionPlan) -> Self {
        let mut flags = PhysicalPlanFlags {
            merge_sort_plan: false,
        };
        PhysicalPlanFlags::physical_plan_flags_fill(p, &mut flags);
        flags
    }

    fn physical_plan_flags_fill(p: &dyn ExecutionPlan, flags: &mut PhysicalPlanFlags) {
        let a = p.as_any();
        if let Some(agg) = a.downcast_ref::<HashAggregateExec>() {
            let is_final_hash_agg_without_groups = agg.mode() == &AggregateMode::Final
                && agg.strategy() == AggregateStrategy::Hash
                && agg.group_expr().len() == 0;

            let is_full_inplace_agg = agg.mode() == &AggregateMode::Full
                && agg.strategy() == AggregateStrategy::InplaceSorted;

            let is_final_inplace_agg = agg.mode() == &AggregateMode::Final
                && agg.strategy() == AggregateStrategy::InplaceSorted;

            if is_final_hash_agg_without_groups || is_full_inplace_agg || is_final_inplace_agg {
                flags.merge_sort_plan = true;
            }
        } else if let Some(f) = a.downcast_ref::<FilterExec>() {
            if flags.merge_sort_plan == false {
                return;
            }
            let output_hints = f.output_hints();
            let is_sorted = match output_hints.sort_order {
                Some(sort_order) => sort_order.len() >= output_hints.single_value_columns.len(),
                _ => false,
            };

            if !is_sorted {
                flags.merge_sort_plan = false;
                return;
            }
        }
        for child in p.children() {
            PhysicalPlanFlags::physical_plan_flags_fill(child.as_ref(), flags);
        }
    }
}
