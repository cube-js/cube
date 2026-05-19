use super::*;
use crate::planner::planners::multi_stage::TimeShiftState;
use crate::planner::query_properties::OrderByItem;

/// How the pre-aggregation optimizer should treat this Query when walking
/// the multi-stage tree. Derived from `Query.kind()`; not stored on
/// modifiers anymore.
#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
pub enum PreAggregationRewriteRole {
    /// Try `try_rewrite_query` over this Query's own schema/filter
    /// (regular leaf — top-level, regular_measures, etc.).
    #[default]
    Leaf,
    /// Replace this whole subtree atomically by a pre-aggregation match
    /// on schema + outer-query filter (aggregate-multiplied subquery).
    WholeSubtree,
    /// Intermediate machinery — walk through to descendants without
    /// rewriting this Query itself (Stage Calculation).
    PassThrough,
    /// Raw fact source — never rewritten on its own; the rewrite unit is
    /// the parent (MeasureSubquery shape).
    NoRewrite,
}

/// Per-query modifiers that sit outside the result schema: paging,
/// ordering, and the ungrouped flag.
#[derive(Default, Clone)]
pub struct LogicalQueryModifiers {
    pub offset: Option<usize>,
    pub limit: Option<usize>,
    pub ungrouped: bool,
    pub order_by: Vec<OrderByItem>,
    pub time_shifts: TimeShiftState,
    pub render_measure_as_state: bool,
    pub render_measure_for_ungrouped: bool,
}

impl PrettyPrint for LogicalQueryModifiers {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        if let Some(offset) = &self.offset {
            result.println(&format!("offset:{}", offset), &state);
        }
        if let Some(limit) = &self.limit {
            result.println(&format!("limit:{}", limit), &state);
        }
        result.println(&format!("ungrouped:{}", self.ungrouped), &state);
        if !self.order_by.is_empty() {
            let details_state = state.new_level();
            result.println("order_by:", &state);
            for order_by in self.order_by.iter() {
                result.println(
                    &format!(
                        "{} {}",
                        order_by.name(),
                        if order_by.desc() { "desc" } else { "asc" }
                    ),
                    &details_state,
                );
            }
        }
        if !self.time_shifts.is_empty() {
            result.println("time_shifts:", &state);
            let details_state = state.new_level();
            for (_, time_shift) in self.time_shifts.dimensions_shifts.iter() {
                result.println(
                    &format!(
                        "- {}: {}",
                        time_shift.dimension.full_name(),
                        if let Some(interval) = &time_shift.interval {
                            interval.to_sql()
                        } else if let Some(name) = &time_shift.name {
                            format!("{} (named)", name.to_string())
                        } else {
                            "None".to_string()
                        }
                    ),
                    &details_state,
                );
            }
        }
        if self.render_measure_as_state {
            result.println("render_measure_as_state: true", &state);
        }
        if self.render_measure_for_ungrouped {
            result.println("render_measure_for_ungrouped: true", &state);
        }
    }
}
