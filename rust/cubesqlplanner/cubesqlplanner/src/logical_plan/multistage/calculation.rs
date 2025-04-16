use crate::logical_plan::*;
use crate::planner::query_properties::OrderByItem;
use crate::planner::sql_evaluator::MemberSymbol;
use itertools::Itertools;
use std::rc::Rc;

#[derive(PartialEq)]
pub enum MultiStageCalculationType {
    Rank,
    Aggregate,
    Calculate,
}

impl ToString for MultiStageCalculationType {
    fn to_string(&self) -> String {
        match self {
            MultiStageCalculationType::Rank => "Rank".to_string(),
            MultiStageCalculationType::Aggregate => "Aggregate".to_string(),
            MultiStageCalculationType::Calculate => "Calculate".to_string(),
        }
    }
}

#[derive(PartialEq)]
pub enum MultiStageCalculationWindowFunction {
    Rank,
    Window,
    None,
}

impl ToString for MultiStageCalculationWindowFunction {
    fn to_string(&self) -> String {
        match self {
            MultiStageCalculationWindowFunction::Rank => "Rank".to_string(),
            MultiStageCalculationWindowFunction::Window => "Window".to_string(),
            MultiStageCalculationWindowFunction::None => "None".to_string(),
        }
    }
}

pub struct MultiStageMeasureCalculation {
    pub schema: Rc<LogicalSchema>,
    pub is_ungrouped: bool,
    pub calculation_type: MultiStageCalculationType,
    pub partition_by: Vec<Rc<MemberSymbol>>,
    pub window_function_to_use: MultiStageCalculationWindowFunction,
    pub order_by: Vec<OrderByItem>,
    pub source: Rc<FullKeyAggregate>,
}

impl PrettyPrint for MultiStageMeasureCalculation {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(
            &format!("Measure Calculation: {}", self.calculation_type.to_string()),
            state,
        );
        let state = state.new_level();
        let details_state = state.new_level();
        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        if !self.partition_by.is_empty() {
            result.println(
                &format!(
                    "partition_by: {}",
                    self.partition_by.iter().map(|m| m.full_name()).join(", ")
                ),
                &state,
            );
        }
        if self.window_function_to_use != MultiStageCalculationWindowFunction::None {
            result.println(
                &format!(
                    "window_function_to_use: {}",
                    self.window_function_to_use.to_string()
                ),
                &state,
            );
        }
        if self.is_ungrouped {
            result.println("is_ungrouped: true", &state);
        }
        if !self.order_by.is_empty() {
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
        result.println("source:", &state);
        self.source.pretty_print(result, &details_state);
    }
}
