use crate::logical_plan::*;
use crate::planner::planners::multi_stage::TimeShiftState;
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct MultiStageLeafMeasure {
    pub measure: Rc<MemberSymbol>,
    pub render_measure_as_state: bool, //Render measure as state, for example hll state for count_approx
    pub render_measure_for_ungrouped: bool,
    pub time_shifts: TimeShiftState,
    pub query: Rc<Query>,
}

impl PrettyPrint for MultiStageLeafMeasure {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("Leaf Measure Query", state);
        let state = state.new_level();
        result.println(&format!("measure: {}", self.measure.full_name()), &state);
        if self.render_measure_as_state {
            result.println("render_measure_as_state: true", &state);
        }
        if self.render_measure_for_ungrouped {
            result.println("render_measure_for_ungrouped: true", &state);
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
        result.println(&format!("query:"), &state);
        let details_state = state.new_level();
        self.query.pretty_print(result, &details_state);
    }
}

impl LogicalNode for MultiStageLeafMeasure {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::MultiStageLeafMeasure(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![self.query.as_plan_node()]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 1, self.node_name())?;
        let query = &inputs[0];

        Ok(Rc::new(Self {
            measure: self.measure.clone(),
            render_measure_as_state: self.render_measure_as_state,
            render_measure_for_ungrouped: self.render_measure_for_ungrouped,
            time_shifts: self.time_shifts.clone(),
            query: query.clone().into_logical_node()?,
        }))
    }

    fn node_name(&self) -> &'static str {
        "MultiStageLeafMeasure"
    }

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::MultiStageLeafMeasure(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "MultiStageLeafMeasure"))
        }
    }
}
