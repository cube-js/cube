use crate::logical_plan::*;
use crate::planner::query_properties::OrderByItem;
use crate::planner::sql_evaluator::collectors::has_multi_stage_members;
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct MultiStageDimensionCalculation {
    schema: Rc<LogicalSchema>,
    multi_stage_dimension: Rc<MemberSymbol>,
    #[builder(default)]
    order_by: Vec<OrderByItem>,
    source: Rc<FullKeyAggregate>,
}

impl MultiStageDimensionCalculation {
    pub fn schema(&self) -> &Rc<LogicalSchema> {
        &self.schema
    }

    pub fn multi_stage_dimension(&self) -> &Rc<MemberSymbol> {
        &self.multi_stage_dimension
    }

    pub fn order_by(&self) -> &Vec<OrderByItem> {
        &self.order_by
    }

    pub fn source(&self) -> &Rc<FullKeyAggregate> {
        &self.source
    }

    pub fn resolved_dimensions(&self) -> Result<Vec<String>, CubeError> {
        let mut result = vec![];
        for dim in self.schema.all_dimensions() {
            if has_multi_stage_members(dim, true)? {
                result.push(dim.clone().resolve_reference_chain().full_name());
            }
        }
        result.sort();
        Ok(result)
    }

    pub fn join_dimensions(&self) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
        let mut result = if let Ok(dimension) = self.multi_stage_dimension.as_dimension() {
            dimension.add_group_by().clone().unwrap_or_default()
        } else {
            vec![]
        };
        for dim in self.schema.all_dimensions() {
            if !has_multi_stage_members(dim, true)? {
                result.push(dim.clone());
            }
        }
        let result = result
            .into_iter()
            .unique_by(|d| d.full_name())
            .collect_vec();
        Ok(result)
    }
}

impl PrettyPrint for MultiStageDimensionCalculation {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("Dimension Calculation",), state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println("schema:", &state);
        self.schema().pretty_print(result, &details_state);
        if !self.order_by().is_empty() {
            result.println("order_by:", &state);
            for order_by in self.order_by().iter() {
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
        self.source().pretty_print(result, &details_state);
    }
}

impl LogicalNode for MultiStageDimensionCalculation {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::MultiStageDimensionCalculation(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![self.source().as_plan_node()]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 1, self.node_name())?;
        let source = &inputs[0];

        Ok(Rc::new(
            Self::builder()
                .schema(self.schema().clone())
                .order_by(self.order_by().clone())
                .multi_stage_dimension(self.multi_stage_dimension.clone())
                .source(source.clone().into_logical_node()?)
                .build(),
        ))
    }

    fn node_name(&self) -> &'static str {
        "MultiStageDimensionCalculation"
    }

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::MultiStageDimensionCalculation(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "MultiStageMeasureCalculation"))
        }
    }
}
