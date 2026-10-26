use super::pretty_print::*;
use super::*;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// The subquery computing the multi-stage dimensions a query selects, read by
/// the `keys` it is grouped by.
#[derive(Clone)]
pub struct MultiStageDimensionInput {
    pub subquery: Rc<SubqueryRef>,
    pub keys: Vec<Rc<MemberSymbol>>,
}

/// The input a query reads, with the multi-stage dimension subquery joined to it.
#[derive(Clone)]
pub struct QuerySource {
    base: InputBase,
    multi_stage_dimension: Option<MultiStageDimensionInput>,
}

impl QuerySource {
    pub fn new(base: impl Into<InputBase>) -> Rc<Self> {
        Rc::new(Self {
            base: base.into(),
            multi_stage_dimension: None,
        })
    }

    pub fn base(&self) -> &InputBase {
        &self.base
    }

    pub fn multi_stage_dimension(&self) -> &Option<MultiStageDimensionInput> {
        &self.multi_stage_dimension
    }

    pub fn with_multi_stage_dimension(&self, input: MultiStageDimensionInput) -> Rc<Self> {
        Rc::new(Self {
            multi_stage_dimension: Some(input),
            ..self.clone()
        })
    }
}

impl LogicalNode for QuerySource {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::QuerySource(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![self.base.as_plan_node()]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 1, self.node_name())?;
        Ok(Rc::new(Self {
            base: self.base.with_plan_node(inputs[0].clone())?,
            multi_stage_dimension: self.multi_stage_dimension.clone(),
        }))
    }

    fn referenced_subqueries(&self) -> Vec<String> {
        self.multi_stage_dimension
            .iter()
            .map(|input| input.subquery.name().clone())
            .collect()
    }

    fn node_name(&self) -> &'static str {
        "QuerySource"
    }

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::QuerySource(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "QuerySource"))
        }
    }
}

impl PrettyPrint for QuerySource {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        self.base.pretty_print(result, state);
        if let Some(input) = &self.multi_stage_dimension {
            result.println(
                &format!(
                    "multi_stage_dimension: {} by {}",
                    input.subquery.name(),
                    print_symbols(&input.keys)
                ),
                state,
            );
        }
    }
}
