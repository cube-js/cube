use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub enum QuerySource {
    LogicalJoin(Rc<LogicalJoin>),
    FullKeyAggregate(Rc<FullKeyAggregate>),
    PreAggregation(Rc<PreAggregation>),
}
impl PrettyPrint for QuerySource {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match self {
            QuerySource::LogicalJoin(join) => join.pretty_print(result, state),
            QuerySource::FullKeyAggregate(full_key) => full_key.pretty_print(result, state),
            QuerySource::PreAggregation(pre_aggregation) => {
                pre_aggregation.pretty_print(result, state)
            }
        }
    }
}
#[derive(Clone)]
pub struct Query {
    pub multistage_members: Vec<Rc<LogicalMultiStageMember>>,
    pub schema: Rc<LogicalSchema>,
    pub filter: Rc<LogicalFilter>,
    pub modifers: Rc<LogicalQueryModifiers>,
    pub source: QuerySource,
}

impl LogicalNode for Query {
    type InputsType = SingleNodeInput;

    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::Query(self.clone())
    }

    fn inputs(&self) -> Self::InputsType {
        let plan_node = match &self.source {
            QuerySource::LogicalJoin(join) => SingleNodeInput::new(join.as_plan_node()),
            QuerySource::FullKeyAggregate(full_key) => {
                SingleNodeInput::new(full_key.as_plan_node())
            }
            QuerySource::PreAggregation(pre_aggregation) => {
                SingleNodeInput::new(pre_aggregation.as_plan_node())
            }
        };
        plan_node
    }

    fn with_inputs(self: Rc<Self>, inputs: Self::InputsType) -> Result<Rc<Self>, CubeError> {
        let source = match inputs.item() {
            PlanNode::LogicalJoin(item) => QuerySource::LogicalJoin(item.clone()),
            PlanNode::FullKeyAggregate(item) => QuerySource::FullKeyAggregate(item.clone()),
            PlanNode::PreAggregation(item) => QuerySource::PreAggregation(item.clone()),
            _ => {
                return Err(CubeError::internal(format!(
                    "{} is incorrect input for Query node",
                    inputs.item().node_name()
                )))
            }
        };
        Ok(Rc::new(Query {
            multistage_members: self.multistage_members.clone(),
            schema: self.schema.clone(),
            filter: self.filter.clone(),
            modifers: self.modifers.clone(),
            source,
        }))
    }

    fn node_name() -> &'static str {
        "Query"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::Query(query) = plan_node {
            Ok(query)
        } else {
            Err(cast_error::<Self>(&plan_node))
        }
    }
}

impl PrettyPrint for Query {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("Query: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        if !self.multistage_members.is_empty() {
            result.println("multistage_members:", &state);
            for member in self.multistage_members.iter() {
                member.pretty_print(result, &details_state);
            }
        }

        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        result.println("filters:", &state);
        self.filter.pretty_print(result, &details_state);
        self.modifers.pretty_print(result, &state);

        result.println("source:", &state);
        self.source.pretty_print(result, &details_state);
    }
}
