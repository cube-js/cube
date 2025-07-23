use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub enum QuerySource {
    LogicalJoin(Rc<LogicalJoin>),
    FullKeyAggregate(Rc<FullKeyAggregate>),
    PreAggregation(Rc<PreAggregation>),
}

impl QuerySource {
    fn as_plan_node(&self) -> PlanNode {
        match self {
            Self::LogicalJoin(item) => item.as_plan_node(),
            Self::FullKeyAggregate(item) => item.as_plan_node(),
            Self::PreAggregation(item) => item.as_plan_node(),
        }
    }
    fn with_plan_node(&self, plan_node: PlanNode) -> Result<Self, CubeError> {
        Ok(match self {
            Self::LogicalJoin(_) => Self::LogicalJoin(plan_node.into_logical_node()?),
            Self::FullKeyAggregate(_) => Self::FullKeyAggregate(plan_node.into_logical_node()?),
            Self::PreAggregation(_) => Self::PreAggregation(plan_node.into_logical_node()?),
        })
    }
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
    type InputsType = QueryInput;

    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::Query(self.clone())
    }

    fn inputs(&self) -> Self::InputsType {
        let source = self.source.as_plan_node();
        let multistage_members = self
            .multistage_members
            .iter()
            .map(|member| member.as_plan_node())
            .collect();

        QueryInput {
            source,
            multistage_members,
        }
    }

    fn with_inputs(self: Rc<Self>, inputs: Self::InputsType) -> Result<Rc<Self>, CubeError> {
        let QueryInput {
            source,
            multistage_members,
        } = inputs;

        check_inputs_len(
            "multistage_members",
            &multistage_members,
            self.multistage_members.len(),
            self.node_name(),
        )?;

        Ok(Rc::new(Self {
            multistage_members: multistage_members
                .into_iter()
                .map(|member| member.into_logical_node())
                .collect::<Result<Vec<_>, _>>()?,
            schema: self.schema.clone(),
            filter: self.filter.clone(),
            modifers: self.modifers.clone(),
            source: self.source.with_plan_node(source)?,
        }))
    }

    fn node_name(&self) -> &'static str {
        "Query"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::Query(query) = plan_node {
            Ok(query)
        } else {
            Err(cast_error(&plan_node, "Query"))
        }
    }
}

pub struct QueryInput {
    pub source: PlanNode,
    pub multistage_members: Vec<PlanNode>,
}

impl NodeInputs for QueryInput {
    fn iter(&self) -> Box<dyn Iterator<Item = &PlanNode> + '_> {
        Box::new(std::iter::once(&self.source).chain(self.multistage_members.iter()))
    }

    fn iter_mut(&mut self) -> Box<dyn Iterator<Item = &mut PlanNode> + '_> {
        Box::new(std::iter::once(&mut self.source).chain(self.multistage_members.iter_mut()))
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
