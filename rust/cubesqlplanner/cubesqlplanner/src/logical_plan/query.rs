use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(Clone, TypedBuilder)]
pub struct Query {
    #[builder(default)]
    multistage_members: Vec<Rc<LogicalMultiStageMember>>,
    schema: Rc<LogicalSchema>,
    filter: Rc<LogicalFilter>,
    modifers: Rc<LogicalQueryModifiers>,
    source: QuerySource,
}

impl Query {
    pub fn multistage_members(&self) -> &Vec<Rc<LogicalMultiStageMember>> {
        &self.multistage_members
    }
    pub fn schema(&self) -> &Rc<LogicalSchema> {
        &self.schema
    }
    pub fn filter(&self) -> &Rc<LogicalFilter> {
        &self.filter
    }
    pub fn modifers(&self) -> &Rc<LogicalQueryModifiers> {
        &self.modifers
    }
    pub fn source(&self) -> &QuerySource {
        &self.source
    }
    pub fn set_source(&mut self, source: QuerySource) {
        self.source = source;
    }
}

impl LogicalNode for Query {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::Query(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        QueryInputPacker::pack(self)
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        let QueryInputUnPacker {
            multistage_members,
            source,
        } = QueryInputUnPacker::new(&self, &inputs)?;

        Ok(Rc::new(Self {
            multistage_members: multistage_members
                .iter()
                .map(|member| member.clone().into_logical_node())
                .collect::<Result<Vec<_>, _>>()?,
            schema: self.schema.clone(),
            filter: self.filter.clone(),
            modifers: self.modifers.clone(),
            source: self.source.with_plan_node(source.clone())?,
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

pub struct QueryInputPacker;

impl QueryInputPacker {
    pub fn pack(query: &Query) -> Vec<PlanNode> {
        let mut result = vec![];
        result.extend(
            query
                .multistage_members
                .iter()
                .map(|member| member.as_plan_node()),
        );
        result.push(query.source.as_plan_node());
        result
    }
}
pub struct QueryInputUnPacker<'a> {
    multistage_members: &'a [PlanNode],
    source: &'a PlanNode,
}

impl<'a> QueryInputUnPacker<'a> {
    pub fn new(query: &Query, inputs: &'a Vec<PlanNode>) -> Result<Self, CubeError> {
        check_inputs_len(&inputs, Self::inputs_len(query), query.node_name())?;
        let multistage_members = &inputs[0..query.multistage_members.len()];
        let source = &inputs[query.multistage_members.len()];
        Ok(Self {
            multistage_members,
            source,
        })
    }
    fn inputs_len(query: &Query) -> usize {
        query.multistage_members.len() + 1
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
