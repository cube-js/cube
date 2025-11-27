use std::{collections::HashMap, rc::Rc};

use cubenativeutils::CubeError;
use tokio::runtime::Handle;

use crate::{
    logical_plan::v2::{LeafNode, NodeKind, NodeSchema, PlanNode},
    planner::{
        planners::v2::PlannerPushDownContext,
        query_tools::QueryTools,
        sql_evaluator::{CompiledJoin, JoinHints, JoinHintsBuilder},
        QueryProperties,
    },
};

pub struct Planner {
    query_tools: Rc<QueryTools>,
    nodes: HashMap<String, Rc<PlanNode>>,
}

impl Planner {
    pub fn new(query_tools: Rc<QueryTools>) -> Self {
        Self {
            query_tools,
            nodes: HashMap::new(),
        }
    }

    pub fn build(&mut self, query_properties: Rc<QueryProperties>) -> Result<(), CubeError> {
        // Implementation goes here
        Ok(())
    }

    fn build_plan_node(
        &mut self,
        node_schema: NodeSchema,
        context: &PlannerPushDownContext,
    ) -> Result<Rc<PlanNode>, CubeError> {
        // Implementation goes here
        todo!()
    }

    fn build_leaf_node(
        &mut self,
        node_schema: NodeSchema,
        join_hints: JoinHints,
        context: &PlannerPushDownContext,
    ) -> Result<Rc<PlanNode>, CubeError> {
        let join = self.query_tools.get_join_by_hints(&join_hints)?;
        let kind = NodeKind::Leaf(LeafNode::new(join));
        let node_name = self.next_node_name();
        let result = PlanNode::builder()
            .name(node_name.clone())
            .context(context.node_context().clone())
            .kind(kind)
            .schema(node_schema)
            .build();
        let result = Rc::new(result);
        self.nodes.insert(node_name, result.clone());
        Ok(result)
    }

    fn next_node_name(&mut self) -> String {
        let name = format!("node_{}", self.nodes.len());
        name
    }

    fn get_common_join_hints(schema: &NodeSchema) -> Result<JoinHints, CubeError> {
        let mut hints_builder = JoinHints::builder();
        for symbol in schema.common_join_dependencies() {
            hints_builder.add_symbol(symbol)?;
        }
        Ok(hints_builder.finish())
    }
}
