use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use super::query_inputs::QueryInputsProcessor;
use crate::logical_plan::{InputBase, QueryInputs, QuerySource};
use crate::physical_plan::From;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct QuerySourceProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, QuerySource> for QuerySourceProcessor<'a> {
    type PhysycalNode = Rc<From>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        query_source: &QuerySource,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let dimension_inputs = query_source.dimension_inputs();
        let inputs = QueryInputsProcessor::new(self.builder);
        match query_source.base() {
            InputBase::QueryInputs(base) => {
                inputs.process_add_dimension_inputs(base, dimension_inputs, context)
            }
            // An empty merge reads the multi-stage dimension CTE alone.
            InputBase::Merge(merge) if merge.is_empty() => inputs.process_add_dimension_inputs(
                &QueryInputs::builder().build(),
                dimension_inputs,
                context,
            ),
            _ if !dimension_inputs.is_empty() => Err(CubeError::internal(
                "Dimension inputs over a non-empty merge or a pre-aggregation are not supported"
                    .to_string(),
            )),
            base => self.builder.process_node(base, context),
        }
    }
}

impl ProcessableNode for QuerySource {
    type ProcessorType<'a> = QuerySourceProcessor<'a>;
}
