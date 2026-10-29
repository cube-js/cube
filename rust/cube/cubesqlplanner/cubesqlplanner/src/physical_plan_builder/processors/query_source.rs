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
        let multi_stage_dimension = query_source.multi_stage_dimension();
        let inputs = QueryInputsProcessor::new(self.builder);
        match query_source.base() {
            InputBase::QueryInputs(base) => {
                inputs.process_with_multi_stage_dimension(base, multi_stage_dimension, context)
            }
            // An empty merge reads the multi-stage dimension CTE alone.
            InputBase::Merge(merge) if merge.is_empty() => inputs
                .process_with_multi_stage_dimension(
                    &QueryInputs::builder().build(),
                    multi_stage_dimension,
                    context,
                ),
            _ if multi_stage_dimension.is_some() => Err(CubeError::internal(
                "Multi-stage dimension input over a non-empty merge or a pre-aggregation is not supported"
                    .to_string(),
            )),
            base => self.builder.process_node(base, context),
        }
    }
}

impl ProcessableNode for QuerySource {
    type ProcessorType<'a> = QuerySourceProcessor<'a>;
}
