mod full_join_merge_strategy;
mod inner_join_merge_strategy;
mod keys_merge_strategy;
mod union_merge_strategy;

use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::Merge;
use crate::physical_plan::sql_nodes::SqlNodesFactory;
use crate::physical_plan::{From, FromSource, SelectBuilder, SingleAliasedSource};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use cubenativeutils::CubeError;
use full_join_merge_strategy::FullJoinMergeStrategy;
use inner_join_merge_strategy::InnerJoinMergeStrategy;
use keys_merge_strategy::KeysMergeStrategy;
use std::rc::Rc;
use union_merge_strategy::UnionMergeStrategy;

trait MergeStrategy {
    fn process(
        &self,
        merge: &Merge,
        context: &PushDownBuilderContext,
    ) -> Result<Rc<From>, CubeError>;
}

pub struct MergeProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, Merge> for MergeProcessor<'a> {
    type PhysycalNode = Rc<From>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        merge: &Merge,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        if merge.is_empty() {
            return Ok(From::new_empty());
        }
        if let ([sole], []) = (merge.subqueries().as_slice(), merge.keys().as_slice()) {
            let source = SingleAliasedSource::new_from_table_reference(
                sole.name().clone(),
                context.get_multi_stage_schema(sole.name())?,
                None,
            );
            let select = SelectBuilder::new(From::new(FromSource::Single(source)))
                .build(self.builder.query_tools().clone(), SqlNodesFactory::new());
            return Ok(From::new_from_subselect(
                Rc::new(select),
                "fk_aggregate".to_string(),
            ));
        }
        let strategy: Rc<dyn MergeStrategy> = if !merge.keys().is_empty() {
            // JOIN-model: keys side comes from the logical plan.
            // `KeysMergeStrategy` handles both the explicit
            // `keys` shape and the derived-from-measure-refs shape.
            KeysMergeStrategy::new(self.builder)
        } else if !merge.schema().has_dimensions() {
            InnerJoinMergeStrategy::new(self.builder)
        } else if self.builder.supports_union_full_key_aggregate() && merge.subqueries().len() > 1 {
            UnionMergeStrategy::new(self.builder)
        } else if self.builder.templates().supports_full_join() {
            FullJoinMergeStrategy::new(self.builder)
        } else {
            KeysMergeStrategy::new(self.builder)
        };
        strategy.process(merge, context)
    }
}

impl ProcessableNode for Merge {
    type ProcessorType<'a> = MergeProcessor<'a>;
}
