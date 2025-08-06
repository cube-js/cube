use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::KeysSubQuery;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{Select, SelectBuilder};
use crate::planner::sql_evaluator::ReferencesBuilder;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct KeysSubQueryProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, KeysSubQuery> for KeysSubQueryProcessor<'a> {
    type PhysycalNode = Rc<Select>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        keys_subquery: &KeysSubQuery,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let query_tools = self.builder.query_tools();
        let mut render_references = HashMap::new();
        let alias_prefix = Some(format!(
            "{}_key",
            query_tools.alias_for_cube(&keys_subquery.pk_cube.cube.name())?
        ));

        let mut context = context.clone();
        context.alias_prefix = alias_prefix;

        let source = self
            .builder
            .process_node(keys_subquery.source.as_ref(), &context)?;

        let references_builder = ReferencesBuilder::new(source.clone());
        let mut select_builder = SelectBuilder::new(source);
        self.builder.resolve_subquery_dimensions_references(
            &keys_subquery.source.dimension_subqueries,
            &references_builder,
            &mut render_references,
        )?;
        for member in keys_subquery
            .schema
            .all_dimensions()
            .chain(keys_subquery.primary_keys_dimensions.iter())
        {
            let alias = member.alias();
            select_builder.add_projection_member(member, Some(alias));
        }

        select_builder.set_distinct();
        select_builder.set_filter(keys_subquery.filter.all_filters());
        let mut context_factory = context.make_sql_nodes_factory()?;
        context_factory.set_render_references(render_references);
        let res = Rc::new(select_builder.build(query_tools.clone(), context_factory));
        Ok(res)
    }
}

impl ProcessableNode for KeysSubQuery {
    type ProcessorType<'a> = KeysSubQueryProcessor<'a>;
}
