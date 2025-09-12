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
        let alias_prefix = Some(format!(
            "{}_key",
            query_tools.alias_for_cube(&keys_subquery.pk_cube().cube().name())?
        ));

        let mut context = context.clone();
        context.alias_prefix = alias_prefix;

        let mut context_factory = context.make_sql_nodes_factory()?;
        let source = self
            .builder
            .process_node(keys_subquery.source().as_ref(), &context)?;

        let references_builder = ReferencesBuilder::new(source.clone());
        let mut select_builder = SelectBuilder::new(source);
        self.builder.resolve_subquery_dimensions_references(
            &keys_subquery.source().dimension_subqueries(),
            &references_builder,
            &mut context_factory,
        )?;
        for member in keys_subquery
            .schema()
            .all_dimensions()
            .chain(keys_subquery.primary_keys_dimensions().iter())
        {
            let alias = member.alias();
            self.builder.process_calc_group(
                member,
                &mut context_factory,
                &keys_subquery.filter().all_filters(),
            )?;
            references_builder.resolve_references_for_member(
                member.clone(),
                &None,
                context_factory.render_references_mut(),
            )?;
            select_builder.add_projection_member(member, Some(alias));
        }

        select_builder.set_distinct();
        select_builder.set_filter(keys_subquery.filter().all_filters());
        let res = Rc::new(select_builder.build(query_tools.clone(), context_factory));
        Ok(res)
    }
}

impl ProcessableNode for KeysSubQuery {
    type ProcessorType<'a> = KeysSubQueryProcessor<'a>;
}
