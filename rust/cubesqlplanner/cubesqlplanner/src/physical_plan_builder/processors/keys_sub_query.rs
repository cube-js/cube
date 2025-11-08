use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{all_symbols, KeysSubQuery};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{CalcGroupItem, CalcGroupsJoin, From, Select, SelectBuilder};
use crate::planner::sql_evaluator::collectors::collect_calc_group_dims_from_nodes;
use crate::planner::sql_evaluator::{get_filtered_values, ReferencesBuilder};
use cubenativeutils::CubeError;
use itertools::Itertools as _;
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

        //FIXME duplication with QueryProcessor
        let all_symbols = all_symbols(&keys_subquery.schema(), &keys_subquery.filter());
        let calc_group_dims = collect_calc_group_dims_from_nodes(all_symbols.iter())?;

        let filter = keys_subquery.filter().all_filters();
        let calc_groups_items = calc_group_dims.into_iter().map(|dim| {
            let values = get_filtered_values(&dim, &filter);
            CalcGroupItem {
                symbol: dim,
                values,
            }
        });
        for item in calc_groups_items
            .clone()
            .filter(|itm| itm.values.len() == 1)
        {
            context_factory.add_render_reference(item.symbol.full_name(), item.values[0].clone());
        }
        let calc_groups_to_join = calc_groups_items
            .filter(|itm| itm.values.len() > 1)
            .collect_vec();
        let source = if calc_groups_to_join.is_empty() {
            source
        } else {
            let groups_join = CalcGroupsJoin::try_new(source, calc_groups_to_join)?;
            From::new_from_calc_groups_join(groups_join)
        };

        let references_builder = ReferencesBuilder::new(source.clone());
        let mut select_builder = SelectBuilder::new(source);
        self.builder.resolve_subquery_dimensions_references(
            &keys_subquery.source().dimension_subqueries(),
            &references_builder,
            &mut context_factory,
        )?;
        for member in keys_subquery.schema().all_dimensions() {
            let alias = member.alias();
            references_builder.resolve_references_for_member(
                member.clone(),
                &None,
                context_factory.render_references_mut(),
            )?;
            select_builder.add_projection_member(member, Some(alias));
        }

        if !context.dimensions_query {
            for member in keys_subquery.primary_keys_dimensions().iter() {
                let alias = member.alias();
                references_builder.resolve_references_for_member(
                    member.clone(),
                    &None,
                    context_factory.render_references_mut(),
                )?;
                select_builder.add_projection_member(member, Some(alias));
            }
        }

        select_builder.set_distinct();
        select_builder.set_filter(filter);
        let res = Rc::new(select_builder.build(query_tools.clone(), context_factory));
        Ok(res)
    }
}

impl ProcessableNode for KeysSubQuery {
    type ProcessorType<'a> = KeysSubQueryProcessor<'a>;
}
