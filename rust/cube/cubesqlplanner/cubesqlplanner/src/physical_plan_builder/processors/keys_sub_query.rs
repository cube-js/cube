use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::transforms as logical_transforms;
use crate::logical_plan::{all_symbols, KeysSubQuery};
use crate::physical_plan::symbols::column_ref_symbol::literal_reference;
use crate::physical_plan::{
    CalcGroupItem, CalcGroupsJoin, From, ReferenceSubstitutions, ReferencesBuilder, Select,
    SelectBuilder,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::collectors::collect_calc_group_dims_from_nodes;
use crate::planner::symbols::transforms;
use crate::planner::symbols::transforms::get_filtered_values;
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
        // A value pinned by the query is not read from the source, so it is
        // recorded before the walk below can map the dimension to a column.
        let mut substitutions = ReferenceSubstitutions::new();
        for item in calc_groups_items
            .clone()
            .filter(|itm| itm.values.len() == 1)
        {
            substitutions.insert(
                item.symbol.full_name(),
                literal_reference(&item.symbol, item.values[0].clone()),
            );
            // A join condition can name a calc-group dimension too, and it is
            // built before there is a select symbol environment to rewrite, so
            // it resolves the value while rendering.
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
        self.builder.collect_subquery_dimensions_substitutions(
            &keys_subquery.source().dimension_subqueries(),
            &references_builder,
            &mut substitutions,
            &mut context_factory,
        )?;
        for member in keys_subquery.schema().all_dimensions() {
            references_builder.collect_substitutions_for_member(
                member.clone(),
                &None,
                &mut substitutions,
            )?;
        }
        let primary_keys_dimensions = if context.dimensions_query {
            vec![]
        } else {
            // A primary key that is also a query dimension is already projected
            // above. Projecting it again would put two columns under one alias,
            // making every reference to it from the enclosing re-join ambiguous.
            // Symbols are matched the way `Schema::find_column_for_member`
            // matches them, so that the re-join resolves to the surviving
            // column.
            let members = keys_subquery
                .primary_keys_dimensions()
                .iter()
                .filter(|member| {
                    let resolved = (*member).clone().resolve_reference_chain();
                    !keys_subquery
                        .schema()
                        .all_dimensions()
                        .any(|dim| dim.clone().resolve_reference_chain() == resolved)
                })
                .cloned()
                .collect_vec();
            for member in members.iter() {
                references_builder.collect_substitutions_for_member(
                    member.clone(),
                    &None,
                    &mut substitutions,
                )?;
            }
            members
        };

        let schema = logical_transforms::substitute_symbols_in_schema(
            &keys_subquery.schema(),
            &substitutions,
        )?;
        let filter = logical_transforms::substitute_symbols_in_filter(filter, &substitutions)?;

        let mut select_builder = SelectBuilder::new(source);
        for member in schema.all_dimensions() {
            let alias = member.alias();
            select_builder.add_projection_member(member, Some(alias));
        }
        for member in primary_keys_dimensions.iter() {
            let member = transforms::substitute_by_name(member, &substitutions)?;
            let alias = member.alias();
            select_builder.add_projection_member(&member, Some(alias));
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
