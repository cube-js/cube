use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{GridLookup, LookupLeg, SubqueryBody};
use crate::physical_plan::sql_nodes::SqlNodesFactory;
use crate::physical_plan::{
    Expr, From, FromSource, JoinBuilder, JoinCondition, QualifiedColumnName, Select, SelectBuilder,
    SingleAliasedSource, SourceInputs,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct GridLookupProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, GridLookup> for GridLookupProcessor<'a> {
    type PhysycalNode = Rc<From>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        lookup: &GridLookup,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let grid = context.get_inline_subquery(lookup.grid())?;
        let SubqueryBody::KeysGrid(grid) = &grid.body else {
            return Err(CubeError::internal(format!(
                "Subquery {} read as a keys grid is not one",
                grid.name
            )));
        };
        let grid_select = self.builder.process_node(grid.as_ref(), context)?;
        build_grid_lookup(
            self.builder,
            grid_select,
            SourceInputs::reading(lookup.grid().id()),
            lookup.legs(),
            context,
        )
    }
}

// The `grid` rows with each leg left-joined by its `on` keys. The grid answers
// references to `grid_inputs`, each leg to its own subquery.
pub(super) fn build_grid_lookup(
    builder: &PhysicalPlanBuilder,
    grid_select: Rc<Select>,
    grid_inputs: SourceInputs,
    legs: &[LookupLeg],
    context: &PushDownBuilderContext,
) -> Result<Rc<From>, CubeError> {
    let query_tools = builder.query_tools();
    let grid_alias = "fk_aggregate_keys".to_string();
    let mut join_builder = JoinBuilder::new(
        SingleAliasedSource::new_from_subselect(grid_select.clone(), grid_alias.clone())
            .with_inputs(grid_inputs),
    );

    for (idx, leg) in legs.iter().enumerate() {
        let source = SingleAliasedSource::new_from_table_reference(
            leg.subquery().name().clone(),
            context.get_cte_schema(leg.subquery())?,
            None,
        );
        let leg_select = Rc::new(
            SelectBuilder::new(From::new(FromSource::Single(source)))
                .build(query_tools.clone(), SqlNodesFactory::new()),
        );
        let leg_alias = format!("q_{}", idx);
        let conditions = leg
            .on()
            .iter()
            .map(|dim| {
                let grid_ref = Expr::Reference(QualifiedColumnName::new(
                    Some(grid_alias.clone()),
                    grid_select.schema().resolve_member_alias(dim),
                ));
                let leg_ref = Expr::Reference(QualifiedColumnName::new(
                    Some(leg_alias.clone()),
                    leg_select.schema().resolve_member_alias(dim),
                ));
                vec![(grid_ref, leg_ref)]
            })
            .collect();
        // Null-safe, unconditionally: nothing rules a NULL key out, and a plain
        // `=` would keep such a grid row while dropping its value.
        join_builder.left_join_aliased_source(
            SingleAliasedSource::new_from_subselect(leg_select, leg_alias)
                .with_inputs(SourceInputs::reading(leg.subquery().id())),
            JoinCondition::new_dimension_join(conditions, true),
        );
    }

    Ok(From::new_from_join(join_builder.build()))
}

impl ProcessableNode for GridLookup {
    type ProcessorType<'a> = GridLookupProcessor<'a>;
}
