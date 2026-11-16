use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{KeysGrid, SubqueryRef};
use crate::physical_plan::sql_nodes::SqlNodesFactory;
use crate::physical_plan::{
    From, FromSource, QualifiedColumnName, Select, SelectBuilder, SingleAliasedSource, Union,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct KeysGridProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, KeysGrid> for KeysGridProcessor<'a> {
    type PhysycalNode = Rc<Select>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        grid: &KeysGrid,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let key_dims = grid.schema().all_dimensions().cloned().collect::<Vec<_>>();
        build_keys_grid(self.builder, &key_dims, grid.sources(), context)
    }
}

// The distinct `key_dims` rows of `sources`.
pub(super) fn build_keys_grid(
    builder: &PhysicalPlanBuilder,
    key_dims: &[Rc<MemberSymbol>],
    sources: &[Rc<SubqueryRef>],
    context: &PushDownBuilderContext,
) -> Result<Rc<Select>, CubeError> {
    let query_tools = builder.query_tools();
    let mut projections = vec![];
    for source in sources.iter() {
        let schema = context.get_cte_schema(source)?;
        let from = From::new(FromSource::Single(
            SingleAliasedSource::new_from_table_reference(
                source.name().clone(),
                schema.clone(),
                None,
            ),
        ));
        let mut select_builder = SelectBuilder::new(from);
        for dim in key_dims.iter() {
            let alias = schema.resolve_member_alias(dim);
            select_builder
                .add_projection_member_reference(dim, QualifiedColumnName::new(None, alias));
        }
        select_builder.set_distinct();
        projections.push(Rc::new(
            select_builder.build(query_tools.clone(), SqlNodesFactory::new()),
        ));
    }
    if projections.len() == 1 {
        return Ok(projections.remove(0));
    }

    let union = Rc::new(Union::new_from_subselects(&projections));
    let union_schema = union.schema();
    let mut outer = SelectBuilder::new(From::new_from_union(
        union,
        "fk_aggregate_keys_source".to_string(),
    ));
    for dim in key_dims.iter() {
        let alias = union_schema.resolve_member_reference(dim).ok_or_else(|| {
            CubeError::internal(format!(
                "Source for {} not found in the keys grid sources",
                dim.full_name()
            ))
        })?;
        outer.add_projection_member_reference(dim, QualifiedColumnName::new(None, alias));
    }
    outer.set_distinct();
    Ok(Rc::new(
        outer.build(query_tools.clone(), SqlNodesFactory::new()),
    ))
}

impl ProcessableNode for KeysGrid {
    type ProcessorType<'a> = KeysGridProcessor<'a>;
}
