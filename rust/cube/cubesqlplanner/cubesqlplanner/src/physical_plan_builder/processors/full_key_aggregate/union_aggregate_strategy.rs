use super::FullKeyAggregateStrategy;
use crate::logical_plan::FullKeyAggregate;
use crate::physical_plan::sql_nodes::SqlNodesFactory;
use crate::physical_plan::{
    Expr, From, FromSource, QualifiedColumnName, SelectBuilder, SingleAliasedSource, Union,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::physical_plan_builder::PushDownBuilderContext;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

/// One `UNION ALL` branch per ref (own measures, NULL padding for the rest), grouped by
/// the key dims: a FULL JOIN on the dims, NULL keys included, with each ref read once.
/// Needs `GROUP_ANY` to skip NULLs and each ref to hold at most one row per key.
pub(super) struct UnionFullKeyAggregateStrategy<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> UnionFullKeyAggregateStrategy<'a> {
    pub fn new(builder: &'a PhysicalPlanBuilder) -> Rc<Self> {
        Rc::new(Self { builder })
    }
}

impl FullKeyAggregateStrategy for UnionFullKeyAggregateStrategy<'_> {
    fn process(
        &self,
        full_key_aggregate: &FullKeyAggregate,
        context: &PushDownBuilderContext,
    ) -> Result<Rc<From>, CubeError> {
        let query_tools = self.builder.query_tools();
        let refs = full_key_aggregate.multi_stage_subquery_refs();
        debug_assert!(refs.len() > 1);

        let key_dims = full_key_aggregate
            .schema()
            .all_dimensions()
            .cloned()
            .collect_vec();
        let measures: Vec<Rc<MemberSymbol>> = refs
            .iter()
            .flat_map(|r| r.symbols().iter().cloned())
            .unique_by(|m| m.full_name())
            .collect();

        let mut branches = vec![];
        for multi_stage_ref in refs.iter() {
            let ref_schema = context.get_multi_stage_schema(multi_stage_ref.name())?;
            let source = SingleAliasedSource::new_from_table_reference(
                multi_stage_ref.name().clone(),
                ref_schema.clone(),
                None,
            );
            let mut select_builder = SelectBuilder::new(From::new(FromSource::Single(source)));
            for dim in key_dims.iter() {
                select_builder.add_projection_reference_member(
                    dim,
                    QualifiedColumnName::new(None, ref_schema.resolve_member_alias(dim)),
                    Some(dim.alias()),
                );
            }
            for measure in measures.iter() {
                let own = multi_stage_ref
                    .symbols()
                    .iter()
                    .any(|m| m.full_name() == measure.full_name());
                if own {
                    select_builder.add_projection_reference_member(
                        measure,
                        QualifiedColumnName::new(None, ref_schema.resolve_member_alias(measure)),
                        Some(measure.alias()),
                    );
                } else {
                    select_builder.add_union_padding_null_projection(measure, measure.alias());
                }
            }
            branches.push(Rc::new(
                select_builder.build(query_tools.clone(), SqlNodesFactory::new()),
            ));
        }

        let union_from = From::new_from_union(
            Rc::new(Union::new_from_subselects(&branches)),
            "fk_aggregate_source".to_string(),
        );
        let mut select_builder = SelectBuilder::new(union_from);
        let mut group_by = vec![];
        for dim in key_dims.iter() {
            let reference = QualifiedColumnName::new(None, dim.alias());
            select_builder.add_projection_member_reference(dim, reference.clone());
            group_by.push(Expr::Reference(reference));
        }
        for measure in measures.iter() {
            select_builder.add_projection_group_any_member(
                measure,
                QualifiedColumnName::new(None, measure.alias()),
            );
        }
        select_builder.set_group_by(group_by);
        let select = Rc::new(select_builder.build(query_tools.clone(), SqlNodesFactory::new()));
        Ok(From::new_from_subselect(select, "fk_aggregate".to_string()))
    }
}
