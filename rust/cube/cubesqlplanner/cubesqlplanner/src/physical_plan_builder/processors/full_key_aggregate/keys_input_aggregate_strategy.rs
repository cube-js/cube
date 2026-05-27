use super::FullKeyAggregateStrategy;
use crate::logical_plan::{FullKeyAggregate, FullKeyAggregateKeysInput, LogicalJoin};
use crate::physical_plan::sql_nodes::SqlNodesFactory;
use crate::physical_plan::{
    Expr, From, FromSource, JoinBuilder, JoinCondition, QualifiedColumnName, SelectBuilder,
    SingleAliasedSource,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::physical_plan_builder::PushDownBuilderContext;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Strategy for the JOIN-based assembly: the planner has supplied an
/// explicit keys-side source (`keys_input`) carrying the full output
/// grain, and measure-side refs (`multi_stage_subquery_refs`) live at
/// partition grain. We project keys-side as the outer source and LEFT
/// JOIN each measure-side ref by the dimensions present in its schema
/// — i.e. the partition grain of that measure.
pub(super) struct KeysInputFullKeyAggregateStrategy<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> KeysInputFullKeyAggregateStrategy<'a> {
    pub fn new(builder: &'a PhysicalPlanBuilder) -> Rc<Self> {
        Rc::new(Self { builder })
    }

    fn dim_in_schema(
        schema: &crate::logical_plan::LogicalSchema,
        keys_member: &Rc<MemberSymbol>,
    ) -> bool {
        let target = keys_member.clone().resolve_reference_chain().full_name();
        schema
            .all_dimensions()
            .any(|d| d.clone().resolve_reference_chain().full_name() == target)
    }
}

impl FullKeyAggregateStrategy for KeysInputFullKeyAggregateStrategy<'_> {
    fn process(
        &self,
        full_key_aggregate: &FullKeyAggregate,
        context: &PushDownBuilderContext,
    ) -> Result<Rc<From>, CubeError> {
        let query_tools = self.builder.query_tools();
        let keys_input: &FullKeyAggregateKeysInput = full_key_aggregate
            .keys_input()
            .map(|k| k.as_ref())
            .ok_or_else(|| {
                CubeError::internal(
                    "KeysInputFullKeyAggregateStrategy invoked without keys_input".to_string(),
                )
            })?;

        if keys_input.refs().is_empty() {
            // Nothing to drive the keys grid — fall back to an empty join.
            let empty_join = LogicalJoin::builder().build();
            return self.builder.process_node(&empty_join, context);
        }

        // Build the keys-side source. For now we use the first ref; multi-ref
        // unioning will follow when multi-fact reduce_by lands.
        let keys_ref = &keys_input.refs()[0];
        let keys_schema = context.get_multi_stage_schema(keys_ref.name())?;
        let keys_source = SingleAliasedSource::new_from_table_reference(
            keys_ref.name().clone(),
            keys_schema.clone(),
            None,
        );

        let mut keys_select_builder =
            SelectBuilder::new(From::new(FromSource::Single(keys_source)));
        // Project the full leaf grain (not just keys_input.keys()) so JOIN
        // conditions against measure-side partition dims have all their
        // columns visible. keys_input.keys is the *output* grain of the FKA
        // (used downstream), and is a subset of the leaf grain.
        for member in keys_ref.schema().all_dimensions() {
            let alias = keys_schema.resolve_member_alias(member);
            keys_select_builder
                .add_projection_member_reference(member, QualifiedColumnName::new(None, alias));
        }
        let keys_select =
            Rc::new(keys_select_builder.build(query_tools.clone(), SqlNodesFactory::new()));

        let keys_alias = "fk_aggregate_keys".to_string();
        let mut join_builder =
            JoinBuilder::new_from_subselect(keys_select.clone(), keys_alias.clone());

        for (idx, measure_ref) in full_key_aggregate
            .multi_stage_subquery_refs()
            .iter()
            .enumerate()
        {
            let measure_schema = context.get_multi_stage_schema(measure_ref.name())?;
            let measure_source = SingleAliasedSource::new_from_table_reference(
                measure_ref.name().clone(),
                measure_schema.clone(),
                None,
            );
            let measure_select = Rc::new(
                SelectBuilder::new(From::new(FromSource::Single(measure_source)))
                    .build(query_tools.clone(), SqlNodesFactory::new()),
            );
            let measure_alias = format!("q_m_{}", idx);

            // JOIN-keys: intersection of keys-ref schema with this measure
            // ref's schema. keys-ref carries the full leaf grain; measure-ref
            // carries the partition grain, so the intersection equals the
            // partition grain — every measure row matches exactly one keys
            // row, no duplication.
            let keys_logical_schema = keys_ref.schema().clone();
            let measure_logical_schema = measure_ref.schema().clone();
            let conditions = keys_logical_schema
                .all_dimensions()
                .filter(|k| Self::dim_in_schema(measure_logical_schema.as_ref(), k))
                .map(|dim| -> Result<_, CubeError> {
                    let alias_in_keys = keys_select.schema().resolve_member_alias(dim);
                    let keys_ref_expr = Expr::Reference(QualifiedColumnName::new(
                        Some(keys_alias.clone()),
                        alias_in_keys,
                    ));
                    let alias_in_measure = measure_select.schema().resolve_member_alias(dim);
                    let measure_ref_expr = Expr::Reference(QualifiedColumnName::new(
                        Some(measure_alias.clone()),
                        alias_in_measure,
                    ));
                    Ok(vec![(keys_ref_expr, measure_ref_expr)])
                })
                .collect::<Result<Vec<_>, _>>()?;

            join_builder.left_join_subselect(
                measure_select,
                measure_alias,
                JoinCondition::new_dimension_join(conditions, false),
            );
        }

        Ok(From::new_from_join(join_builder.build()))
    }
}
