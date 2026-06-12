use super::FullKeyAggregateStrategy;
use crate::logical_plan::{FullKeyAggregate, LogicalJoin, MultiStageSubqueryRef};
use crate::physical_plan::sql_nodes::SqlNodesFactory;
use crate::physical_plan::ReferencesBuilder;
use crate::physical_plan::{
    Expr, From, FromSource, JoinBuilder, JoinCondition, QualifiedColumnName, SelectBuilder,
    SingleAliasedSource, Union,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::physical_plan_builder::PushDownBuilderContext;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Keys-based assembly: build a `UNION DISTINCT` of dim projections over the
/// keys-side refs, then LEFT JOIN each measure-side ref by the dims its
/// schema actually carries. Two modes:
///   - keys-side comes from an explicit `keys_input` (JOIN-model: measure
///     refs live at partition grain, keys refs at leaf grain);
///   - keys-side derived from the measure refs themselves (chosen when the
///     dialect lacks FULL JOIN and the logical plan carries no `keys_input`).
pub(super) struct KeysFullKeyAggregateStrategy<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> KeysFullKeyAggregateStrategy<'a> {
    pub fn new(builder: &'a PhysicalPlanBuilder) -> Rc<Self> {
        Rc::new(Self { builder })
    }

    fn dim_in_schema(
        schema: &crate::logical_plan::LogicalSchema,
        member: &Rc<MemberSymbol>,
    ) -> bool {
        let target = member.clone().resolve_reference_chain().full_name();
        schema
            .all_dimensions()
            .any(|d| d.clone().resolve_reference_chain().full_name() == target)
    }
}

impl FullKeyAggregateStrategy for KeysFullKeyAggregateStrategy<'_> {
    fn process(
        &self,
        full_key_aggregate: &FullKeyAggregate,
        context: &PushDownBuilderContext,
    ) -> Result<Rc<From>, CubeError> {
        let query_tools = self.builder.query_tools();

        // Decide the source for keys-side. Either an explicit set of refs
        // from the logical plan (JOIN-model), or derive from measure refs
        // themselves (no `keys_input` in the plan).
        let (keys_refs, has_explicit_keys): (Vec<Rc<MultiStageSubqueryRef>>, bool) =
            if let Some(keys_input) = full_key_aggregate.keys_input() {
                (keys_input.refs().clone(), true)
            } else {
                (
                    full_key_aggregate.multi_stage_subquery_refs().clone(),
                    false,
                )
            };

        // Dimensions projected on the keys-side. With explicit keys this is
        // the leaf grain (the refs' own schemas); otherwise it's the FKA
        // output grain (= query grain).
        let key_dims: Vec<Rc<MemberSymbol>> = if has_explicit_keys && !keys_refs.is_empty() {
            keys_refs[0].schema().all_dimensions().cloned().collect()
        } else {
            full_key_aggregate
                .schema()
                .all_dimensions()
                .cloned()
                .collect()
        };

        // Build a DISTINCT projection of `key_dims` over each keys ref.
        let mut keys_projections = vec![];
        for keys_ref in keys_refs.iter() {
            let ref_schema = context.get_multi_stage_schema(keys_ref.name())?;
            let ref_source = SingleAliasedSource::new_from_table_reference(
                keys_ref.name().clone(),
                ref_schema.clone(),
                None,
            );
            let mut select_builder = SelectBuilder::new(From::new(FromSource::Single(ref_source)));
            for dim in key_dims.iter() {
                let alias = ref_schema.resolve_member_alias(dim);
                select_builder
                    .add_projection_member_reference(dim, QualifiedColumnName::new(None, alias));
            }
            select_builder.set_distinct();
            keys_projections.push(Rc::new(
                select_builder.build(query_tools.clone(), SqlNodesFactory::new()),
            ));
        }

        // Build data (measure) sub-selects for each measure ref.
        let mut data_queries = vec![];
        for multi_stage_ref in full_key_aggregate.multi_stage_subquery_refs().iter() {
            let multi_stage_schema = context.get_multi_stage_schema(multi_stage_ref.name())?;
            let multi_stage_source = SingleAliasedSource::new_from_table_reference(
                multi_stage_ref.name().clone(),
                multi_stage_schema.clone(),
                None,
            );
            let data_select = Rc::new(
                SelectBuilder::new(From::new(FromSource::Single(multi_stage_source)))
                    .build(query_tools.clone(), SqlNodesFactory::new()),
            );
            data_queries.push((data_select, multi_stage_ref.schema().clone()));
        }

        if data_queries.is_empty() {
            let empty_join = LogicalJoin::builder().build();
            return self.builder.process_node(&empty_join, context);
        }

        // Fast path: single measure ref and no explicit keys side — the ref
        // already has full key coverage on its own, no need to UNION keys.
        if data_queries.len() == 1 && !has_explicit_keys {
            let (select, _) = data_queries.into_iter().next().unwrap();
            return Ok(From::new_from_subselect(select, "fk_aggregate".to_string()));
        }

        // Combine keys projections via UNION (DISTINCT inside each plus
        // UNION DISTINCT) and resolve canonical aliases.
        let keys_alias = "fk_aggregate_keys".to_string();
        let keys_select = if keys_projections.len() == 1 {
            keys_projections.into_iter().next().unwrap()
        } else {
            let keys_from = From::new_from_union(
                Rc::new(Union::new_from_subselects(&keys_projections)),
                "fk_aggregate_keys_source".to_string(),
            );
            let references_builder = ReferencesBuilder::new(keys_from.clone());
            let mut outer = SelectBuilder::new(keys_from);
            for dim in key_dims.iter() {
                let alias = references_builder
                    .resolve_alias_for_member(dim, &None)
                    .ok_or_else(|| {
                        CubeError::internal(format!(
                            "Source for {} not found in full key aggregate subqueries",
                            dim.full_name()
                        ))
                    })?;
                outer.add_projection_member_reference(dim, QualifiedColumnName::new(None, alias));
            }
            outer.set_distinct();
            Rc::new(outer.build(query_tools.clone(), SqlNodesFactory::new()))
        };

        let mut join_builder =
            JoinBuilder::new_from_subselect(keys_select.clone(), keys_alias.clone());

        for (idx, (query, query_logical_schema)) in data_queries.into_iter().enumerate() {
            let query_alias = format!("q_{}", idx);
            // JOIN-keys are the dims present in both sides. With explicit
            // keys-side this collapses to the measure ref's partition grain;
            // without it the measure ref already has all key_dims.
            let conditions = key_dims
                .iter()
                .filter(|d| Self::dim_in_schema(query_logical_schema.as_ref(), d))
                .map(|dim| -> Result<_, CubeError> {
                    let alias_in_keys = keys_select.schema().resolve_member_alias(dim);
                    let keys_ref_expr = Expr::Reference(QualifiedColumnName::new(
                        Some(keys_alias.clone()),
                        alias_in_keys,
                    ));
                    let alias_in_data = query.schema().resolve_member_alias(dim);
                    let data_ref_expr = Expr::Reference(QualifiedColumnName::new(
                        Some(query_alias.clone()),
                        alias_in_data,
                    ));
                    Ok(vec![(keys_ref_expr, data_ref_expr)])
                })
                .collect::<Result<Vec<_>, _>>()?;

            // Null-safe dimension join when keys are derived from the
            // measure refs (FULL JOIN-equivalent shape). For the JOIN-model
            // path (explicit keys) it's a one-to-one match — null-safety is
            // unnecessary but stays correct.
            join_builder.left_join_subselect(
                query,
                query_alias,
                JoinCondition::new_dimension_join(conditions, !has_explicit_keys),
            );
        }

        Ok(From::new_from_join(join_builder.build()))
    }
}
