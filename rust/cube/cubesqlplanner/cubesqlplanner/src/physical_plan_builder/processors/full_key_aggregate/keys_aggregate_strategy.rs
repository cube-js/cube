use super::FullKeyAggregateStrategy;
use crate::logical_plan::{FullKeyAggregate, LogicalJoin};
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
use std::collections::HashSet;
use std::rc::Rc;

pub(super) struct KeysFullKeyAggregateStrategy<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> KeysFullKeyAggregateStrategy<'a> {
    pub fn new(builder: &'a PhysicalPlanBuilder) -> Rc<Self> {
        Rc::new(Self { builder })
    }
}

impl FullKeyAggregateStrategy for KeysFullKeyAggregateStrategy<'_> {
    fn process(
        &self,
        full_key_aggregate: &FullKeyAggregate,
        context: &PushDownBuilderContext,
    ) -> Result<Rc<From>, CubeError> {
        let query_tools = self.builder.query_tools();
        let mut keys_queries = vec![];
        let mut data_queries = vec![];
        let mut keys_context = context.clone();
        keys_context.dimensions_query = true;
        let has_explicit_keys = full_key_aggregate.keys_subquery_ref().is_some();

        let join_keys: Vec<Rc<MemberSymbol>> = if full_key_aggregate.join_keys().is_empty() {
            full_key_aggregate
                .schema()
                .all_dimensions()
                .cloned()
                .collect()
        } else {
            full_key_aggregate.join_keys().clone()
        };
        // keys_select must expose both outer dimensions (so the outer
        // SELECT can resolve them via ReferencesBuilder from the FROM
        // chain) AND join_keys (so the JOIN ON clause can resolve their
        // aliases in `keys_select.schema()`). They overlap in the
        // multi-stage flow but diverge when join_keys are pk dimensions.
        let projection_members: Vec<Rc<MemberSymbol>> = {
            let mut members: Vec<Rc<MemberSymbol>> = full_key_aggregate
                .schema()
                .all_dimensions()
                .cloned()
                .collect();
            let mut seen: HashSet<String> = members.iter().map(|m| m.full_name()).collect();
            for jk in join_keys.iter() {
                if seen.insert(jk.full_name()) {
                    members.push(jk.clone());
                }
            }
            members
        };

        for cte_ref in full_key_aggregate.data_inputs().iter() {
            let cte_schema = context.get_cte_schema(cte_ref.name())?;
            let cte_source = SingleAliasedSource::new_from_table_reference(
                cte_ref.name().clone(),
                cte_schema.clone(),
                None,
            );
            if !has_explicit_keys {
                let mut keys_select_builder =
                    SelectBuilder::new(From::new(FromSource::Single(cte_source.clone())));
                for member in projection_members.iter() {
                    let alias = cte_schema.resolve_member_alias(member);
                    let reference = QualifiedColumnName::new(None, alias);
                    keys_select_builder.add_projection_member_reference(member, reference);
                }
                let sql_context = SqlNodesFactory::new();
                keys_select_builder.set_distinct();
                let keys_select =
                    Rc::new(keys_select_builder.build(query_tools.clone(), sql_context));
                keys_queries.push(keys_select);
            }

            let data_select_builder = SelectBuilder::new(From::new(FromSource::Single(cte_source)));
            let data_select =
                Rc::new(data_select_builder.build(query_tools.clone(), SqlNodesFactory::new()));
            data_queries.push(data_select);
        }
        if data_queries.is_empty() {
            let empty_join = LogicalJoin::builder().build();
            return self.builder.process_node(&empty_join, context);
        }

        if !has_explicit_keys && data_queries.len() == 1 {
            let select = data_queries[0].clone();
            let result = From::new_from_subselect(select, "fk_aggregate".to_string());
            return Ok(result);
        }

        let keys_select = if let Some(keys_ref) = full_key_aggregate.keys_subquery_ref() {
            let schema = context.get_cte_schema(keys_ref.name())?;
            let source = SingleAliasedSource::new_from_table_reference(
                keys_ref.name().clone(),
                schema.clone(),
                None,
            );
            let mut select_builder = SelectBuilder::new(From::new(FromSource::Single(source)));
            for member in projection_members.iter() {
                let alias = schema.resolve_member_alias(member);
                let reference = QualifiedColumnName::new(None, alias);
                select_builder.add_projection_member_reference(member, reference);
            }
            Rc::new(select_builder.build(query_tools.clone(), SqlNodesFactory::new()))
        } else {
            let keys_from = From::new_from_union(
                Rc::new(Union::new_from_subselects(&keys_queries)),
                "pk_aggregate_keys_source".to_string(),
            );
            let references_builder = ReferencesBuilder::new(keys_from.clone());
            let mut keys_select_builder = SelectBuilder::new(keys_from);

            for member in projection_members.iter() {
                let alias = references_builder.resolve_alias_for_member(member, &None);
                if alias.is_none() {
                    return Err(CubeError::internal(format!(
                        "Source for {} not found in full key aggregate subqueries",
                        member.full_name()
                    )));
                }
                let reference = QualifiedColumnName::new(None, alias.unwrap());
                keys_select_builder.add_projection_member_reference(member, reference);
            }
            keys_select_builder.set_distinct();
            Rc::new(keys_select_builder.build(query_tools.clone(), SqlNodesFactory::new()))
        };

        let keys_alias = "fk_aggregate_keys".to_string();

        let mut join_builder =
            JoinBuilder::new_from_subselect(keys_select.clone(), keys_alias.clone());

        for (i, query) in data_queries.into_iter().enumerate() {
            let query_alias = format!("q_{}", i);
            let conditions = join_keys
                .iter()
                .map(|dim| -> Result<_, CubeError> {
                    let alias_in_keys_query = keys_select.schema().resolve_member_alias(dim);
                    let keys_query_ref = Expr::Reference(QualifiedColumnName::new(
                        Some(keys_alias.clone()),
                        alias_in_keys_query,
                    ));
                    let alias_in_data_query = query.schema().resolve_member_alias(dim);
                    let data_query_ref = Expr::Reference(QualifiedColumnName::new(
                        Some(query_alias.clone()),
                        alias_in_data_query,
                    ));

                    Ok(vec![(keys_query_ref, data_query_ref)])
                })
                .collect::<Result<Vec<_>, _>>()?;

            join_builder.left_join_subselect(
                query,
                query_alias.clone(),
                JoinCondition::new_dimension_join(conditions, true),
            );
        }

        let result = join_builder.build();
        Ok(From::new_from_join(result))
    }
}
