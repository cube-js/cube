use super::MergeStrategy;
use crate::logical_plan::Merge;
use crate::physical_plan::sql_nodes::SqlNodesFactory;
use crate::physical_plan::{
    Expr, From, FromSource, JoinBuilder, JoinCondition, QualifiedColumnName, SelectBuilder,
    SingleAliasedSource,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::physical_plan_builder::PushDownBuilderContext;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub(super) struct InnerJoinMergeStrategy<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> InnerJoinMergeStrategy<'a> {
    pub fn new(builder: &'a PhysicalPlanBuilder) -> Rc<Self> {
        Rc::new(Self { builder })
    }
}

impl MergeStrategy for InnerJoinMergeStrategy<'_> {
    fn process(
        &self,
        merge: &Merge,
        context: &PushDownBuilderContext,
    ) -> Result<Rc<From>, CubeError> {
        let query_tools = self.builder.query_tools();
        let mut data_queries = vec![];

        for multi_stage_ref in merge.subqueries().iter() {
            let multi_stage_schema = context.get_multi_stage_schema(multi_stage_ref.name())?;
            let multi_stage_source = SingleAliasedSource::new_from_table_reference(
                multi_stage_ref.name().clone(),
                multi_stage_schema.clone(),
                None,
            );
            let sql_context = SqlNodesFactory::new();

            let data_select_builder =
                SelectBuilder::new(From::new(FromSource::Single(multi_stage_source)));
            let data_select = Rc::new(data_select_builder.build(query_tools.clone(), sql_context));
            data_queries.push(data_select);
        }

        let mut join_builder =
            JoinBuilder::new_from_subselect(data_queries[0].clone(), "q_0".to_string());

        for (i, query) in data_queries.iter().skip(1).enumerate() {
            let prev_alias = format!("q_{}", i);
            let query_alias = format!("q_{}", i + 1);
            let conditions = merge
                .schema()
                .all_dimensions()
                .map(|dim| -> Result<_, CubeError> {
                    let alias_in_prev_query = data_queries[i].schema().resolve_member_alias(dim);
                    let prev_query_ref = Expr::Reference(QualifiedColumnName::new(
                        Some(prev_alias.clone()),
                        alias_in_prev_query,
                    ));
                    let alias_in_data_query = query.schema().resolve_member_alias(dim);
                    let data_query_ref = Expr::Reference(QualifiedColumnName::new(
                        Some(query_alias.clone()),
                        alias_in_data_query,
                    ));

                    Ok(vec![(prev_query_ref, data_query_ref)])
                })
                .collect::<Result<Vec<_>, _>>()?;

            join_builder.inner_join_subselect(
                query.clone(),
                query_alias.clone(),
                JoinCondition::new_dimension_join(conditions, true),
            );
        }

        let result = join_builder.build();
        Ok(From::new_from_join(result))
    }
}
