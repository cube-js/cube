use super::OrderPlanner;
use crate::plan::{
    Cte, Expr, Filter, From, JoinBuilder, JoinCondition, QualifiedColumnName, Select, SelectBuilder,
};
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_evaluator::ReferencesBuilder;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::BaseMeasure;
use crate::planner::BaseMemberHelper;
use crate::planner::QueryProperties;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

pub struct FullKeyAggregateQueryPlanner {
    query_properties: Rc<QueryProperties>,
    order_planner: OrderPlanner,
    context_factory: SqlNodesFactory,
    plan_sql_templates: PlanSqlTemplates,
}

impl FullKeyAggregateQueryPlanner {
    pub fn new(
        query_properties: Rc<QueryProperties>,
        context_factory: SqlNodesFactory,
        // TODO get rid of this dependency
        plan_sql_templates: PlanSqlTemplates,
    ) -> Self {
        Self {
            order_planner: OrderPlanner::new(query_properties.clone()),
            query_properties,
            context_factory,
            plan_sql_templates,
        }
    }

    pub fn plan(self, joins: Vec<Rc<Select>>, ctes: Vec<Rc<Cte>>) -> Result<Rc<Select>, CubeError> {
        if self.query_properties.is_simple_query()? {
            return Err(CubeError::internal(format!(
                "FullKeyAggregateQueryPlanner should not be used for simple query"
            )));
        }

        let aggregate = self.outer_measures_join_full_key_aggregate(
            &self.query_properties.measures(),
            joins,
            ctes,
        )?;

        Ok(aggregate)
    }

    fn outer_measures_join_full_key_aggregate(
        &self,
        outer_measures: &Vec<Rc<BaseMeasure>>,
        joins: Vec<Rc<Select>>,
        ctes: Vec<Rc<Cte>>,
    ) -> Result<Rc<Select>, CubeError> {
        let mut join_builder = JoinBuilder::new_from_subselect(joins[0].clone(), format!("q_0"));
        let dimensions_to_select = self.query_properties.dimensions_for_select();
        for (i, join) in joins.iter().enumerate().skip(1) {
            let right_alias = format!("q_{}", i);
            let left_schema = joins[i - 1].schema();
            let right_schema = joins[i].schema();
            // TODO every next join should join to all previous dimensions through OR: q_0.a = q_1.a, q_0.a = q_2.a OR q_1.a = q_2.a, ...
            let conditions = dimensions_to_select
                .iter()
                .map(|dim| {
                    (0..i)
                        .map(|left_i| {
                            let left_alias = format!("q_{}", left_i);
                            let alias_in_left_query = left_schema.resolve_member_alias(dim);
                            let left_ref = Expr::Reference(QualifiedColumnName::new(
                                Some(left_alias.clone()),
                                alias_in_left_query,
                            ));
                            let alias_in_right_query = right_schema.resolve_member_alias(dim);
                            let right_ref = Expr::Reference(QualifiedColumnName::new(
                                Some(right_alias.clone()),
                                alias_in_right_query,
                            ));
                            (left_ref, right_ref)
                        })
                        .collect::<Vec<_>>()
                })
                .collect_vec();
            let on = JoinCondition::new_dimension_join(conditions, true);
            let next_alias = format!("q_{}", i);
            if self.plan_sql_templates.supports_full_join() {
                join_builder.full_join_subselect(join.clone(), next_alias, on);
            } else {
                // TODO in case of full join is not supported there should be correct blending query that keeps NULL values
                join_builder.inner_join_subselect(join.clone(), next_alias, on);
            }
        }

        let from = From::new_from_join(join_builder.build());
        let references_builder = ReferencesBuilder::new(from.clone());
        let mut render_references = HashMap::new();

        let mut select_builder = SelectBuilder::new(from.clone());

        let dimensions_source = Some(format!("q_0"));
        for member in dimensions_to_select.iter() {
            references_builder.resolve_references_for_member(
                member.member_evaluator(),
                &dimensions_source,
                &mut render_references,
            )?;
            let references = (0..joins.len())
                .map(|i| {
                    let alias = format!("q_{}", i);
                    references_builder
                        .find_reference_for_member(
                            &member.member_evaluator().full_name(),
                            &Some(alias.clone()),
                        )
                        .ok_or_else(|| {
                            CubeError::internal(format!(
                                "Reference for join not found for {} in {}",
                                member.member_evaluator().full_name(),
                                alias
                            ))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let alias = references_builder
                .resolve_alias_for_member(&member.full_name(), &dimensions_source);
            select_builder.add_projection_coalesce_member(member, references, alias);
        }

        for member in BaseMemberHelper::iter_as_base_member(&outer_measures) {
            references_builder.resolve_references_for_member(
                member.member_evaluator(),
                &None,
                &mut render_references,
            )?;
            let alias = references_builder.resolve_alias_for_member(&member.full_name(), &None);
            select_builder.add_projection_member(&member, alias);
        }

        let having = if self.query_properties.measures_filters().is_empty() {
            None
        } else {
            let filter = Filter {
                items: self.query_properties.measures_filters().clone(),
            };
            references_builder.resolve_references_for_filter(&filter, &mut render_references)?;
            Some(filter)
        };

        select_builder.set_order_by(self.order_planner.default_order());
        select_builder.set_filter(having);
        select_builder.set_limit(self.query_properties.row_limit());
        select_builder.set_offset(self.query_properties.offset());
        if !ctes.is_empty() {
            select_builder.set_ctes(ctes.clone());
        }

        let mut context_factory = self.context_factory.clone();
        context_factory.set_render_references(render_references);

        Ok(Rc::new(select_builder.build(context_factory)))
    }
}
