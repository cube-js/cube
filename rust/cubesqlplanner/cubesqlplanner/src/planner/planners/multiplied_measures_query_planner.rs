use super::{CommonUtils, JoinPlanner};
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::plan::{
    Expr, From, JoinBuilder, JoinCondition, MemberExpression, QualifiedColumnName, Select,
    SelectBuilder,
};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::{
    collect_cube_names, collect_join_hints, collect_join_hints_for_measures,
};
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_evaluator::ReferencesBuilder;
use crate::planner::{BaseMeasure, BaseMember, BaseMemberHelper, QueryProperties};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

pub struct MultipliedMeasuresQueryPlanner {
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
    join_planner: JoinPlanner,
    common_utils: CommonUtils,
    context_factory: SqlNodesFactory,
}

impl MultipliedMeasuresQueryPlanner {
    pub fn new(
        query_tools: Rc<QueryTools>,
        query_properties: Rc<QueryProperties>,
        context_factory: SqlNodesFactory,
    ) -> Self {
        Self {
            query_tools: query_tools.clone(),
            join_planner: JoinPlanner::new(query_tools.clone()),
            common_utils: CommonUtils::new(query_tools.clone()),
            query_properties,
            context_factory,
        }
    }

    pub fn plan_queries(&self) -> Result<Vec<Rc<Select>>, CubeError> {
        if self.query_properties.is_simple_query()? {
            return Err(CubeError::internal(format!(
                "MultipliedMeasuresQueryPlanner should not be used for simple query"
            )));
        }

        let measures = self.query_properties.full_key_aggregate_measures()?;

        let mut joins = Vec::new();

        if !measures.regular_measures.is_empty() {
            let join_multi_fact_groups = self
                .query_properties
                .compute_join_multi_fact_groups_with_measures(&measures.regular_measures)?;
            for (i, (join, measures)) in join_multi_fact_groups.iter().enumerate() {
                let regular_subquery = self.regular_measures_subquery(
                    measures,
                    join.clone(),
                    if i == 0 {
                        "main".to_string()
                    } else {
                        format!("main_{}", i)
                    },
                )?;
                joins.push(regular_subquery);
            }
        }

        for (cube_name, measures) in measures
            .multiplied_measures
            .clone()
            .into_iter()
            .into_group_map_by(|m| m.cube_name().clone())
        {
            let join_multi_fact_groups = self
                .query_properties
                .compute_join_multi_fact_groups_with_measures(&measures)?;
            if join_multi_fact_groups.len() != 1 {
                return Err(CubeError::internal(
                    format!(
                        "Expected just one multi-fact join group for aggregate measures but got multiple: {}",
                        join_multi_fact_groups.into_iter().map(|(_, measures)| format!("({})", measures.iter().map(|m| m.full_name()).join(", "))).join(", ")
                    )
                ));
            }
            let aggregate_subquery = self.aggregate_subquery(
                &cube_name,
                &measures,
                join_multi_fact_groups.into_iter().next().unwrap().0,
            )?;
            joins.push(aggregate_subquery);
        }
        Ok(joins)
    }

    fn aggregate_subquery(
        &self,
        key_cube_name: &String,
        measures: &Vec<Rc<BaseMeasure>>,
        key_join: Rc<dyn JoinDefinition>,
    ) -> Result<Rc<Select>, CubeError> {
        let primary_keys_dimensions = self.common_utils.primary_keys_dimensions(key_cube_name)?;
        let keys_query = self.key_query(&primary_keys_dimensions, key_join, key_cube_name)?;
        let keys_query_alias = format!("keys");
        let should_build_join_for_measure_select =
            self.check_should_build_join_for_measure_select(measures, key_cube_name)?;

        let mut join_builder =
            JoinBuilder::new_from_subselect(keys_query.clone(), keys_query_alias.clone());

        let pk_cube = self.common_utils.cube_from_path(key_cube_name.clone())?;
        let pk_cube_alias =
            pk_cube.default_alias_with_prefix(&Some(format!("{key_cube_name}_key")));
        let mut ungrouped_measure_references = HashMap::new();
        if should_build_join_for_measure_select {
            let subquery = self.aggregate_subquery_measure_join(
                key_cube_name,
                &measures,
                &primary_keys_dimensions,
            )?;

            let conditions = primary_keys_dimensions
                .iter()
                .map(|dim| {
                    let alias_in_keys_query = keys_query.schema().resolve_member_alias(dim);
                    let keys_query_ref = Expr::Reference(QualifiedColumnName::new(
                        Some(keys_query_alias.clone()),
                        alias_in_keys_query,
                    ));
                    let alias_in_subquery = subquery.schema().resolve_member_alias(dim);
                    let subquery_ref = Expr::Reference(QualifiedColumnName::new(
                        Some(pk_cube_alias.clone()),
                        alias_in_subquery,
                    ));
                    vec![(keys_query_ref, subquery_ref)]
                })
                .collect_vec();

            for meas in measures.iter() {
                ungrouped_measure_references.insert(
                    meas.full_name(),
                    QualifiedColumnName::new(
                        Some(pk_cube_alias.clone()),
                        subquery
                            .schema()
                            .resolve_member_alias(&meas.clone().as_base_member()),
                    ),
                );
            }

            join_builder.left_join_subselect(
                subquery,
                pk_cube_alias.clone(),
                JoinCondition::new_dimension_join(conditions, false),
            );
        } else {
            let conditions = primary_keys_dimensions
                .iter()
                .map(|dim| {
                    let alias_in_keys_query = keys_query.schema().resolve_member_alias(dim);
                    let keys_query_ref = Expr::Reference(QualifiedColumnName::new(
                        Some(keys_query_alias.clone()),
                        alias_in_keys_query,
                    ));
                    let pk_cube_expr = Expr::Member(MemberExpression::new(dim.clone()));
                    vec![(keys_query_ref, pk_cube_expr)]
                })
                .collect_vec();
            join_builder.left_join_cube(
                pk_cube.clone(),
                Some(pk_cube_alias.clone()),
                JoinCondition::new_dimension_join(conditions, false),
            );
        };

        let from = From::new_from_join(join_builder.build());
        let references_builder = ReferencesBuilder::new(from.clone());
        let mut select_builder = SelectBuilder::new(from.clone());
        let mut render_references = HashMap::new();
        for member in self
            .query_properties
            .all_dimensions_and_measures(&vec![])?
            .iter()
        {
            references_builder.resolve_references_for_member(
                member.member_evaluator(),
                &None,
                &mut render_references,
            )?;
            let alias = references_builder.resolve_alias_for_member(&member.full_name(), &None);
            select_builder.add_projection_member(member, alias);
        }
        for member in BaseMemberHelper::iter_as_base_member(&measures) {
            let alias = if !should_build_join_for_measure_select {
                references_builder.resolve_references_for_member(
                    member.member_evaluator(),
                    &None,
                    &mut render_references,
                )?;
                references_builder.resolve_alias_for_member(&member.full_name(), &None)
            } else {
                None
            };
            select_builder.add_projection_member(&member, alias);
        }
        select_builder.set_group_by(self.query_properties.group_by());
        let mut context_factory = self.context_factory.clone();
        context_factory.set_render_references(render_references);
        context_factory.set_ungrouped_measure_references(ungrouped_measure_references);
        Ok(Rc::new(select_builder.build(context_factory)))
    }

    fn check_should_build_join_for_measure_select(
        &self,
        measures: &Vec<Rc<BaseMeasure>>,
        key_cube_name: &String,
    ) -> Result<bool, CubeError> {
        for measure in measures.iter() {
            let cubes = collect_cube_names(measure.member_evaluator())?;
            let join_hints = collect_join_hints(measure.member_evaluator())?;
            if cubes.iter().any(|cube| cube != key_cube_name) {
                let measures_join = self.query_tools.join_graph().build_join(join_hints)?;
                if *measures_join
                    .static_data()
                    .multiplication_factor
                    .get(key_cube_name)
                    .unwrap_or(&false)
                {
                    return Err(CubeError::user(format!("{}' references cubes that lead to row multiplication. Please rewrite it using sub query.", measure.full_name())));
                }
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn aggregate_subquery_measure_join(
        &self,
        _key_cube_name: &String,
        measures: &Vec<Rc<BaseMeasure>>,
        primary_keys_dimensions: &Vec<Rc<dyn BaseMember>>,
    ) -> Result<Rc<Select>, CubeError> {
        let join_hints = collect_join_hints_for_measures(measures)?;
        let from = self
            .join_planner
            .make_join_node_with_prefix_and_join_hints(&None, join_hints)?;
        let mut context_factory = self.context_factory.clone();
        context_factory.set_ungrouped_measure(true);
        let mut select_builder = SelectBuilder::new(from);
        for dim in primary_keys_dimensions.iter() {
            select_builder.add_projection_member(dim, None);
        }
        for meas in measures.iter() {
            select_builder.add_projection_member(&meas.clone().as_base_member(), None);
        }
        Ok(Rc::new(select_builder.build(context_factory)))
    }

    fn regular_measures_subquery(
        &self,
        measures: &Vec<Rc<BaseMeasure>>,
        join: Rc<dyn JoinDefinition>,
        alias_prefix: String,
    ) -> Result<Rc<Select>, CubeError> {
        let source = self
            .join_planner
            .make_join_node_impl(&Some(alias_prefix), join)?;

        let mut select_builder = SelectBuilder::new(source.clone());
        let mut context_factory = self.context_factory.clone();
        for time_dim in self.query_properties.time_dimensions() {
            if let Some(granularity) = time_dim.get_granularity() {
                context_factory.add_leaf_time_dimension(&time_dim.full_name(), &granularity);
            }
        }

        for member in self
            .query_properties
            .all_dimensions_and_measures(&measures)?
            .iter()
        {
            select_builder.add_projection_member(member, None);
        }
        let filter = self.query_properties.all_filters();
        select_builder.set_filter(filter);
        select_builder.set_group_by(self.query_properties.group_by());
        Ok(Rc::new(select_builder.build(context_factory)))
    }

    fn key_query(
        &self,
        dimensions: &Vec<Rc<dyn BaseMember>>,
        key_join: Rc<dyn JoinDefinition>,
        key_cube_name: &String,
    ) -> Result<Rc<Select>, CubeError> {
        let source = self
            .join_planner
            .make_join_node_impl(&Some(format!("{}_key", key_cube_name)), key_join)?;
        let dimensions = self
            .query_properties
            .dimensions_for_select_append(dimensions);

        let mut select_builder = SelectBuilder::new(source);
        let mut context_factory = self.context_factory.clone();
        for time_dim in self.query_properties.time_dimensions() {
            if let Some(granularity) = time_dim.get_granularity() {
                context_factory.add_leaf_time_dimension(&time_dim.full_name(), &granularity);
            }
        }
        for member in dimensions.iter() {
            select_builder.add_projection_member(&member, None);
        }
        select_builder.set_distinct();
        select_builder.set_filter(self.query_properties.all_filters());

        Ok(Rc::new(select_builder.build(context_factory)))
    }
}
