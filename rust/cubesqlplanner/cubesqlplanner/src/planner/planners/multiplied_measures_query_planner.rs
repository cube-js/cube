use super::{CommonUtils, DimensionSubqueryPlanner, JoinPlanner};
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::plan::{
    Expr, From, JoinBuilder, JoinCondition, MemberExpression, QualifiedColumnName, Select,
    SelectBuilder,
};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::{
    collect_cube_names, collect_join_hints, collect_join_hints_for_measures,
    collect_sub_query_dimensions_from_members, collect_sub_query_dimensions_from_symbols,
};
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_evaluator::ReferencesBuilder;
use crate::planner::{
    BaseMeasure, BaseMember, BaseMemberHelper, FullKeyAggregateMeasures, QueryProperties,
};
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
    full_key_aggregate_measures: FullKeyAggregateMeasures,
}

impl MultipliedMeasuresQueryPlanner {
    pub fn try_new(
        query_tools: Rc<QueryTools>,
        query_properties: Rc<QueryProperties>,
        context_factory: SqlNodesFactory,
    ) -> Result<Self, CubeError> {
        let full_key_aggregate_measures = query_properties.full_key_aggregate_measures()?;
        Ok(Self {
            query_tools: query_tools.clone(),
            join_planner: JoinPlanner::new(query_tools.clone()),
            common_utils: CommonUtils::new(query_tools.clone()),
            query_properties,
            context_factory,
            full_key_aggregate_measures,
        })
    }

    pub fn plan_queries(&self) -> Result<Vec<Rc<Select>>, CubeError> {
        if self.query_properties.is_simple_query()? {
            return Err(CubeError::internal(format!(
                "MultipliedMeasuresQueryPlanner should not be used for simple query"
            )));
        }

        let full_key_aggregate_measures = &self.full_key_aggregate_measures;

        let mut joins = Vec::new();

        if !full_key_aggregate_measures.regular_measures.is_empty() {
            let join_multi_fact_groups = self
                .query_properties
                .compute_join_multi_fact_groups_with_measures(
                    &full_key_aggregate_measures.regular_measures,
                )?;
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

        for (cube_name, measures) in full_key_aggregate_measures
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
        let subquery_dimensions = collect_sub_query_dimensions_from_members(
            &BaseMemberHelper::iter_as_base_member(measures).collect_vec(),
            &self.join_planner,
            &key_join,
            self.query_tools.clone(),
        )?;

        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;

        let primary_keys_dimensions = self
            .common_utils
            .primary_keys_dimensions(key_cube_name)?
            .into_iter()
            .map(|d| d.as_base_member())
            .collect_vec();
        let keys_query = self.key_query(&primary_keys_dimensions, key_join, key_cube_name)?;
        let keys_query_alias = format!("keys");
        let should_build_join_for_measure_select =
            self.check_should_build_join_for_measure_select(measures, key_cube_name)?;

        let mut join_builder =
            JoinBuilder::new_from_subselect(keys_query.clone(), keys_query_alias.clone());

        let pk_cube = self.common_utils.cube_from_path(key_cube_name.clone())?;
        let pk_cube_alias =
            pk_cube.default_alias_with_prefix(&Some(format!("{}_key", pk_cube.default_alias())));
        let mut ungrouped_measure_references = HashMap::new();
        if should_build_join_for_measure_select {
            let subquery = self.aggregate_subquery_measure_join(
                key_cube_name,
                &measures,
                &primary_keys_dimensions,
                &dimension_subquery_planner,
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
            for sub_dim in subquery_dimensions.iter() {
                dimension_subquery_planner.add_join(&mut join_builder, sub_dim.clone())?;
            }
        };

        let from = From::new_from_join(join_builder.build());
        let references_builder = ReferencesBuilder::new(from.clone());
        let mut select_builder = SelectBuilder::new(from.clone());
        let mut render_references = dimension_subquery_planner.dimensions_refs().clone();
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
        context_factory.set_rendered_as_multiplied_measures(
            self.full_key_aggregate_measures
                .rendered_as_multiplied_measures
                .clone(),
        );
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
        dimension_subquery_planner: &DimensionSubqueryPlanner,
    ) -> Result<Rc<Select>, CubeError> {
        let join_hints = collect_join_hints_for_measures(measures)?;
        let from = self
            .join_planner
            .make_join_node_with_prefix_and_join_hints(
                &None,
                join_hints,
                &dimension_subquery_planner,
            )?;
        let mut context_factory = self.context_factory.clone();
        context_factory.set_ungrouped_measure(true);
        context_factory.set_render_references(dimension_subquery_planner.dimensions_refs().clone());

        context_factory.set_rendered_as_multiplied_measures(
            self.full_key_aggregate_measures
                .rendered_as_multiplied_measures
                .clone(),
        );

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
        let subquery_dimensions = collect_sub_query_dimensions_from_symbols(
            &self.query_properties.all_member_symbols(false),
            &self.join_planner,
            &join,
            self.query_tools.clone(),
        )?;

        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let source = self.join_planner.make_join_node_impl(
            &Some(alias_prefix),
            join,
            &dimension_subquery_planner,
        )?;

        let mut select_builder = SelectBuilder::new(source.clone());
        let mut context_factory = self.context_factory.clone();

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

        let render_references = dimension_subquery_planner.dimensions_refs().clone();
        context_factory.set_render_references(render_references);
        context_factory.set_rendered_as_multiplied_measures(
            self.full_key_aggregate_measures
                .rendered_as_multiplied_measures
                .clone(),
        );

        Ok(Rc::new(select_builder.build(context_factory)))
    }

    fn key_query(
        &self,
        dimensions: &Vec<Rc<dyn BaseMember>>,
        key_join: Rc<dyn JoinDefinition>,
        key_cube_name: &String,
    ) -> Result<Rc<Select>, CubeError> {
        let dimensions = self
            .query_properties
            .dimensions_for_select_append(dimensions);

        let mut symbols_for_subquery_dimensions =
            BaseMemberHelper::extract_symbols_from_members(&dimensions);
        for item in self.query_properties.dimensions_filters() {
            item.find_all_member_evaluators(&mut symbols_for_subquery_dimensions);
        }

        for item in self.query_properties.measures_filters() {
            item.find_all_member_evaluators(&mut symbols_for_subquery_dimensions);
        }

        let symbols_for_subquery_dimensions = symbols_for_subquery_dimensions
            .into_iter()
            .unique_by(|m| m.full_name())
            .collect_vec();

        let subquery_dimensions = collect_sub_query_dimensions_from_symbols(
            &symbols_for_subquery_dimensions,
            &self.join_planner,
            &key_join,
            self.query_tools.clone(),
        )?;

        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;

        let source = self.join_planner.make_join_node_impl(
            &Some(format!(
                "{}_key",
                self.query_tools.alias_for_cube(key_cube_name)?
            )),
            key_join,
            &dimension_subquery_planner,
        )?;

        let mut select_builder = SelectBuilder::new(source);
        let mut context_factory = self.context_factory.clone();

        context_factory.set_render_references(dimension_subquery_planner.dimensions_refs().clone());

        for member in dimensions.iter() {
            select_builder.add_projection_member(&member, None);
        }
        select_builder.set_distinct();
        select_builder.set_filter(self.query_properties.all_filters());

        Ok(Rc::new(select_builder.build(context_factory)))
    }
}
