use super::{CommonUtils, DimensionSubqueryPlanner, JoinPlanner};
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::logical_plan::*;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::{
    collect_cube_names, collect_join_hints, collect_join_hints_for_measures,
    collect_sub_query_dimensions_from_members, collect_sub_query_dimensions_from_symbols,
};
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::{FullKeyAggregateMeasures, QueryProperties};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub struct MultipliedMeasuresQueryPlanner {
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
    join_planner: JoinPlanner,
    common_utils: CommonUtils,
    full_key_aggregate_measures: FullKeyAggregateMeasures,
}

impl MultipliedMeasuresQueryPlanner {
    pub fn try_new(
        query_tools: Rc<QueryTools>,
        query_properties: Rc<QueryProperties>,
    ) -> Result<Self, CubeError> {
        let full_key_aggregate_measures = query_properties.full_key_aggregate_measures()?;
        Ok(Self {
            query_tools: query_tools.clone(),
            join_planner: JoinPlanner::new(query_tools.clone()),
            common_utils: CommonUtils::new(query_tools.clone()),
            query_properties,
            full_key_aggregate_measures,
        })
    }

    pub fn plan_queries(&self) -> Result<Option<Rc<ResolveMultipliedMeasures>>, CubeError> {
        if self.query_properties.is_simple_query()? {
            return Err(CubeError::internal(format!(
                "MultipliedMeasuresQueryPlanner should not be used for simple query"
            )));
        }

        let full_key_aggregate_measures = &self.full_key_aggregate_measures;

        let mut regular_measure_subqueries = Vec::new();
        let mut aggregate_multiplied_subqueries = Vec::new();

        if !full_key_aggregate_measures.regular_measures.is_empty() {
            let join_multi_fact_groups = self
                .query_properties
                .compute_join_multi_fact_groups_with_measures(
                    &full_key_aggregate_measures.regular_measures,
                )?;
            for (join, measures) in join_multi_fact_groups.iter() {
                let regular_subquery_logical_plan =
                    self.regular_measures_subquery(measures, join.clone())?;
                regular_measure_subqueries.push(regular_subquery_logical_plan);
            }
        }

        for (cube_name, measures) in full_key_aggregate_measures
            .multiplied_measures
            .clone()
            .into_iter()
            .into_group_map_by(|m| m.cube_name().clone())
        {
            let measures = measures
                .into_iter()
                .map(|m| m.measure().clone())
                .collect_vec();
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
            let aggregate_subquery_logical_plan = self.aggregate_subquery_plan(
                &cube_name,
                &measures,
                join_multi_fact_groups.into_iter().next().unwrap().0,
            )?;
            aggregate_multiplied_subqueries.push(aggregate_subquery_logical_plan);
        }
        if regular_measure_subqueries.is_empty() && aggregate_multiplied_subqueries.is_empty() {
            Ok(None)
        } else {
            let all_measures = full_key_aggregate_measures
                .regular_measures
                .iter()
                .chain(
                    full_key_aggregate_measures
                        .multiplied_measures
                        .iter()
                        .map(|m| m.measure()),
                )
                .map(|m| m.clone())
                .collect_vec();
            let schema = LogicalSchema::default()
                .set_time_dimensions(self.query_properties.time_dimensions().clone())
                .set_dimensions(self.query_properties.dimensions().clone())
                .set_measures(all_measures)
                .set_multiplied_measures(
                    full_key_aggregate_measures
                        .rendered_as_multiplied_measures
                        .clone(),
                )
                .into_rc();
            let logical_filter = Rc::new(LogicalFilter {
                dimensions_filters: self.query_properties.dimensions_filters().clone(),
                time_dimensions_filters: self.query_properties.time_dimensions_filters().clone(),
                measures_filter: self.query_properties.measures_filters().clone(), //TODO may be reduce filters to only used measures here
                segments: self.query_properties.segments().clone(),
            });
            let result = Rc::new(ResolveMultipliedMeasures {
                schema,
                filter: logical_filter,
                regular_measure_subqueries,
                aggregate_multiplied_subqueries,
            });
            Ok(Some(result))
        }
    }

    fn aggregate_subquery_plan(
        &self,
        key_cube_name: &String,
        measures: &Vec<Rc<MemberSymbol>>,
        key_join: Rc<dyn JoinDefinition>,
    ) -> Result<Rc<AggregateMultipliedSubquery>, CubeError> {
        let pk_cube = self.common_utils.cube_from_path(key_cube_name.clone())?;
        let pk_cube = Cube::new(pk_cube);
        let subquery_dimensions =
            collect_sub_query_dimensions_from_symbols(&measures, &self.join_planner, &key_join)?;

        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let subquery_dimension_queries =
            dimension_subquery_planner.plan_queries(&subquery_dimensions)?;

        let primary_keys_dimensions = self.common_utils.primary_keys_dimensions(key_cube_name)?;
        let keys_subquery =
            self.key_query(&primary_keys_dimensions, key_join.clone(), pk_cube.clone())?;

        let schema = LogicalSchema::default()
            .set_dimensions(self.query_properties.dimensions().clone())
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
            .set_measures(measures.clone())
            .set_multiplied_measures(
                self.full_key_aggregate_measures
                    .rendered_as_multiplied_measures
                    .clone(),
            )
            .into_rc();
        let should_build_join_for_measure_select =
            self.check_should_build_join_for_measure_select(measures, key_cube_name)?;
        let source = if should_build_join_for_measure_select {
            let measure_subquery = self.aggregate_subquery_measure(
                key_join.clone(),
                &measures,
                &primary_keys_dimensions,
            )?;
            AggregateMultipliedSubquerySouce::MeasureSubquery(measure_subquery)
        } else {
            AggregateMultipliedSubquerySouce::Cube(pk_cube)
        };
        Ok(Rc::new(AggregateMultipliedSubquery {
            schema,
            keys_subquery,
            dimension_subqueries: subquery_dimension_queries,
            source,
        }))
    }

    fn check_should_build_join_for_measure_select(
        &self,
        measures: &Vec<Rc<MemberSymbol>>,
        key_cube_name: &String,
    ) -> Result<bool, CubeError> {
        for measure in measures.iter() {
            let member_expression_over_dimensions_cubes =
                if let Ok(member_expression) = measure.as_member_expression() {
                    member_expression.cube_names_if_dimension_only_expression()?
                } else {
                    None
                };
            let cubes = if let Some(cubes) = member_expression_over_dimensions_cubes {
                cubes
            } else {
                collect_cube_names(&measure)?
            };
            let join_hints = collect_join_hints(&measure)?;
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

    fn aggregate_subquery_measure(
        &self,
        key_join: Rc<dyn JoinDefinition>,
        measures: &Vec<Rc<MemberSymbol>>,
        primary_keys_dimensions: &Vec<Rc<MemberSymbol>>,
    ) -> Result<Rc<MeasureSubquery>, CubeError> {
        let subquery_dimensions =
            collect_sub_query_dimensions_from_members(&measures, &self.join_planner, &key_join)?;
        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let subquery_dimension_queries =
            dimension_subquery_planner.plan_queries(&subquery_dimensions)?;
        let join_hints = collect_join_hints_for_measures(&measures)?;
        let source = self
            .join_planner
            .make_join_logical_plan_with_join_hints(join_hints, subquery_dimension_queries)?;

        let schema = LogicalSchema::default()
            .set_dimensions(primary_keys_dimensions.clone())
            .set_measures(measures.clone())
            .into_rc();

        let result = MeasureSubquery { schema, source };
        Ok(Rc::new(result))
    }

    fn regular_measures_subquery(
        &self,
        measures: &Vec<Rc<MemberSymbol>>,
        join: Rc<dyn JoinDefinition>,
    ) -> Result<Rc<Query>, CubeError> {
        let all_symbols = self
            .query_properties
            .get_member_symbols(true, true, false, true, &measures);

        let subquery_dimensions =
            collect_sub_query_dimensions_from_symbols(&all_symbols, &self.join_planner, &join)?;

        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let subquery_dimension_queries =
            dimension_subquery_planner.plan_queries(&subquery_dimensions)?;

        let source = self
            .join_planner
            .make_join_logical_plan(join, subquery_dimension_queries.clone())?;

        let schema = LogicalSchema::default()
            .set_dimensions(self.query_properties.dimensions().clone())
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
            .set_measures(measures.clone())
            .set_multiplied_measures(
                self.full_key_aggregate_measures
                    .rendered_as_multiplied_measures
                    .clone(),
            )
            .into_rc();

        let logical_filter = Rc::new(LogicalFilter {
            dimensions_filters: self.query_properties.dimensions_filters().clone(),
            time_dimensions_filters: self.query_properties.time_dimensions_filters().clone(),
            measures_filter: vec![],
            segments: self.query_properties.segments().clone(),
        });

        let query = Query {
            schema,
            filter: logical_filter,
            modifers: Rc::new(LogicalQueryModifiers {
                offset: None,
                limit: None,
                ungrouped: self.query_properties.ungrouped(),
                order_by: vec![],
            }),
            source: QuerySource::LogicalJoin(source),
            multistage_members: vec![],
        };
        Ok(Rc::new(query))
    }

    fn key_query(
        &self,
        dimensions: &Vec<Rc<MemberSymbol>>,
        key_join: Rc<dyn JoinDefinition>,
        key_cube: Rc<Cube>,
    ) -> Result<Rc<KeysSubQuery>, CubeError> {
        let all_symbols =
            self.query_properties
                .get_member_symbols(true, true, false, true, &dimensions);

        let subquery_dimensions =
            collect_sub_query_dimensions_from_symbols(&all_symbols, &self.join_planner, &key_join)?;

        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let subquery_dimension_queries =
            dimension_subquery_planner.plan_queries(&subquery_dimensions)?;

        let source = self
            .join_planner
            .make_join_logical_plan(key_join.clone(), subquery_dimension_queries)?;

        let logical_filter = Rc::new(LogicalFilter {
            dimensions_filters: self.query_properties.dimensions_filters().clone(),
            time_dimensions_filters: self.query_properties.time_dimensions_filters().clone(),
            measures_filter: vec![],
            segments: self.query_properties.segments().clone(),
        });

        let schema = LogicalSchema::default()
            .set_dimensions(self.query_properties.dimensions().clone())
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
            .into_rc();

        let keys_query = KeysSubQuery {
            schema,
            primary_keys_dimensions: dimensions.clone(),
            filter: logical_filter,
            source,
            //dimension_subqueries: subquery_dimension_queries,
            pk_cube: key_cube,
        };

        Ok(Rc::new(keys_query))
    }
}
