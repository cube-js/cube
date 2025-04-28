use super::{CommonUtils, DimensionSubqueryPlanner, JoinPlanner};
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::logical_plan::*;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::{
    collect_cube_names, collect_join_hints, collect_join_hints_for_measures,
    collect_sub_query_dimensions_from_members, collect_sub_query_dimensions_from_symbols,
};
use crate::planner::{
    BaseMeasure, BaseMember, BaseMemberHelper, FullKeyAggregateMeasures, QueryProperties,
};
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

    pub fn plan_queries(&self) -> Result<Rc<ResolveMultipliedMeasures>, CubeError> {
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
        let result = Rc::new(ResolveMultipliedMeasures {
            regular_measure_subqueries,
            aggregate_multiplied_subqueries,
        });
        Ok(result)
    }

    fn aggregate_subquery_plan(
        &self,
        key_cube_name: &String,
        measures: &Vec<Rc<BaseMeasure>>,
        key_join: Rc<dyn JoinDefinition>,
    ) -> Result<Rc<AggregateMultipliedSubquery>, CubeError> {
        let measures_symbols = measures
            .iter()
            .map(|m| m.member_evaluator().clone())
            .collect();
        let subquery_dimensions = collect_sub_query_dimensions_from_symbols(
            &measures_symbols,
            &self.join_planner,
            &key_join,
            self.query_tools.clone(),
        )?;

        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let subquery_dimension_queries =
            dimension_subquery_planner.plan_queries(&subquery_dimensions)?;

        let primary_keys_dimensions = self
            .common_utils
            .primary_keys_dimensions(key_cube_name)?
            .into_iter()
            .map(|d| d.as_base_member())
            .collect_vec();
        let keys_subquery =
            self.key_query(&primary_keys_dimensions, key_join.clone(), key_cube_name)?;
        let schema = Rc::new(LogicalSchema {
            time_dimensions: self.query_properties.time_dimension_symbols(),
            dimensions: self.query_properties.dimension_symbols(),
            measures: measures_symbols,
            multiplied_measures: self
                .full_key_aggregate_measures
                .rendered_as_multiplied_measures
                .clone(),
        });
        let pk_cube = self.common_utils.cube_from_path(key_cube_name.clone())?;
        let should_build_join_for_measure_select =
            self.check_should_build_join_for_measure_select(measures, key_cube_name)?;
        let source = if should_build_join_for_measure_select {
            let measure_subquery = self.aggregate_subquery_measure(
                key_join.clone(),
                &measures,
                &primary_keys_dimensions,
            )?;
            Rc::new(AggregateMultipliedSubquerySouce::MeasureSubquery(
                measure_subquery,
            ))
        } else {
            Rc::new(AggregateMultipliedSubquerySouce::Cube)
        };
        Ok(Rc::new(AggregateMultipliedSubquery {
            schema,
            pk_cube,
            keys_subquery,
            dimension_subqueries: subquery_dimension_queries,
            source,
        }))
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

    fn aggregate_subquery_measure(
        &self,
        key_join: Rc<dyn JoinDefinition>,
        measures: &Vec<Rc<BaseMeasure>>,
        primary_keys_dimensions: &Vec<Rc<dyn BaseMember>>,
    ) -> Result<Rc<MeasureSubquery>, CubeError> {
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
        let subquery_dimension_queries =
            dimension_subquery_planner.plan_queries(&subquery_dimensions)?;
        let join_hints = collect_join_hints_for_measures(measures)?;
        let source = self
            .join_planner
            .make_join_logical_plan_with_join_hints(join_hints)?;

        let result = MeasureSubquery {
            primary_keys_dimensions: primary_keys_dimensions
                .iter()
                .map(|dim| dim.member_evaluator().clone())
                .collect(),
            measures: measures
                .iter()
                .map(|meas| meas.member_evaluator().clone())
                .collect(),
            dimension_subqueries: subquery_dimension_queries,
            source,
        };
        Ok(Rc::new(result))
    }

    fn regular_measures_subquery(
        &self,
        measures: &Vec<Rc<BaseMeasure>>,
        join: Rc<dyn JoinDefinition>,
    ) -> Result<Rc<SimpleQuery>, CubeError> {
        let measures_symbols = measures
            .iter()
            .map(|m| m.member_evaluator().clone())
            .collect();
        let all_symbols =
            self.query_properties
                .get_member_symbols(true, true, false, true, &measures_symbols);

        let subquery_dimensions = collect_sub_query_dimensions_from_symbols(
            &all_symbols,
            &self.join_planner,
            &join,
            self.query_tools.clone(),
        )?;

        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let subquery_dimension_queries =
            dimension_subquery_planner.plan_queries(&subquery_dimensions)?;

        let source = self.join_planner.make_join_logical_plan(join)?;

        let schema = Rc::new(LogicalSchema {
            dimensions: self.query_properties.dimension_symbols(),
            time_dimensions: self.query_properties.time_dimension_symbols(),
            measures: measures_symbols,
            multiplied_measures: self
                .full_key_aggregate_measures
                .rendered_as_multiplied_measures
                .clone(),
        });

        let logical_filter = Rc::new(LogicalFilter {
            dimensions_filters: self.query_properties.dimensions_filters().clone(),
            time_dimensions_filters: self.query_properties.time_dimensions_filters().clone(),
            measures_filter: vec![],
            segments: self.query_properties.segments().clone(),
        });

        let query = SimpleQuery {
            schema,
            filter: logical_filter,
            offset: self.query_properties.offset(),
            limit: self.query_properties.row_limit(),
            ungrouped: self.query_properties.ungrouped(),
            dimension_subqueries: subquery_dimension_queries,
            source,
            order_by: vec![],
        };
        Ok(Rc::new(query))
    }

    fn key_query(
        &self,
        dimensions: &Vec<Rc<dyn BaseMember>>,
        key_join: Rc<dyn JoinDefinition>,
        key_cube_name: &String,
    ) -> Result<Rc<KeysSubQuery>, CubeError> {
        let source = self.join_planner.make_join_logical_plan(key_join.clone())?;

        let dimensions_symbols = dimensions
            .iter()
            .map(|d| d.member_evaluator().clone())
            .collect();

        let all_symbols =
            self.query_properties
                .get_member_symbols(true, true, false, true, &dimensions_symbols);

        let subquery_dimensions = collect_sub_query_dimensions_from_symbols(
            &all_symbols,
            &self.join_planner,
            &key_join,
            self.query_tools.clone(),
        )?;

        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let subquery_dimension_queries =
            dimension_subquery_planner.plan_queries(&subquery_dimensions)?;

        let logical_filter = Rc::new(LogicalFilter {
            dimensions_filters: self.query_properties.dimensions_filters().clone(),
            time_dimensions_filters: self.query_properties.time_dimensions_filters().clone(),
            measures_filter: vec![],
            segments: self.query_properties.segments().clone(),
        });

        let keys_query = KeysSubQuery {
            time_dimensions: self.query_properties.time_dimension_symbols(),
            dimensions: self.query_properties.dimension_symbols(),
            primary_keys_dimensions: dimensions_symbols,
            filter: logical_filter,
            source,
            dimension_subqueries: subquery_dimension_queries,
            key_cube_name: key_cube_name.clone(),
        };

        Ok(Rc::new(keys_query))
    }
}
