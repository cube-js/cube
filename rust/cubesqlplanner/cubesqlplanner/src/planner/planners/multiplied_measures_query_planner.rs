use super::{CommonUtils, JoinPlanner};
use crate::plan::{From, JoinBuilder, JoinCondition, Select, SelectBuilder};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::{
    collect_cube_names, collect_join_hints, collect_join_hints_for_measures,
};
use crate::planner::sql_evaluator::sql_nodes::{ungroupped_measure, SqlNodesFactory};
use crate::planner::BaseMember;
use crate::planner::QueryProperties;
use crate::planner::{BaseMeasure, VisitorContext};
use cubenativeutils::CubeError;
use itertools::Itertools;
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
            let regular_subquery = self.regular_measures_subquery(&measures.regular_measures)?;
            joins.push(regular_subquery);
        }

        for (cube_name, measures) in measures
            .multiplied_measures
            .clone()
            .into_iter()
            .into_group_map_by(|m| m.cube_name().clone())
        {
            let aggregate_subquery = self.aggregate_subquery(&cube_name, &measures)?;
            joins.push(aggregate_subquery);
        }
        Ok(joins)
    }

    fn aggregate_subquery(
        &self,
        key_cube_name: &String,
        measures: &Vec<Rc<BaseMeasure>>,
    ) -> Result<Rc<Select>, CubeError> {
        let primary_keys_dimensions = self.common_utils.primary_keys_dimensions(key_cube_name)?;
        let keys_query = self.key_query(&primary_keys_dimensions, key_cube_name)?;
        let keys_query_alias = format!("keys");
        let should_build_join_for_measure_select =
            self.check_should_build_join_for_measure_select(measures, key_cube_name)?;

        let mut join_builder =
            JoinBuilder::new_from_subselect(keys_query, keys_query_alias.clone());

        let pk_cube = self.common_utils.cube_from_path(key_cube_name.clone())?;
        let pk_cube_alias =
            pk_cube.default_alias_with_prefix(&Some(format!("{key_cube_name}_key")));
        let measures = if should_build_join_for_measure_select {
            let mut top_measures = vec![];
            let mut ungroupped_measures = vec![];
            /* for meas in measures.iter() {
                let ungropped_name = format!("{}_ungrouped", meas.name());
                let (top, ungrouped) = meas
                    .member_evaluator()
                    .clone()
                    .try_split_measure(ungropped_name)
                    .unwrap();
                top_measures.push(BaseMeasure::try_new(top, self.query_tools.clone())?.unwrap());
                ungroupped_measures
                    .push(BaseMeasure::try_new(ungrouped, self.query_tools.clone())?.unwrap());
            } */
            join_builder.left_join_subselect(
                self.aggregate_subquery_measure_join(
                    key_cube_name,
                    &ungroupped_measures,
                    &primary_keys_dimensions,
                )?,
                pk_cube_alias.clone(),
                JoinCondition::new_dimension_join(
                    keys_query_alias,
                    pk_cube_alias,
                    primary_keys_dimensions,
                    false,
                ),
            );
            top_measures
        } else {
            join_builder.left_join_cube(
                pk_cube.clone(),
                Some(pk_cube_alias.clone()),
                JoinCondition::new_dimension_join(
                    keys_query_alias,
                    pk_cube_alias,
                    primary_keys_dimensions,
                    false,
                ),
            );
            measures.clone()
        };

        let mut select_builder = SelectBuilder::new(
            From::new_from_join(join_builder.build()),
            self.context_factory.clone(),
        );
        for member in self
            .query_properties
            .all_dimensions_and_measures(&measures)?
            .iter()
        {
            select_builder.add_projection_member(member, None, None);
        }
        select_builder.set_group_by(self.query_properties.group_by());
        Ok(Rc::new(select_builder.build()))
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
        key_cube_name: &String,
        measures: &Vec<Rc<BaseMeasure>>,
        primary_keys_dimensions: &Vec<Rc<dyn BaseMember>>,
    ) -> Result<Rc<Select>, CubeError> {
        let join_hints = collect_join_hints_for_measures(measures)?;
        let from = self
            .join_planner
            .make_join_node_with_prefix_and_join_hints(&None, join_hints)?;
        let mut context_factory = self.context_factory.clone();
        context_factory.set_ungrouped_measure(true);
        let mut select_builder = SelectBuilder::new(from, context_factory);
        for dim in primary_keys_dimensions.iter() {
            select_builder.add_projection_member(dim, None, None);
        }
        for meas in measures.iter() {
            select_builder.add_projection_member(&meas.clone().as_base_member(), None, None);
        }
        Ok(Rc::new(select_builder.build()))
    }

    fn regular_measures_subquery(
        &self,
        measures: &Vec<Rc<BaseMeasure>>,
    ) -> Result<Rc<Select>, CubeError> {
        let source = self
            .join_planner
            .make_join_node_with_prefix(&Some(format!("main")))?;
        let mut select_builder = SelectBuilder::new(source, self.context_factory.clone());
        for member in self
            .query_properties
            .all_dimensions_and_measures(&measures)?
            .iter()
        {
            select_builder.add_projection_member(member, None, None);
        }
        select_builder.set_filter(self.query_properties.all_filters());
        select_builder.set_group_by(self.query_properties.group_by());
        Ok(Rc::new(select_builder.build()))
    }

    fn key_query(
        &self,
        dimensions: &Vec<Rc<dyn BaseMember>>,
        key_cube_name: &String,
    ) -> Result<Rc<Select>, CubeError> {
        let source = self
            .join_planner
            .make_join_node_with_prefix(&Some(format!("{}_key", key_cube_name)))?;
        let dimensions = self
            .query_properties
            .dimensions_for_select_append(dimensions);

        let mut select_builder = SelectBuilder::new(source, self.context_factory.clone());
        for member in dimensions.iter() {
            select_builder.add_projection_member(&member, None, None);
        }
        select_builder.set_distinct();
        select_builder.set_filter(self.query_properties.all_filters());

        Ok(Rc::new(select_builder.build()))
    }
}
