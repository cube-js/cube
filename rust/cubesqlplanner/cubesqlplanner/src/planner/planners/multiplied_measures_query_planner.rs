use super::{CommonUtils, JoinPlanner, MultiStageQueryPlanner, OrderPlanner, SimpleQueryPlanner};
use crate::plan::{Filter, From, FromSource, Join, JoinItem, JoinSource, Select, SelectBuilder};
use crate::planner::base_join_condition::DimensionJoinCondition;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::{
    collect_multiplied_measures, has_multi_stage_members,
};
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::BaseMember;
use crate::planner::QueryProperties;
use crate::planner::{BaseDimension, BaseMeasure, PrimaryJoinCondition, VisitorContext};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

pub struct MultipliedMeasuresQueryPlanner {
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
    join_planner: JoinPlanner,
    order_planner: OrderPlanner,
    multi_stage_planner: MultiStageQueryPlanner,
    common_utils: CommonUtils,
    context_factory: Rc<SqlNodesFactory>,
}

impl MultipliedMeasuresQueryPlanner {
    pub fn new(
        query_tools: Rc<QueryTools>,
        query_properties: Rc<QueryProperties>,
        context_factory: Rc<SqlNodesFactory>,
    ) -> Self {
        Self {
            join_planner: JoinPlanner::new(query_tools.clone()),
            order_planner: OrderPlanner::new(query_properties.clone()),
            common_utils: CommonUtils::new(query_tools.clone()),
            multi_stage_planner: MultiStageQueryPlanner::new(
                query_tools.clone(),
                query_properties.clone(),
            ),
            query_tools,
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

        let pk_cube =
            JoinSource::new_from_cube(self.common_utils.cube_from_path(key_cube_name.clone())?);
        let mut joins = vec![];
        joins.push(JoinItem {
            from: pk_cube,
            on: PrimaryJoinCondition::try_new(self.query_tools.clone(), primary_keys_dimensions)?,
            is_inner: false,
        });
        let join = Rc::new(Join {
            root: JoinSource::new_from_select(
                keys_query,
                self.query_tools.escape_column_name("keys"),
            ), //FIXME replace with constant
            joins,
        });
        let mut select_builder = SelectBuilder::new(
            From::new(FromSource::Join(join)),
            VisitorContext::new_with_cube_alias_prefix(
                self.context_factory.clone(),
                format!("{}_key", key_cube_name),
            ),
        );
        select_builder.set_projection(self.query_properties.dimensions_references_and_measures(
            &self.query_tools.escape_column_name("keys"),
            &measures,
        )?);
        select_builder.set_group_by(self.query_properties.group_by());
        Ok(Rc::new(select_builder.build()))
    }

    fn regular_measures_subquery(
        &self,
        measures: &Vec<Rc<BaseMeasure>>,
    ) -> Result<Rc<Select>, CubeError> {
        let source = self.join_planner.make_join_node()?;
        let mut select_builder = SelectBuilder::new(
            source,
            VisitorContext::new_with_cube_alias_prefix(
                self.context_factory.clone(),
                "main".to_string(),
            ),
        );
        select_builder.set_projection(
            self.query_properties
                .select_all_dimensions_and_measures(measures)?,
        );
        select_builder.set_filter(self.query_properties.all_filters());
        select_builder.set_group_by(self.query_properties.group_by());
        Ok(Rc::new(select_builder.build()))
    }

    fn key_query(
        &self,
        dimensions: &Vec<Rc<BaseDimension>>,
        key_cube_name: &String,
    ) -> Result<Rc<Select>, CubeError> {
        let source = self.join_planner.make_join_node()?;
        let dimensions = self
            .query_properties
            .dimensions_for_select_append(dimensions);

        let mut select_builder = SelectBuilder::new(
            source,
            VisitorContext::new_with_cube_alias_prefix(
                self.context_factory.clone(),
                format!("{}_key", key_cube_name),
            ),
        );
        select_builder.set_projection(self.query_properties.columns_to_expr(&dimensions));
        select_builder.set_distinct();
        select_builder.set_filter(self.query_properties.all_filters());

        Ok(Rc::new(select_builder.build()))
    }
}
