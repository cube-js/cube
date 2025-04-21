use crate::planner::query_tools::QueryTools;
use crate::planner::GranularityHelper;
use std::rc::Rc;
use cubenativeutils::CubeError;
use super::*;
use crate::logical_plan::*;
use crate::planner::sql_evaluator::MemberSymbol;
use std::collections::{HashMap, HashSet};
use crate::cube_bridge::pre_aggregation_obj::PreAggregationObj;

#[derive(Clone, Debug, PartialEq)]
pub enum MatchState {
    Partial,
    Full,
    NotMatched,
}

impl MatchState {
    pub fn combine(&self, other: &MatchState) -> MatchState {
        if matches!(self, MatchState::NotMatched) || matches!(other, MatchState::NotMatched) {
            return MatchState::NotMatched;
        }
        if matches!(self, MatchState::Partial)
            || matches!(other, MatchState::Partial)
        {
            return MatchState::Partial;
        }
        return MatchState::Full;
    }
}

pub struct PreAggregationOptimizer {
    query_tools: Rc<QueryTools>,
    used_pre_aggregations: HashMap<(String, String), Rc<dyn PreAggregationObj>>,
}

impl PreAggregationOptimizer {
    pub fn new(query_tools: Rc<QueryTools>) -> Self {
        Self { query_tools, used_pre_aggregations: HashMap::new() }
    }

    pub fn try_optimize(&mut self, plan: Rc<Query>) -> Result<Option<Rc<Query>>, CubeError> {
        let mut cube_names_collector = CubeNamesCollector::new();
        cube_names_collector.collect(&plan)?;
        let cube_names = cube_names_collector.result();
        println!("!!! Cube names: {:?}", cube_names);

        let mut compiled_pre_aggregations = Vec::new();
        for cube_name in cube_names.iter() {
            let pre_aggregations = self
                .query_tools
                .cube_evaluator()
                .pre_aggregations_for_cube_as_array(cube_name.clone())?;
            for pre_aggregation in pre_aggregations.iter() {
                let compiled = CompiledPreAggregation::try_new(
                    self.query_tools.clone(),
                    cube_name,
                    pre_aggregation.clone(),
                )?;
                compiled_pre_aggregations.push(compiled);
            }
        }

        for pre_aggregation in compiled_pre_aggregations.iter() {
            let new_query = self.try_rewrite_query(plan.clone(), pre_aggregation)?;
            if new_query.is_some() {
                return Ok(new_query);
            }
        }

        Ok(None)
    }

    pub fn get_used_pre_aggregations(&self) -> Vec<Rc<dyn PreAggregationObj>> {
        self.used_pre_aggregations.values().cloned().collect()
    }

    fn try_rewrite_query(&mut self, query: Rc<Query>, pre_aggregation: &Rc<CompiledPreAggregation>) -> Result<Option<Rc<Query>>, CubeError> {
        
        match query.as_ref() {
            Query::SimpleQuery(query) => self.try_rewrite_simple_query(query, pre_aggregation),
            Query::FullKeyAggregateQuery(query) => self.try_rewrite_full_key_aggregate_query(query, pre_aggregation),
        }
    }

    fn try_rewrite_simple_query(&mut self, query: &SimpleQuery, pre_aggregation: &Rc<CompiledPreAggregation>) -> Result<Option<Rc<Query>>, CubeError> {

        if self.is_schema_and_filters_match(&query.schema, &query.filter, pre_aggregation)? {
            let mut new_query = SimpleQuery::clone(&query);
            new_query.source = SimpleQuerySource::PreAggregation(self.make_pre_aggregation_source(pre_aggregation)?);
            Ok(Some(Rc::new(Query::SimpleQuery(new_query))))
        } else {
            Ok(None)
        }
    }

    fn try_rewrite_full_key_aggregate_query(&mut self, query: &FullKeyAggregateQuery, pre_aggregation: &Rc<CompiledPreAggregation>) -> Result<Option<Rc<Query>>, CubeError> {
        if self.is_schema_and_filters_match(&query.schema, &query.filter, pre_aggregation)? {
            let source = SimpleQuerySource::PreAggregation(self.make_pre_aggregation_source(pre_aggregation)?);
            let mut new_query = SimpleQuery {
                schema: query.schema.clone(),
                dimension_subqueries: vec![],
                filter: query.filter.clone(),
                offset: query.offset,
                limit: query.limit,
                ungrouped: query.ungrouped,
                order_by: query.order_by.clone(),
                source,
            };
            Ok(Some(Rc::new(Query::SimpleQuery(new_query))))
        } else {
            Ok(None)
        }
    }

    fn make_pre_aggregation_source(&mut self, pre_aggregation: &Rc<CompiledPreAggregation>) -> Result<Rc<PreAggregation>, CubeError> {
        let pre_aggregation_obj = self.query_tools.base_tools().get_pre_aggregation_by_name(pre_aggregation.cube_name.clone(), pre_aggregation.name.clone())?;
        if let Some(table_name) = &pre_aggregation_obj.static_data().table_name {
            
            let schema = LogicalSchema {
                time_dimensions: vec![],
                dimensions: pre_aggregation.dimensions.iter().cloned().chain(pre_aggregation.time_dimensions.iter().map(|(d, _)| d.clone())).collect(),
                measures: pre_aggregation.measures.iter().cloned().collect(),
                multiplied_measures: HashSet::new(),
            };
            let pre_aggregation = PreAggregation {
                name: pre_aggregation.name.clone(),
                time_dimensions: pre_aggregation.time_dimensions.clone(),
                dimensions: pre_aggregation.dimensions.clone(),
                measures: pre_aggregation.measures.clone(),
                schema: Rc::new(schema),
                external: pre_aggregation.external.unwrap_or_default(),
                granularity: pre_aggregation.granularity.clone(),
                table_name: table_name.clone(),
                cube_name: pre_aggregation.cube_name.clone(),
            };
            self.used_pre_aggregations.insert((pre_aggregation.cube_name.clone(), pre_aggregation.name.clone()), pre_aggregation_obj.clone());
            Ok(Rc::new(pre_aggregation))
        } else {
            Err(CubeError::internal(format!("Cannot find pre aggregation object for cube {} and name {}", pre_aggregation.cube_name, pre_aggregation.name)))
        }
    }
        
    fn is_schema_and_filters_match(&self, schema: &Rc<LogicalSchema>, filters: &Rc<LogicalFilter>, pre_aggregation: &CompiledPreAggregation) -> Result<bool, CubeError> {
        let helper = OptimizerHelper::new();
        let dimensions = helper.all_dimensions(schema, filters);
        let time_dimensions = helper.all_time_dimensions(schema, filters);

        let mut match_state = self.match_dimensions(&dimensions, pre_aggregation)?;
        for time_dimension in time_dimensions.iter() {
            match_state = match_state.combine(&self.match_time_dimension(time_dimension, pre_aggregation)?);
        }

        if match_state == MatchState::NotMatched {
            return Ok(false);
        }
        let all_measures = helper.all_measures(schema, filters);
        let measures_match = self.try_match_measures(&all_measures, pre_aggregation, match_state == MatchState::Partial)?;
        Ok(measures_match)
    }

    fn try_match_measures(&self, measures: &Vec<Rc<MemberSymbol>>, pre_aggregation: &CompiledPreAggregation, only_addictive: bool) -> Result<bool, CubeError> {
        let matcher = MeasureMatcher::new(pre_aggregation, only_addictive);
        for measure in measures.iter() {
            if !matcher.try_match(measure)? {
                return Ok(false);
            }
        }
        Ok(true)
    }


    fn match_dimensions(&self, dimensions: &Vec<Rc<MemberSymbol>>, pre_aggregation: &CompiledPreAggregation) -> Result<MatchState, CubeError> {
        let mut pre_aggrs_dims = pre_aggregation.dimensions.iter().map(|d| (d.full_name(), false)).collect::<HashMap<_, _>>();
        for dimension in dimensions.iter() {
            if let Some(found) = pre_aggrs_dims.get_mut(&dimension.full_name()) {
                *found = true;
            } else {
                return Ok(MatchState::NotMatched);
            }
        }
        if pre_aggrs_dims.values().all(|v| *v) {
            Ok(MatchState::Full)
        } else {
            Ok(MatchState::Partial)
        }
    }

    fn match_time_dimension(&self, time_dimension: &Rc<MemberSymbol>, pre_aggregation: &CompiledPreAggregation) -> Result<MatchState, CubeError> {
        let time_dimension = time_dimension.as_time_dimension()?;
        let result = if let Some((pre_aggregation_time_dimension, pre_aggr_granularity)) = pre_aggregation
            .time_dimensions
            .iter()
            .find(|(pre_agg_dim, _)| pre_agg_dim.full_name() == time_dimension.base_symbol().full_name())
        {
            if pre_aggr_granularity == time_dimension.granularity() {
                    MatchState::Full
                } else if pre_aggr_granularity.is_none() || GranularityHelper::is_predefined_granularity(pre_aggr_granularity.as_ref().unwrap()) {
                    let min_granularity = GranularityHelper::min_granularity(
                        &time_dimension.granularity(),
                        &pre_aggr_granularity,
                    )?;
                    if &min_granularity == pre_aggr_granularity {
                        MatchState::Partial
                    } else {
                        MatchState::NotMatched
                    }
                } else {
                    MatchState::NotMatched //TODO Custom granularities!!!
                }
        } else {
            MatchState::NotMatched
        };
        Ok(result)
    }
}