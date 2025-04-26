use super::CompiledPreAggregation;
use super::MatchState;
use crate::plan::{Filter, FilterItem};
use crate::planner::filter::BaseFilter;
use crate::planner::filter::FilterOperator;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_nodes::time_dimension;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::TimeDimensionSymbol;
use crate::planner::GranularityHelper;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct TimeDimensionMatcher {
    query_tools: Rc<QueryTools>,
}

impl TimeDimensionMatcher {
    pub fn new(query_tools: Rc<QueryTools>) -> Self {
        Self { query_tools }
    }

    pub fn try_match(
        &self,
        symbols: &Vec<Rc<MemberSymbol>>,
        filters: &Vec<FilterItem>,
        pre_aggregation: &CompiledPreAggregation,
    ) -> Result<MatchState, CubeError> {
        let mut pre_aggregation_time_dimensions = pre_aggregation
            .time_dimensions
            .iter()
            .map(|(dim, granularity)| (dim.full_name(), (granularity.clone(), false)))
            .collect::<HashMap<_, _>>();
        let mut result = MatchState::Full;
        for symbol in symbols.iter() {
            let dimension_match = self.try_match_time_dimension(
                symbol,
                pre_aggregation,
                false,
                &mut pre_aggregation_time_dimensions,
            )?;
            result = result.combine(&dimension_match);
            if result == MatchState::NotMatched {
                return Ok(result);
            }
        }
        for filter in filters.iter() {
            let filter_match = self.try_match_filter_item(
                filter,
                pre_aggregation,
                &mut pre_aggregation_time_dimensions,
            )?;
            result = result.combine(&filter_match);
            if result == MatchState::NotMatched {
                return Ok(result);
            }
        }
        let coverage_result = if pre_aggregation_time_dimensions.values().all(|v| v.1) {
            MatchState::Full
        } else {
            MatchState::Partial
        };

        Ok(result.combine(&coverage_result))
    }

    fn try_match_time_dimension(
        &self,
        dimension: &Rc<MemberSymbol>,
        pre_aggregation: &CompiledPreAggregation,
        is_filter: bool,
        pre_aggregation_time_dimensions: &mut HashMap<String, (Option<String>, bool)>,
    ) -> Result<MatchState, CubeError> {
        if let Ok(time_dimension) = dimension.as_time_dimension() {
            let granularity = if pre_aggregation.allow_non_strict_date_range_match {
                time_dimension.granularity().clone()
            } else {
                time_dimension.rollup_granularity(self.query_tools.clone())?
            };
            let base_dimension = time_dimension.base_symbol();
            let symbol_match = self.try_match_symbol(
                base_dimension,
                &granularity,
                is_filter,
                pre_aggregation_time_dimensions,
            )?;

            Ok(symbol_match)
        } else {
            Ok(MatchState::Full)
        }
    }

    fn try_match_filter_item(
        &self,
        filter_item: &FilterItem,
        pre_aggregation: &CompiledPreAggregation,
        pre_aggregation_time_dimensions: &mut HashMap<String, (Option<String>, bool)>,
    ) -> Result<MatchState, CubeError> {
        match filter_item {
            FilterItem::Item(filter) => {
                self.try_match_filter(filter, pre_aggregation, pre_aggregation_time_dimensions)
            }
            FilterItem::Group(group) => {
                let mut result = MatchState::Full;
                for item in group.items.iter() {
                    result = result.combine(&self.try_match_filter_item(
                        item,
                        pre_aggregation,
                        pre_aggregation_time_dimensions,
                    )?);
                }
                Ok(result)
            }
            FilterItem::Segment(_) => Ok(MatchState::Full),
        }
    }

    fn try_match_filter(
        &self,
        filter: &Rc<BaseFilter>,
        pre_aggregation: &CompiledPreAggregation,
        pre_aggregation_time_dimensions: &mut HashMap<String, (Option<String>, bool)>,
    ) -> Result<MatchState, CubeError> {
        if let Some(time_dimension) = filter.time_dimension_symbol() {
            self.try_match_time_dimension(
                &time_dimension,
                pre_aggregation,
                true,
                pre_aggregation_time_dimensions,
            )
        } else {
            Ok(MatchState::Full)
        }
    }

    fn try_match_symbol(
        &self,
        symbol: &Rc<MemberSymbol>,
        granularity: &Option<String>,
        is_filter: bool,
        pre_aggregation_time_dimensions: &mut HashMap<String, (Option<String>, bool)>,
    ) -> Result<MatchState, CubeError> {
        let mut result = match symbol.as_ref() {
            MemberSymbol::Dimension(dimension) => {
                if let Some(found) = pre_aggregation_time_dimensions.get_mut(&dimension.full_name())
                {
                    if !is_filter {
                        found.1 = true;
                    }
                    let pre_aggr_granularity = &found.0;
                    if granularity.is_none() || pre_aggr_granularity == granularity {
                        return Ok(MatchState::Full);
                    } else if pre_aggr_granularity.is_none()
                        || GranularityHelper::is_predefined_granularity(
                            pre_aggr_granularity.as_ref().unwrap(),
                        )
                    {
                        let min_granularity = GranularityHelper::min_granularity(
                            &granularity,
                            &pre_aggr_granularity,
                        )?;
                        if &min_granularity == pre_aggr_granularity {
                            return Ok(MatchState::Partial);
                        } else {
                            return Ok(MatchState::NotMatched);
                        }
                    } else {
                        return Ok(MatchState::NotMatched); //TODO Custom granularities!!!
                    }
                } else {
                    if dimension.owned_by_cube() {
                        return Ok(MatchState::NotMatched);
                    }
                }
                MatchState::Full
            }
            MemberSymbol::MemberExpression(member_expression) => MatchState::NotMatched, //TODO We not allow to use pre-aggregations with member expressions before sqlapi ready for it
            _ => return Ok(MatchState::NotMatched),
        };

        if symbol.get_dependencies().is_empty() {
            return Ok(MatchState::NotMatched);
        }

        for dep in symbol.get_dependencies() {
            let dep_match = self.try_match_symbol(
                &dep,
                granularity,
                is_filter,
                pre_aggregation_time_dimensions,
            )?;
            if dep_match == MatchState::NotMatched {
                return Ok(MatchState::NotMatched);
            }
            result = result.combine(&dep_match);
        }
        Ok(result)
    }
}
