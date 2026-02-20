use super::CompiledPreAggregation;
use crate::plan::filter::FilterGroupOperator;
use crate::plan::FilterItem;
use crate::planner::filter::BaseFilter;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::DimensionSymbol;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::TimeDimensionSymbol;
use crate::planner::GranularityHelper;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

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
        if matches!(self, MatchState::Partial) || matches!(other, MatchState::Partial) {
            return MatchState::Partial;
        }
        MatchState::Full
    }
}

pub struct DimensionMatcher<'a> {
    query_tools: Rc<QueryTools>,
    pre_aggregation: &'a CompiledPreAggregation,
    pre_aggregation_dimensions: HashMap<String, bool>,
    pre_aggregation_time_dimensions: HashMap<String, (Option<Rc<TimeDimensionSymbol>>, bool)>,
    result: MatchState,
}

impl<'a> DimensionMatcher<'a> {
    pub fn new(query_tools: Rc<QueryTools>, pre_aggregation: &'a CompiledPreAggregation) -> Self {
        let pre_aggregation_dimensions = pre_aggregation
            .dimensions
            .iter()
            .map(|d| (d.full_name(), false))
            .collect();
        let pre_aggregation_time_dimensions = pre_aggregation
            .time_dimensions
            .iter()
            .map(|dim| {
                if let Ok(td) = dim.as_time_dimension() {
                    (td.base_symbol().full_name(), (Some(td), false))
                } else {
                    (dim.full_name(), (None, false))
                }
            })
            .collect::<HashMap<_, _>>();
        Self {
            query_tools,
            pre_aggregation,
            pre_aggregation_dimensions,
            pre_aggregation_time_dimensions,
            result: MatchState::Full,
        }
    }

    pub fn try_match(
        &mut self,
        dimensions: &Vec<Rc<MemberSymbol>>,
        time_dimensions: &Vec<Rc<MemberSymbol>>,
        filters: &Vec<FilterItem>,
        time_dimension_filters: &Vec<FilterItem>,
        segments: &Vec<FilterItem>,
    ) -> Result<(), CubeError> {
        for dimension in dimensions.iter() {
            let dimension_match = self.try_match_symbol(dimension, true)?;
            self.result = self.result.combine(&dimension_match);
            if self.result == MatchState::NotMatched {
                return Ok(());
            }
        }
        for time_dimension in time_dimensions.iter() {
            let time_dimension_match = self.try_match_symbol(time_dimension, true)?;
            self.result = self.result.combine(&time_dimension_match);
            if self.result == MatchState::NotMatched {
                return Ok(());
            }
        }

        for filter in filters.iter() {
            let filter_match = self.try_match_filter_item(filter, true)?;
            self.result = self.result.combine(&filter_match);
            if self.result == MatchState::NotMatched {
                return Ok(());
            }
        }

        for filter in time_dimension_filters.iter() {
            let filter_match = self.try_match_filter_item(filter, true)?;
            self.result = self.result.combine(&filter_match);
            if self.result == MatchState::NotMatched {
                return Ok(());
            }
        }

        for segment in segments.iter() {
            let segment_match = self.try_match_filter_item(segment, true)?;
            self.result = self.result.combine(&segment_match);
            if self.result == MatchState::NotMatched {
                return Ok(());
            }
        }
        Ok(())
    }

    pub fn result(mut self) -> MatchState {
        let dimension_coverage_result = if self.pre_aggregation_dimensions.values().all(|v| *v) {
            MatchState::Full
        } else {
            MatchState::Partial
        };
        self.result = self.result.combine(&dimension_coverage_result);
        let time_dimension_coverage_result =
            if self.pre_aggregation_time_dimensions.values().all(|v| v.1) {
                MatchState::Full
            } else {
                MatchState::Partial
            };
        self.result = self.result.combine(&time_dimension_coverage_result);
        self.result
    }

    fn try_match_symbol(
        &mut self,
        symbol: &Rc<MemberSymbol>,
        add_to_matched_dimension: bool,
    ) -> Result<MatchState, CubeError> {
        match symbol.as_ref() {
            MemberSymbol::Dimension(dimension) => {
                self.try_match_dimension(dimension, add_to_matched_dimension)
            }
            MemberSymbol::TimeDimension(time_dimension) => {
                self.try_match_time_dimension(time_dimension, add_to_matched_dimension)
            }
            MemberSymbol::MemberExpression(_member_expression) => Ok(MatchState::NotMatched), //TODO We don't allow to use pre-aggregations with member expressions before SQL API is ready for it
            _ => Ok(MatchState::NotMatched),
        }
    }

    fn try_match_dimension(
        &mut self,
        dimension: &DimensionSymbol,
        add_to_matched_dimension: bool,
    ) -> Result<MatchState, CubeError> {
        if let Some(found) = self
            .pre_aggregation_dimensions
            .get_mut(&dimension.full_name())
        {
            if add_to_matched_dimension {
                *found = true;
            }
            Ok(MatchState::Full)
        } else if dimension.owned_by_cube() {
            Ok(MatchState::NotMatched)
        } else {
            let dependencies = dimension.get_dependencies();
            if dependencies.is_empty() {
                Ok(MatchState::NotMatched)
            } else {
                let mut result = if dimension.is_reference() {
                    MatchState::Full
                } else {
                    MatchState::Partial
                };
                for dep in dimension.get_dependencies() {
                    let dep_match = self.try_match_symbol(&dep, add_to_matched_dimension)?;
                    if dep_match == MatchState::NotMatched {
                        return Ok(MatchState::NotMatched);
                    }
                    result = result.combine(&dep_match);
                }
                Ok(result)
            }
        }
    }

    fn try_match_time_dimension(
        &mut self,
        time_dimension: &TimeDimensionSymbol,
        add_to_matched_dimension: bool,
    ) -> Result<MatchState, CubeError> {
        let granularity = if self.pre_aggregation.allow_non_strict_date_range_match {
            if let Some(granularity) = time_dimension.granularity_obj() {
                granularity.min_granularity()?
            } else {
                time_dimension.granularity().clone()
            }
        } else {
            time_dimension.rollup_granularity(self.query_tools.clone())?
        };
        let base_symbol_name = time_dimension.base_symbol().full_name();

        if let Some(found) = self
            .pre_aggregation_time_dimensions
            .get_mut(&base_symbol_name)
        {
            if add_to_matched_dimension {
                found.1 = true;
            }

            let pre_agg_td = &found.0;
            let pre_aggr_granularity = if let Some(pre_agg_td) = pre_agg_td {
                pre_agg_td.granularity().clone()
            } else {
                None
            };

            if granularity.is_none() || pre_aggr_granularity == granularity {
                Ok(MatchState::Full)
            } else if pre_aggr_granularity.is_none() {
                Ok(MatchState::NotMatched)
            } else if let Some(pre_agg_td) = pre_agg_td {
                let min_granularity = GranularityHelper::min_granularity_for_time_dimensions(
                    (&granularity, time_dimension),
                    (&pre_aggr_granularity, &pre_agg_td),
                )?;

                if min_granularity == pre_aggr_granularity {
                    Ok(MatchState::Partial)
                } else {
                    Ok(MatchState::NotMatched)
                }
            } else {
                Ok(MatchState::NotMatched)
            }
        } else {
            if time_dimension.owned_by_cube() {
                Ok(MatchState::NotMatched)
            } else {
                let mut result = if time_dimension.is_reference() {
                    MatchState::Full
                } else {
                    MatchState::Partial
                };
                for dep in time_dimension.get_dependencies_as_time_dimensions() {
                    let dep_match = self.try_match_symbol(&dep, add_to_matched_dimension)?;
                    if dep_match == MatchState::NotMatched {
                        return Ok(MatchState::NotMatched);
                    }
                    result = result.combine(&dep_match);
                }
                Ok(result)
            }
        }
    }

    fn try_match_filter_item(
        &mut self,
        filter_item: &FilterItem,
        add_to_matched_dimension: bool,
    ) -> Result<MatchState, CubeError> {
        match filter_item {
            FilterItem::Item(filter) => self.try_match_filter(filter, add_to_matched_dimension),
            FilterItem::Group(group) => {
                let add_to_matched_dimension =
                    add_to_matched_dimension && group.operator == FilterGroupOperator::And;
                let mut result = MatchState::Full;
                for item in group.items.iter() {
                    result = result
                        .combine(&self.try_match_filter_item(item, add_to_matched_dimension)?);
                }
                Ok(result)
            }
            FilterItem::Segment(segment) => {
                self.try_match_symbol(&segment.member_evaluator(), add_to_matched_dimension)
            }
        }
    }

    fn try_match_filter(
        &mut self,
        filter: &Rc<BaseFilter>,
        add_to_matched_dimension: bool,
    ) -> Result<MatchState, CubeError> {
        let symbol = if let Some(time_dimension) = filter.time_dimension_symbol() {
            time_dimension
        } else {
            filter.member_evaluator().clone()
        };
        let add_to_matched_dimension = add_to_matched_dimension && filter.is_single_value_equal();
        let res = self.try_match_symbol(&symbol, add_to_matched_dimension)?;
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::optimizers::pre_aggregation::{
        PreAggregationFullName, PreAggregationsCompiler,
    };
    use crate::test_fixtures::cube_bridge::MockSchema;
    use crate::test_fixtures::test_utils::TestContext;
    use indoc::indoc;

    fn create_test_context() -> TestContext {
        let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml");
        TestContext::new(schema).unwrap()
    }

    fn match_pre_agg(ctx: &TestContext, pre_agg_name: &str, query_yaml: &str) -> MatchState {
        let cube_names = vec!["orders".to_string()];
        let mut compiler =
            PreAggregationsCompiler::try_new(ctx.query_tools().clone(), &cube_names).unwrap();
        let name = PreAggregationFullName::new("orders".to_string(), pre_agg_name.to_string());
        let pre_agg = compiler.compile_pre_aggregation(&name).unwrap();

        let qp = ctx.create_query_properties(query_yaml).unwrap();
        let mut matcher = DimensionMatcher::new(ctx.query_tools().clone(), &pre_agg);
        matcher
            .try_match(
                qp.dimensions(),
                qp.time_dimensions(),
                qp.dimensions_filters(),
                qp.time_dimensions_filters(),
                qp.segments(),
            )
            .unwrap();
        matcher.result()
    }

    #[test]
    fn test_full_match_dimensions() {
        let ctx = create_test_context();
        assert_eq!(
            match_pre_agg(
                &ctx,
                "main_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    dimensions:
                      - orders.status
                      - orders.city
                "},
            ),
            MatchState::Full,
        );
    }

    #[test]
    fn test_partial_match_unused_dimension() {
        let ctx = create_test_context();
        assert_eq!(
            match_pre_agg(
                &ctx,
                "main_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    dimensions:
                      - orders.status
                "},
            ),
            MatchState::Partial,
        );
    }

    #[test]
    fn test_not_matched_missing_dimension() {
        let ctx = create_test_context();
        assert_eq!(
            match_pre_agg(
                &ctx,
                "main_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    dimensions:
                      - orders.id
                "},
            ),
            MatchState::NotMatched,
        );
    }

    #[test]
    fn test_time_dimension_matching() {
        let ctx = create_test_context();

        assert_eq!(
            match_pre_agg(
                &ctx,
                "daily_countries_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    dimensions:
                      - orders.country
                    time_dimensions:
                      - dimension: orders.created_at
                        granularity: day
                "},
            ),
            MatchState::Full,
        );

        assert_eq!(
            match_pre_agg(
                &ctx,
                "daily_countries_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    dimensions:
                      - orders.country
                    time_dimensions:
                      - dimension: orders.created_at
                        granularity: month
                "},
            ),
            MatchState::Partial,
        );

        assert_eq!(
            match_pre_agg(
                &ctx,
                "daily_countries_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    dimensions:
                      - orders.country
                    time_dimensions:
                      - dimension: orders.created_at
                        granularity: hour
                "},
            ),
            MatchState::NotMatched,
        );

        assert_eq!(
            match_pre_agg(
                &ctx,
                "daily_countries_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    dimensions:
                      - orders.country
                "},
            ),
            MatchState::Partial,
        );

        assert_eq!(
            match_pre_agg(
                &ctx,
                "daily_countries_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    time_dimensions:
                      - dimension: orders.created_at
                        granularity: day
                "},
            ),
            MatchState::Partial,
        );

        assert_eq!(
            match_pre_agg(
                &ctx,
                "daily_countries_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    dimensions:
                      - orders.status
                    time_dimensions:
                      - dimension: orders.updated_at
                        granularity: day
                "},
            ),
            MatchState::NotMatched,
        );

        assert_eq!(
            match_pre_agg(
                &ctx,
                "daily_countries_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    dimensions:
                      - orders.city
                    time_dimensions:
                      - dimension: orders.created_at
                        granularity: day
                "},
            ),
            MatchState::NotMatched,
        );
    }

    #[test]
    fn test_reference_dimension_full_match() {
        let ctx = create_test_context();
        assert_eq!(
            match_pre_agg(
                &ctx,
                "main_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    dimensions:
                      - orders.status_ref
                      - orders.city
                "},
            ),
            MatchState::Full,
        );
    }

    #[test]
    fn test_compound_dimension_matching() {
        let ctx = create_test_context();
        let query = indoc! {"
            measures:
              - orders.count
            dimensions:
              - orders.location_and_status
        "};

        assert_eq!(
            match_pre_agg(&ctx, "compound_dimension_rollup", query),
            MatchState::Full,
        );

        assert_eq!(
            match_pre_agg(&ctx, "base_dimensions_rollup", query),
            MatchState::Partial,
        );

        assert_eq!(
            match_pre_agg(&ctx, "mixed_dimensions_rollup", query),
            MatchState::Partial,
        );
    }

    #[test]
    fn test_filter_matching() {
        let ctx = create_test_context();

        assert_eq!(
            match_pre_agg(
                &ctx,
                "main_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    dimensions:
                      - orders.city
                    filters:
                      - dimension: orders.status
                        operator: equals
                        values:
                          - shipped
                "},
            ),
            MatchState::Full,
        );

        assert_eq!(
            match_pre_agg(
                &ctx,
                "main_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    dimensions:
                      - orders.city
                    filters:
                      - dimension: orders.status
                        operator: contains
                        values:
                          - ship
                "},
            ),
            MatchState::Partial,
        );

        assert_eq!(
            match_pre_agg(
                &ctx,
                "main_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    dimensions:
                      - orders.status
                      - orders.city
                    filters:
                      - dimension: orders.id
                        operator: gt
                        values:
                          - \"5\"
                "},
            ),
            MatchState::NotMatched,
        );

        assert_eq!(
            match_pre_agg(
                &ctx,
                "main_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    filters:
                      - or:
                          - dimension: orders.status
                            operator: equals
                            values:
                              - shipped
                          - dimension: orders.status
                            operator: equals
                            values:
                              - processing
                "},
            ),
            MatchState::Partial,
        );

        assert_eq!(
            match_pre_agg(
                &ctx,
                "main_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    filters:
                      - and:
                          - dimension: orders.status
                            operator: equals
                            values:
                              - shipped
                          - dimension: orders.city
                            operator: equals
                            values:
                              - New York
                "},
            ),
            MatchState::Full,
        );
    }
}
