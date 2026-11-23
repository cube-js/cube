use super::{CompiledPreAggregation, PreAggregationSource};
use crate::planner::filter::{BaseFilter, FilterGroupOperator, FilterItem};
use crate::planner::query_tools::QueryTools;
use crate::planner::DimensionSymbol;
use crate::planner::GranularityHelper;
use crate::planner::TimeDimensionSymbol;
use crate::planner::{MemberId, MemberSymbol};
use cubenativeutils::CubeError;
use std::collections::{HashMap, HashSet};
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

struct StoredTimeDimension {
    index: usize,
    symbol: Rc<TimeDimensionSymbol>,
    matched: bool,
}

pub struct DimensionMatcher<'a> {
    query_tools: Rc<QueryTools>,
    pre_aggregation: &'a CompiledPreAggregation,
    pre_aggregation_dimensions: HashMap<MemberId, bool>,
    pre_aggregation_time_dimensions: HashMap<MemberId, Vec<StoredTimeDimension>>,
    // Per stored entry of a time dimension: whether its column serves every
    // read of that dimension so far.
    readable_time_columns: HashMap<MemberId, Vec<bool>>,
    pre_aggregation_segments: HashMap<MemberId, bool>,
    projects_stored_dimension: bool,
    // Join keys a rollupJoin reads only from its legs' grain columns.
    truncated_join_keys: HashSet<MemberId>,
    // Set while matching the members a date-filtered member is computed from:
    // the filter renders their raw columns against database-timezone bounds.
    in_date_filter_dependencies: bool,
    result: MatchState,
}

impl<'a> DimensionMatcher<'a> {
    pub fn new(query_tools: Rc<QueryTools>, pre_aggregation: &'a CompiledPreAggregation) -> Self {
        let pre_aggregation_dimensions = pre_aggregation
            .dimensions
            .iter()
            .map(|d| (d.peel_refs().id().clone(), false))
            .collect();
        let mut pre_aggregation_time_dimensions =
            HashMap::<MemberId, Vec<StoredTimeDimension>>::new();
        for (index, dim) in pre_aggregation.time_dimensions.iter().enumerate() {
            if let Ok(symbol) = dim.as_time_dimension() {
                let key = symbol.base_symbol().peel_refs().id().clone();
                pre_aggregation_time_dimensions
                    .entry(key)
                    .or_default()
                    .push(StoredTimeDimension {
                        index,
                        symbol,
                        matched: false,
                    });
            }
        }
        let pre_aggregation_segments = pre_aggregation
            .segments
            .iter()
            .map(|s| (s.id().clone(), false))
            .collect();
        let truncated_join_keys = match pre_aggregation.source.as_ref() {
            PreAggregationSource::Join(join) => join.truncated_keys(),
            _ => HashSet::new(),
        };
        Self {
            query_tools,
            pre_aggregation,
            pre_aggregation_dimensions,
            pre_aggregation_time_dimensions,
            readable_time_columns: HashMap::new(),
            pre_aggregation_segments,
            projects_stored_dimension: false,
            truncated_join_keys,
            in_date_filter_dependencies: false,
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

        self.projects_stored_dimension = self.pre_aggregation_dimensions.values().any(|v| *v)
            || self
                .pre_aggregation_time_dimensions
                .values()
                .flatten()
                .any(|entry| entry.matched);

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

    /// Whether a selected dimension or time dimension reads a rollup column.
    pub fn projects_stored_dimension(&self) -> bool {
        self.projects_stored_dimension
    }

    pub fn result(mut self) -> MatchState {
        let dimension_coverage_result = if self.pre_aggregation_dimensions.values().all(|v| *v) {
            MatchState::Full
        } else {
            MatchState::Partial
        };
        self.result = self.result.combine(&dimension_coverage_result);
        let time_dimension_coverage_result = if self
            .pre_aggregation_time_dimensions
            .values()
            .all(|entries| entries.iter().all(|entry| entry.matched))
        {
            MatchState::Full
        } else {
            MatchState::Partial
        };
        self.result = self.result.combine(&time_dimension_coverage_result);
        let segment_coverage_result = if self.pre_aggregation_segments.values().all(|v| *v) {
            MatchState::Full
        } else {
            MatchState::Partial
        };
        self.result = self.result.combine(&segment_coverage_result);
        self.result
    }

    /// The pre-aggregation's time dimensions, each one stored at several grains
    /// narrowed to the finest column serving all its reads. `None` when no
    /// single column serves them all.
    pub fn read_time_dimensions(&self) -> Option<Vec<Rc<MemberSymbol>>> {
        let mut unread = HashSet::new();
        for (base, serves) in self.readable_time_columns.iter() {
            let entries = &self.pre_aggregation_time_dimensions[base];
            let mut chosen: Option<&StoredTimeDimension> = None;
            for (entry, _) in entries.iter().zip(serves).filter(|(_, serves)| **serves) {
                if chosen.is_none_or(|best| Self::is_finer(entry, best)) {
                    chosen = Some(entry);
                }
            }
            let chosen = chosen?;
            unread.extend(
                entries
                    .iter()
                    .filter(|entry| entry.index != chosen.index)
                    .map(|entry| entry.index),
            );
        }
        Some(
            self.pre_aggregation
                .time_dimensions
                .iter()
                .enumerate()
                .filter(|(index, _)| !unread.contains(index))
                .map(|(_, td)| td.clone())
                .collect(),
        )
    }

    // Unrelated grains are not ordered, so the earlier listed one is kept.
    fn is_finer(a: &StoredTimeDimension, b: &StoredTimeDimension) -> bool {
        let (a, b) = (&a.symbol, &b.symbol);
        if a.granularity().is_none() || a.granularity() == b.granularity() {
            return false;
        }
        GranularityHelper::min_granularity_for_time_dimensions(
            (a.granularity(), a),
            (b.granularity(), b),
        )
        .is_ok_and(|min| min == *a.granularity())
    }

    /// Whether the pre-aggregation can provide the member, without marking
    /// any of its dimensions as used.
    pub fn match_symbol(&mut self, symbol: &Rc<MemberSymbol>) -> Result<MatchState, CubeError> {
        self.try_match_symbol(symbol, false)
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
                self.try_match_time_dimension(time_dimension, add_to_matched_dimension, false)
            }
            MemberSymbol::MemberExpression(me) => {
                if let Some(found) = self.pre_aggregation_segments.get_mut(me.id()) {
                    if add_to_matched_dimension {
                        *found = true;
                    }
                    Ok(MatchState::Full)
                } else if let Some(referenced) = me.reference_member() {
                    // A bare `{member}` adds nothing on top of the member.
                    self.try_match_symbol(&referenced, add_to_matched_dimension)
                } else {
                    let dependencies = me.get_dependencies();
                    if dependencies.is_empty() {
                        Ok(MatchState::NotMatched)
                    } else {
                        let mut result = MatchState::Partial;
                        for dep in dependencies {
                            let dep_match =
                                self.try_match_symbol(&dep, add_to_matched_dimension)?;
                            if dep_match == MatchState::NotMatched {
                                return Ok(MatchState::NotMatched);
                            }
                            result = result.combine(&dep_match);
                        }
                        Ok(result)
                    }
                }
            }
            MemberSymbol::Ref(_) => {
                if symbol.is_measure() || symbol.is_multi_stage() {
                    return Ok(MatchState::NotMatched);
                }
                let mut result = MatchState::Full;
                for dep in symbol.get_dependencies() {
                    let dep_match = self.try_match_symbol(&dep, add_to_matched_dimension)?;
                    if dep_match == MatchState::NotMatched {
                        return Ok(MatchState::NotMatched);
                    }
                    result = result.combine(&dep_match);
                }
                Ok(result)
            }
            MemberSymbol::Measure(_) => Ok(MatchState::NotMatched),
        }
    }

    fn try_match_dimension(
        &mut self,
        dimension: &DimensionSymbol,
        add_to_matched_dimension: bool,
    ) -> Result<MatchState, CubeError> {
        if let Some(found) = self.pre_aggregation_dimensions.get_mut(dimension.id()) {
            if self.in_date_filter_dependencies && self.truncated_join_keys.contains(dimension.id())
            {
                return Ok(MatchState::NotMatched);
            }
            if add_to_matched_dimension {
                *found = true;
            }
            Ok(MatchState::Full)
        } else if self
            .pre_aggregation_time_dimensions
            .contains_key(dimension.id())
        {
            // A grain column holds the value truncated and in the query
            // timezone, so a raw read needs the member listed as a dimension.
            Ok(MatchState::NotMatched)
        } else if dimension.is_calc_group() {
            // Calc-group dimensions are virtual enumerations: they carry no
            // stored data, so any rollup can serve them. At query time the
            // value is either pinned by a filter (rendered as a literal) or
            // re-cross-joined over the rollup scan, exactly like over a raw
            // source.
            Ok(MatchState::Full)
        } else if dimension.owned_by_cube() {
            Ok(MatchState::NotMatched)
        } else if dimension.is_multi_stage() {
            // Multi-stage dimensions carry aggregate semantics that simple
            // dependency recursion would discard; match only if pre-agg
            // exposes the multi-stage dim itself.
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

    // `strict_range` keeps the range alignment check under a non-strict
    // pre-aggregation, whose opt-in covers a time dimension's own date range.
    fn try_match_time_dimension(
        &mut self,
        time_dimension: &TimeDimensionSymbol,
        add_to_matched_dimension: bool,
        strict_range: bool,
    ) -> Result<MatchState, CubeError> {
        let non_strict = self.pre_aggregation.allow_non_strict_date_range_match && !strict_range;
        // Read from a stored column rather than computed, so nothing derives
        // it from another bucket.
        let is_sql_granularity = |td: &TimeDimensionSymbol| {
            td.granularity_obj()
                .as_ref()
                .is_some_and(|granularity_obj| granularity_obj.calendar_sql().is_some())
        };
        let is_sql_defined_granularity = is_sql_granularity(time_dimension);

        let granularity = if non_strict {
            time_dimension.granularity().clone()
        } else {
            time_dimension.rollup_granularity(self.query_tools.clone())?
        };

        // Demoted, i.e. the range needs a finer grain than this one.
        if is_sql_defined_granularity && granularity != *time_dimension.granularity() {
            return Ok(MatchState::NotMatched);
        }

        let tz = self.query_tools.timezone();
        // Stored time dimensions are keyed by the member a view member
        // references.
        let base_symbol_id = time_dimension.base_symbol().peel_refs().id().clone();

        if let Some(entries) = self
            .pre_aggregation_time_dimensions
            .get_mut(&base_symbol_id)
        {
            // A `sql` grain is read as is and never derived, so it is only
            // served from a rollup storing that dimension at one grain.
            if entries.len() > 1
                && (is_sql_defined_granularity
                    || entries
                        .iter()
                        .any(|entry| is_sql_granularity(&entry.symbol)))
            {
                return Ok(MatchState::NotMatched);
            }
            // A stored `sql` grain is only ever itself: a default grain sharing
            // its name is not it.
            let is_exact = |td: &TimeDimensionSymbol| {
                granularity.is_none()
                    || (td.granularity() == &granularity
                        && is_sql_granularity(td) == is_sql_defined_granularity)
            };
            // A range read without a granularity is served by a column whose
            // grain both its bounds fall on.
            let range = match time_dimension.date_range_vec().as_deref() {
                Some([from, to]) if time_dimension.granularity().is_none() && !non_strict => {
                    Some((from.clone(), to.clone()))
                }
                _ => None,
            };
            let exact = entries.iter().position(|entry| is_exact(&entry.symbol));
            let mut serves = Vec::with_capacity(entries.len());
            for entry in entries.iter() {
                let td = &entry.symbol;
                let derives = if let Some((from, to)) = &range {
                    td.granularity_obj()
                        .as_ref()
                        .is_some_and(|grain| grain.bounds_on_grain(from, to, tz).unwrap_or(false))
                } else if is_exact(td) {
                    true
                } else if is_sql_defined_granularity
                    || td.granularity().is_none()
                    || is_sql_granularity(td)
                {
                    false
                } else {
                    let min = GranularityHelper::min_granularity_for_time_dimensions(
                        (&granularity, time_dimension),
                        (td.granularity(), td),
                    );
                    // With an exact column at hand, an unrelated grain just doesn't serve.
                    match min {
                        Err(_) if exact.is_some() => false,
                        min => min? == *td.granularity(),
                    }
                };
                serves.push(derives);
            }

            let exact = entries
                .iter()
                .zip(serves.iter())
                .position(|(entry, serves)| *serves && is_exact(&entry.symbol));
            let served_by = exact.map(|i| (i, MatchState::Full)).or_else(|| {
                serves
                    .iter()
                    .position(|serves| *serves)
                    .map(|i| (i, MatchState::Partial))
            });
            self.readable_time_columns
                .entry(base_symbol_id)
                .and_modify(|seen| {
                    for (seen, serves) in seen.iter_mut().zip(serves.iter()) {
                        *seen &= *serves;
                    }
                })
                .or_insert(serves);
            let Some((index, state)) = served_by else {
                return Ok(MatchState::NotMatched);
            };
            if add_to_matched_dimension {
                entries[index].matched = true;
            }
            Ok(state)
        } else {
            if time_dimension.owned_by_cube() {
                Ok(MatchState::NotMatched)
            } else if time_dimension.is_multi_stage() {
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
                let symbol = segment.member_evaluator();
                // An ad-hoc member-expression segment that references no members
                // (a constant like the `1 = 0` rlsAccessDenied segment RBAC injects
                // on access denial) is a filter pushable on top of any rollup, so it
                // doesn't need pre-aggregation coverage. Named segments still must be
                // covered and fall through to the regular matcher.
                if segment.is_member_expression() && symbol.get_dependencies().is_empty() {
                    return Ok(MatchState::Full);
                }
                self.try_match_symbol(&symbol, add_to_matched_dimension)
            }
        }
    }

    fn try_match_filter(
        &mut self,
        filter: &Rc<BaseFilter>,
        add_to_matched_dimension: bool,
    ) -> Result<MatchState, CubeError> {
        let add_to_matched_dimension = add_to_matched_dimension && filter.is_single_value_equal();
        let symbol = if let Some(time_dimension) = filter.time_dimension_symbol() {
            time_dimension
        } else if filter.operation().is_date_bounded() {
            return self.try_match_date_filter(filter, add_to_matched_dimension);
        } else {
            filter.member_evaluator().clone()
        };
        let res = self.try_match_symbol(&symbol, add_to_matched_dimension)?;
        Ok(res)
    }

    // A date filter reads the raw column of its member, or of the members it
    // is computed from, and the grain column of a member stored only truncated.
    fn try_match_date_filter(
        &mut self,
        filter: &BaseFilter,
        add_to_matched_dimension: bool,
    ) -> Result<MatchState, CubeError> {
        let member = filter.member_evaluator();
        let id = member.peel_refs().id().clone();
        let truncated_key = self.truncated_join_keys.contains(&id);
        let stored_truncated = self.pre_aggregation_time_dimensions.contains_key(&id);
        if truncated_key && !stored_truncated {
            // No time dimension states the key's grain to check the bounds against.
            return self.try_match_symbol(&member, add_to_matched_dimension);
        }
        let stored_raw = self.pre_aggregation_dimensions.contains_key(&id) && !truncated_key;
        if stored_raw || !stored_truncated {
            self.in_date_filter_dependencies = true;
            let res = self.try_match_symbol(&member, add_to_matched_dimension);
            self.in_date_filter_dependencies = false;
            return res;
        }
        match self.date_filter_as_time_dimension(filter) {
            // The grain column holds no raw value to stand for a misaligned range.
            Some(read) => self.try_match_time_dimension(&read, add_to_matched_dimension, true),
            None => Ok(MatchState::NotMatched),
        }
    }

    // A grain column serves a date filter whose bounds fall on its grain, as
    // it would a time dimension's range.
    fn date_filter_as_time_dimension(
        &self,
        filter: &BaseFilter,
    ) -> Option<Rc<TimeDimensionSymbol>> {
        let member = filter.member_evaluator();
        let id = member.peel_refs().id().clone();
        if !self.pre_aggregation_time_dimensions.contains_key(&id) {
            return None;
        }
        let date_range = filter.typed_filter().date_bounds()?;
        Some(TimeDimensionSymbol::new(
            member,
            None,
            None,
            Some(date_range),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cube_bridge::member_expression::MemberExpressionExpressionDef;
    use crate::cube_bridge::member_sql::MemberSql;
    use crate::cube_bridge::options_member::OptionsMember;
    use crate::logical_plan::optimizers::pre_aggregation::{
        PreAggregationFullName, PreAggregationsCompiler,
    };
    use crate::planner::{CubeId, MemberExpressionExpression, MemberExpressionSymbol};
    use crate::test_fixtures::cube_bridge::{
        MockMemberExpressionDefinition, MockMemberSql, MockSchema,
    };
    use crate::test_fixtures::test_utils::TestContext;
    use indoc::indoc;
    use std::rc::Rc;

    fn create_test_context() -> TestContext {
        let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml");
        TestContext::new(schema).unwrap()
    }

    /// Builds an ad-hoc member-expression segment (no registered `segments:`
    /// path) from constant SQL — mirrors the `rlsAccessDenied` `1 = 0` segment.
    fn const_expr_segment(name: &str, cube: &str, sql: &str) -> OptionsMember {
        let member_sql: Rc<dyn MemberSql> = Rc::new(MockMemberSql::new(sql).unwrap());
        let expr = MockMemberExpressionDefinition::builder()
            .expression_name(Some(name.to_string()))
            .cube_name(Some(cube.to_string()))
            .expression(MemberExpressionExpressionDef::Sql(member_sql))
            .build();
        OptionsMember::MemberExpression(Rc::new(expr))
    }

    fn match_pre_agg(ctx: &TestContext, pre_agg_name: &str, query_yaml: &str) -> MatchState {
        match_pre_agg_with_segments(ctx, pre_agg_name, query_yaml, vec![])
    }

    fn match_pre_agg_with_segments(
        ctx: &TestContext,
        pre_agg_name: &str,
        query_yaml: &str,
        extra_segments: Vec<OptionsMember>,
    ) -> MatchState {
        let cube_names = vec![CubeId::cube("orders")];
        let mut compiler =
            PreAggregationsCompiler::try_new(ctx.query_tools().clone(), &cube_names).unwrap();
        let name = PreAggregationFullName::new("orders".to_string(), pre_agg_name.to_string());
        let pre_agg = compiler.compile_pre_aggregation(&name).unwrap();

        let qp = ctx
            .create_query_properties_with_segments(query_yaml, extra_segments)
            .unwrap();
        let mut matcher = DimensionMatcher::new(ctx.query_tools().query_tools().clone(), &pre_agg);
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

    #[test]
    fn test_segment_full_match() {
        let ctx = create_test_context();
        assert_eq!(
            match_pre_agg(
                &ctx,
                "segment_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    dimensions:
                      - orders.status
                    segments:
                      - orders.high_priority
                    time_dimensions:
                      - dimension: orders.created_at
                        granularity: day
                "},
            ),
            MatchState::Full,
        );
    }

    #[test]
    fn test_reference_member_expression_segment_full_match() {
        let ctx = create_test_context();
        // A query expression that only references a rollup's segment, as the
        // SQL API sends a segment filter, matches like the segment itself.
        assert_eq!(
            match_pre_agg_with_segments(
                &ctx,
                "segment_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    dimensions:
                      - orders.status
                    time_dimensions:
                      - dimension: orders.created_at
                        granularity: day
                "},
                vec![const_expr_segment(
                    "high_priority",
                    "orders",
                    "{orders.high_priority}"
                )],
            ),
            MatchState::Full,
        );
    }

    #[test]
    fn test_segment_partial_match_unused_segment() {
        let ctx = create_test_context();
        // No segment in query, but pre-agg has segment => partial (unused segment coverage)
        assert_eq!(
            match_pre_agg(
                &ctx,
                "segment_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    dimensions:
                      - orders.status
                    time_dimensions:
                      - dimension: orders.created_at
                        granularity: day
                "},
            ),
            MatchState::Partial,
        );
    }

    fn match_expression_dimension(ctx: &TestContext, sql: &str) -> MatchState {
        let cube = CubeId::cube("orders");
        let mut compiler = ctx.query_tools().compiler().borrow_mut();
        let sql_call = compiler
            .compile_sql_call(&cube, Rc::new(MockMemberSql::new(sql).unwrap()))
            .unwrap();
        let cube_symbol = compiler
            .add_cube_table_evaluator(cube.clone(), vec![])
            .unwrap();
        let city = compiler
            .add_dimension_evaluator("orders.city".to_string())
            .unwrap();
        drop(compiler);
        let expression = MemberSymbol::new_member_expression(
            MemberExpressionSymbol::try_new(
                cube_symbol,
                "status_expression".to_string(),
                MemberExpressionExpression::SqlCall(sql_call),
                None,
                None,
                vec![cube.clone()],
            )
            .unwrap(),
        );

        let mut pre_aggregations =
            PreAggregationsCompiler::try_new(ctx.query_tools().clone(), &vec![cube]).unwrap();
        let pre_agg = pre_aggregations
            .compile_pre_aggregation(&PreAggregationFullName::new(
                "orders".to_string(),
                "main_rollup".to_string(),
            ))
            .unwrap();
        let mut matcher = DimensionMatcher::new(ctx.query_tools().query_tools().clone(), &pre_agg);
        matcher
            .try_match(&vec![expression, city], &vec![], &vec![], &vec![], &vec![])
            .unwrap();
        matcher.result()
    }

    #[test]
    fn test_reference_member_expression_dimension_full_match() {
        let ctx = create_test_context();
        // A bare reference reads the rollup column as it is; any other
        // expression over it may merge the rollup's groups.
        assert_eq!(
            match_expression_dimension(&ctx, "{orders.status}"),
            MatchState::Full
        );
        assert_eq!(
            match_expression_dimension(&ctx, "UPPER({orders.status})"),
            MatchState::Partial
        );
    }

    #[test]
    fn test_constant_member_expression_segment_full_match() {
        let ctx = create_test_context();
        // The ad-hoc `1 = 0` rlsAccessDenied segment references no members, so it's a
        // constant filter pushable on top of any rollup and imposes no coverage
        // requirement: with both rollup dimensions selected the match stays Full
        // (without the fix the bare segment forces NotMatched).
        assert_eq!(
            match_pre_agg_with_segments(
                &ctx,
                "main_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    dimensions:
                      - orders.status
                      - orders.city
                "},
                vec![const_expr_segment("rlsAccessDenied", "orders", "1 = 0")],
            ),
            MatchState::Full,
        );
    }

    #[test]
    fn test_segment_not_matched_missing_in_pre_agg() {
        let ctx = create_test_context();
        // Segment in query, but pre-agg without segments => NotMatched
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
                    segments:
                      - orders.high_priority
                "},
            ),
            MatchState::NotMatched,
        );
    }

    #[test]
    fn test_expression_segment_matched_via_dimensions() {
        let ctx = create_test_context();
        // status_filter depends on {CUBE.status}, status is in main_rollup dimensions
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
                    segments:
                      - orders.status_filter
                "},
            ),
            MatchState::Partial,
        );
    }

    #[test]
    fn test_expression_segment_not_matched_missing_dependency() {
        let ctx = create_test_context();
        // status_filter depends on {CUBE.status}, but daily_countries_rollup has no status dimension
        assert_eq!(
            match_pre_agg(
                &ctx,
                "daily_countries_rollup",
                indoc! {"
                    measures:
                      - orders.count
                    dimensions:
                      - orders.country
                    segments:
                      - orders.status_filter
                    time_dimensions:
                      - dimension: orders.created_at
                        granularity: day
                "},
            ),
            MatchState::NotMatched,
        );
    }

    #[test]
    fn test_plain_segment_not_matched_via_dimensions() {
        let ctx = create_test_context();
        // high_priority has no {CUBE.x} deps, main_rollup has no segments => NotMatched
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
                    segments:
                      - orders.high_priority
                "},
            ),
            MatchState::NotMatched,
        );
    }
}
