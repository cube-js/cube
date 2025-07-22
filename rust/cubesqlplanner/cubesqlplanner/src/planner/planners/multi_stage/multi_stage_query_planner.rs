use super::{
    MultiStageAppliedState, MultiStageInodeMember, MultiStageInodeMemberType,
    MultiStageLeafMemberType, MultiStageMember, MultiStageMemberQueryPlanner, MultiStageMemberType,
    MultiStageQueryDescription, RollingWindowDescription, TimeSeriesDescription,
};
use crate::cube_bridge::measure_definition::RollingWindow;
use crate::logical_plan::*;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::has_multi_stage_members;
use crate::planner::sql_evaluator::collectors::member_childs;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::GranularityHelper;
use crate::planner::QueryProperties;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub struct MultiStageQueryPlanner {
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
}

impl MultiStageQueryPlanner {
    pub fn new(query_tools: Rc<QueryTools>, query_properties: Rc<QueryProperties>) -> Self {
        Self {
            query_tools,
            query_properties,
        }
    }

    pub fn plan_queries(
        &self,
    ) -> Result<
        (
            Vec<Rc<LogicalMultiStageMember>>,
            Vec<Rc<MultiStageSubqueryRef>>,
        ),
        CubeError,
    > {
        let multi_stage_members = self
            .query_properties
            .all_members(false)
            .into_iter()
            .filter_map(|memb| -> Option<Result<_, CubeError>> {
                match has_multi_stage_members(&memb, false) {
                    Ok(true) => Some(Ok(memb)),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        if multi_stage_members.is_empty() {
            return Ok((vec![], vec![]));
        }
        let mut descriptions = Vec::new();
        let state = MultiStageAppliedState::new(
            self.query_properties.time_dimensions().clone(),
            self.query_properties.dimensions().clone(),
            self.query_properties.time_dimensions_filters().clone(),
            self.query_properties.dimensions_filters().clone(),
            vec![], //TODO: We do not pass measures filters to CTE queries. This seems correct, but we need to check
            self.query_properties.segments().clone(),
        );

        let top_level_ctes = multi_stage_members
            .into_iter()
            .map(|memb| -> Result<_, CubeError> {
                let description =
                    self.make_queries_descriptions(memb.clone(), state.clone(), &mut descriptions)?;
                let result = (
                    description.alias().clone(),
                    vec![description.member_node().clone()],
                );
                Ok(result)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let all_queries = descriptions
            .into_iter()
            .map(|descr| -> Result<_, CubeError> {
                let planner = MultiStageMemberQueryPlanner::new(
                    self.query_tools.clone(),
                    self.query_properties.clone(),
                    descr.clone(),
                );
                let res = planner.plan_logical_query()?;
                Ok(res)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let top_level_ctes = top_level_ctes
            .iter()
            .map(|(alias, symbols)| {
                Rc::new(MultiStageSubqueryRef {
                    name: alias.clone(),
                    symbols: symbols.clone(),
                })
            })
            .collect_vec();

        Ok((all_queries, top_level_ctes))
    }

    fn create_multi_stage_inode_member(
        &self,
        base_member: Rc<MemberSymbol>,
    ) -> Result<(MultiStageInodeMember, bool), CubeError> {
        let inode = if let Ok(measure) = base_member.as_measure() {
            let member_type = if measure.measure_type() == "rank" {
                MultiStageInodeMemberType::Rank
            } else if !measure.is_calculated() {
                MultiStageInodeMemberType::Aggregate
            } else {
                MultiStageInodeMemberType::Calculate
            };

            let time_shift = measure.time_shift().clone();

            let is_ungrupped = match &member_type {
                MultiStageInodeMemberType::Rank | MultiStageInodeMemberType::Calculate => true,
                _ => self.query_properties.ungrouped(),
            };

            let reduce_by = measure.reduce_by().clone().unwrap_or_default();
            let add_group_by = measure.add_group_by().clone().unwrap_or_default();
            let group_by = measure.group_by().clone();
            (
                MultiStageInodeMember::new(
                    member_type,
                    reduce_by,
                    add_group_by,
                    group_by,
                    time_shift,
                ),
                is_ungrupped,
            )
        } else {
            (
                MultiStageInodeMember::new(
                    MultiStageInodeMemberType::Calculate,
                    vec![],
                    vec![],
                    None,
                    None,
                ),
                self.query_properties.ungrouped(),
            )
        };
        Ok(inode)
    }

    fn make_queries_descriptions(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<MultiStageAppliedState>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
    ) -> Result<Rc<MultiStageQueryDescription>, CubeError> {
        let member = member.resolve_reference_chain();
        let member_name = member.full_name();
        if let Some(exists) = descriptions
            .iter()
            .find(|q| q.is_match_member_and_state(&member, &state))
        {
            return Ok(exists.clone());
        };

        if let Some(rolling_window_query) =
            self.try_plan_rolling_window(member.clone(), state.clone(), descriptions)?
        {
            return Ok(rolling_window_query);
        }

        let childs = member_childs(&member, true)?;

        let has_multi_stage_members = has_multi_stage_members(&member, false)?;
        let description = if childs.is_empty() || !has_multi_stage_members {
            if has_multi_stage_members {
                return Err(CubeError::internal(format!(
                    "Leaf multi stage query cannot contain multi stage member"
                )));
            }

            let alias = format!("cte_{}", descriptions.len());
            MultiStageQueryDescription::new(
                MultiStageMember::new(
                    MultiStageMemberType::Leaf(MultiStageLeafMemberType::Measure),
                    member.clone(),
                    self.query_properties.ungrouped(),
                    false,
                ),
                state.clone(),
                vec![],
                alias.clone(),
            )
        } else {
            let (multi_stage_member, is_ungrupped) =
                self.create_multi_stage_inode_member(member.clone())?;

            let dimensions_to_add = multi_stage_member.add_group_by_symbols();

            let new_state = if !dimensions_to_add.is_empty()
                || multi_stage_member.time_shift().is_some()
                || state.has_filters_for_member(&member_name)
            {
                let mut new_state = state.clone_state();
                if !dimensions_to_add.is_empty() {
                    new_state.add_dimensions(dimensions_to_add.clone());
                }
                if let Some(time_shift) = multi_stage_member.time_shift() {
                    new_state.add_time_shifts(time_shift.clone())?;
                }
                if state.has_filters_for_member(&member_name) {
                    new_state.remove_filter_for_member(&member_name);
                }
                Rc::new(new_state)
            } else {
                state.clone()
            };

            let input = childs
                .into_iter()
                .map(
                    |child| -> Result<Rc<MultiStageQueryDescription>, CubeError> {
                        self.make_queries_descriptions(child, new_state.clone(), descriptions)
                    },
                )
                .collect::<Result<Vec<_>, _>>()?;

            let alias = format!("cte_{}", descriptions.len());
            MultiStageQueryDescription::new(
                MultiStageMember::new(
                    MultiStageMemberType::Inode(multi_stage_member),
                    member,
                    is_ungrupped,
                    false,
                ),
                state.clone(),
                input,
                alias.clone(),
            )
        };

        descriptions.push(description.clone());
        Ok(description)
    }

    pub fn try_plan_rolling_window(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<MultiStageAppliedState>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
    ) -> Result<Option<Rc<MultiStageQueryDescription>>, CubeError> {
        if let Ok(measure) = member.as_measure() {
            if measure.is_cumulative() {
                let rolling_window = if let Some(rolling_window) = measure.rolling_window() {
                    rolling_window.clone()
                } else {
                    RollingWindow {
                        trailing: None,
                        leading: None,
                        offset: None,
                        rolling_type: None,
                        granularity: None,
                    }
                };

                if !measure.is_multi_stage() {
                    let childs = member_childs(&member, true)?;
                    let measures = childs
                        .iter()
                        .filter(|s| s.as_measure().is_ok())
                        .collect_vec();
                    if !measures.is_empty() {
                        return Err(CubeError::user(
                            format!("Measure {} references another measures ({}). In this case, {} must have multi_stage: true defined",
                            member.full_name(),
                            measures.into_iter().map(|m| m.full_name()).join(", "),
                            member.full_name(),
                                        ),
                        ));
                    }
                }

                let ungrouped = measure.is_rolling_window() && !measure.is_addictive();

                let mut time_dimensions = self
                    .query_properties
                    .time_dimensions()
                    .iter()
                    .map(|d| d.as_time_dimension())
                    .collect::<Result<Vec<_>, _>>()?;
                for dim in self.query_properties.dimensions() {
                    let dim = dim.clone().resolve_reference_chain();
                    if let Ok(time_dimension) = dim.as_time_dimension() {
                        time_dimensions.push(time_dimension);
                    }
                }

                if time_dimensions.is_empty() {
                    let rolling_base = self.add_rolling_window_base(
                        member.clone(),
                        state.clone(),
                        ungrouped,
                        descriptions,
                    )?;
                    return Ok(Some(rolling_base));
                }
                let uniq_time_dimensions = time_dimensions
                    .iter()
                    .unique_by(|a| (a.cube_name(), a.name(), a.date_range_vec()))
                    .collect_vec();
                if uniq_time_dimensions.len() != 1 {
                    return Err(CubeError::internal(
                        "Rolling window requires one time dimension and equal date ranges"
                            .to_string(),
                    ));
                }

                let time_dimension =
                    GranularityHelper::find_dimension_with_min_granularity(&time_dimensions)?;
                let time_dimension = MemberSymbol::new_time_dimension(time_dimension);

                let (base_rolling_state, base_time_dimension) = self.make_rolling_base_state(
                    time_dimension.clone(),
                    &rolling_window,
                    state.clone(),
                )?;
                let base_member = MemberSymbol::new_measure(measure.new_unrolling());

                let time_series =
                    self.add_time_series(time_dimension.clone(), state.clone(), descriptions)?;

                let rolling_base = if !measure.is_multi_stage() {
                    self.add_rolling_window_base(
                        base_member,
                        base_rolling_state,
                        ungrouped,
                        descriptions,
                    )?
                } else {
                    self.make_queries_descriptions(base_member, base_rolling_state, descriptions)?
                };

                let input = vec![time_series, rolling_base];

                let alias = format!("cte_{}", descriptions.len());

                let rolling_window_descr = if measure.is_running_total() {
                    RollingWindowDescription::new_running_total(time_dimension, base_time_dimension)
                } else if let Some(granularity) =
                    self.get_to_date_rolling_granularity(&rolling_window)?
                {
                    RollingWindowDescription::new_to_date(
                        time_dimension,
                        base_time_dimension,
                        granularity,
                    )
                } else {
                    RollingWindowDescription::new_regular(
                        time_dimension,
                        base_time_dimension,
                        rolling_window.trailing.clone(),
                        rolling_window.leading.clone(),
                        rolling_window.offset.clone().unwrap_or("end".to_string()),
                    )
                };

                let inode_member = MultiStageInodeMember::new(
                    MultiStageInodeMemberType::RollingWindow(rolling_window_descr),
                    vec![],
                    vec![],
                    None,
                    None,
                );

                let description = MultiStageQueryDescription::new(
                    MultiStageMember::new(
                        MultiStageMemberType::Inode(inode_member),
                        member,
                        self.query_properties.ungrouped(),
                        false,
                    ),
                    state.clone(),
                    input,
                    alias.clone(),
                );
                descriptions.push(description.clone());
                Ok(Some(description))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn add_time_series_get_range_query(
        &self,
        time_dimension: Rc<MemberSymbol>,
        state: Rc<MultiStageAppliedState>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
    ) -> Result<Rc<MultiStageQueryDescription>, CubeError> {
        let description = if let Some(description) = descriptions
            .iter()
            .find(|d| d.alias() == "time_series_get_range")
        {
            description.clone()
        } else {
            let time_series_get_range_node = MultiStageQueryDescription::new(
                MultiStageMember::new(
                    MultiStageMemberType::Leaf(MultiStageLeafMemberType::TimeSeriesGetRange(
                        time_dimension.clone(),
                    )),
                    time_dimension.clone(),
                    true,
                    false,
                ),
                state.clone(),
                vec![],
                "time_series_get_range".to_string(),
            );
            descriptions.push(time_series_get_range_node.clone());
            time_series_get_range_node
        };
        Ok(description)
    }

    fn add_time_series(
        &self,
        time_dimension: Rc<MemberSymbol>,
        state: Rc<MultiStageAppliedState>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
    ) -> Result<Rc<MultiStageQueryDescription>, CubeError> {
        let description = if let Some(description) =
            descriptions.iter().find(|d| d.alias() == "time_series")
        {
            description.clone()
        } else {
            let get_range_query_description = if time_dimension
                .as_time_dimension()?
                .date_range_vec()
                .is_some()
            {
                None
            } else {
                Some(self.add_time_series_get_range_query(
                    time_dimension.clone(),
                    state.clone(),
                    descriptions,
                )?)
            };
            let time_series_node = MultiStageQueryDescription::new(
                MultiStageMember::new(
                    MultiStageMemberType::Leaf(MultiStageLeafMemberType::TimeSeries(Rc::new(
                        TimeSeriesDescription {
                            time_dimension: time_dimension.clone(),
                            date_range_cte: get_range_query_description.map(|d| d.alias().clone()),
                        },
                    ))),
                    time_dimension.clone(),
                    true,
                    false,
                ),
                state.clone(),
                vec![],
                "time_series".to_string(),
            );
            descriptions.push(time_series_node.clone());
            time_series_node
        };
        Ok(description)
    }

    fn add_rolling_window_base(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<MultiStageAppliedState>,
        ungrouped: bool,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
    ) -> Result<Rc<MultiStageQueryDescription>, CubeError> {
        let alias = format!("cte_{}", descriptions.len());
        let description = MultiStageQueryDescription::new(
            MultiStageMember::new(
                MultiStageMemberType::Leaf(MultiStageLeafMemberType::Measure),
                member,
                self.query_properties.ungrouped() || ungrouped,
                true,
            ),
            state,
            vec![],
            alias.clone(),
        );
        descriptions.push(description.clone());
        Ok(description)
    }

    fn get_to_date_rolling_granularity(
        &self,
        rolling_window: &RollingWindow,
    ) -> Result<Option<String>, CubeError> {
        let is_to_date = rolling_window
            .rolling_type
            .as_ref()
            .is_some_and(|tp| tp == "to_date");

        if is_to_date {
            if let Some(granularity) = &rolling_window.granularity {
                Ok(Some(granularity.clone()))
            } else {
                Err(CubeError::user(format!(
                    "Granularity required for to_date rolling window"
                )))
            }
        } else {
            Ok(None)
        }
    }

    fn make_rolling_base_state(
        &self,
        time_dimension: Rc<MemberSymbol>,
        rolling_window: &RollingWindow,
        state: Rc<MultiStageAppliedState>,
    ) -> Result<(Rc<MultiStageAppliedState>, Rc<MemberSymbol>), CubeError> {
        let time_dimension_symbol = time_dimension.as_time_dimension()?;
        let time_dimension_base_name = time_dimension_symbol.base_symbol().full_name();
        let mut new_state = state.clone_state();
        let trailing_granularity =
            GranularityHelper::granularity_from_interval(&rolling_window.trailing);
        let leading_granularity =
            GranularityHelper::granularity_from_interval(&rolling_window.leading);
        let window_granularity =
            GranularityHelper::min_granularity(&trailing_granularity, &leading_granularity)?;
        let result_granularity = GranularityHelper::min_granularity(
            &window_granularity,
            &time_dimension_symbol.resolved_granularity()?,
        )?;

        let new_time_dimension_symbol = time_dimension_symbol
            .change_granularity(self.query_tools.clone(), result_granularity.clone())?;
        let new_time_dimension = MemberSymbol::new_time_dimension(new_time_dimension_symbol);
        //We keep only one time_dimension in the leaf query because, even if time_dimension values have different granularity, in the leaf query we need to group by the lowest granularity.
        new_state.set_time_dimensions(vec![new_time_dimension.clone()]);

        let dimensions = new_state
            .dimensions()
            .clone()
            .into_iter()
            .filter(|d| {
                d.clone()
                    .resolve_reference_chain()
                    .as_time_dimension()
                    .is_err()
            })
            .collect_vec();
        new_state.set_dimensions(dimensions);

        if let Some(granularity) = self.get_to_date_rolling_granularity(rolling_window)? {
            new_state.replace_to_date_date_range_filter(&time_dimension_base_name, &granularity);
        } else {
            new_state.replace_regular_date_range_filter(
                &time_dimension_base_name,
                rolling_window.trailing.clone(),
                rolling_window.leading.clone(),
            );
        }

        Ok((Rc::new(new_state), new_time_dimension))
    }
}
