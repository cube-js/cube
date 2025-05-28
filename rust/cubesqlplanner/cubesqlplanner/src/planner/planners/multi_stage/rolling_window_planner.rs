use super::{
    MultiStageAppliedState, MultiStageInodeMember, MultiStageInodeMemberType,
    MultiStageLeafMemberType, MultiStageMember, MultiStageMemberType, MultiStageQueryDescription,
    RollingWindowDescription, TimeSeriesDescription,
};
use crate::cube_bridge::measure_definition::RollingWindow;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::BaseMeasure;
use crate::planner::{BaseMember, BaseTimeDimension, GranularityHelper, QueryProperties};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub struct RollingWindowPlanner {
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
}

impl RollingWindowPlanner {
    pub fn new(query_tools: Rc<QueryTools>, query_properties: Rc<QueryProperties>) -> Self {
        Self {
            query_tools,
            query_properties,
        }
    }

    pub fn try_plan_rolling_window(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<MultiStageAppliedState>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
    ) -> Result<Option<Rc<MultiStageQueryDescription>>, CubeError> {
        if let Some(measure) = BaseMeasure::try_new(member.clone(), self.query_tools.clone())? {
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
                let ungrouped = match member.as_ref() {
                    MemberSymbol::Measure(measure_symbol) => {
                        measure_symbol.is_rolling_window() && !measure_symbol.is_addictive()
                    }
                    _ => false,
                };
                let mut time_dimensions = self.query_properties.time_dimensions().clone();
                for dim in self.query_properties.dimension_symbols() {
                    let dim = dim.resolve_reference_chain();
                    if let Ok(time_dimension_symbol) = dim.as_time_dimension() {
                        let time_dimension = BaseTimeDimension::try_new_from_td_symbol(
                            self.query_tools.clone(),
                            time_dimension_symbol,
                        )?;
                        time_dimensions.push(time_dimension);
                    }
                }

                if time_dimensions.len() == 0 {
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
                    .unique_by(|a| (a.cube_name(), a.name(), a.get_date_range()))
                    .collect_vec();
                if uniq_time_dimensions.len() != 1 {
                    return Err(CubeError::internal(
                        "Rolling window requires one time dimension and equal date ranges"
                            .to_string(),
                    ));
                }

                let time_dimension =
                    GranularityHelper::find_dimension_with_min_granularity(&time_dimensions)?;

                let (base_rolling_state, base_time_dimension) = self.make_rolling_base_state(
                    time_dimension.clone(),
                    &rolling_window,
                    state.clone(),
                )?;
                let input = vec![
                    self.add_time_series(time_dimension.clone(), state.clone(), descriptions)?,
                    self.add_rolling_window_base(
                        member.clone(),
                        base_rolling_state,
                        ungrouped,
                        descriptions,
                    )?,
                ];

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
                    vec![],
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
        time_dimension: Rc<BaseTimeDimension>,
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
                    time_dimension.member_evaluator(),
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
        time_dimension: Rc<BaseTimeDimension>,
        state: Rc<MultiStageAppliedState>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
    ) -> Result<Rc<MultiStageQueryDescription>, CubeError> {
        let description = if let Some(description) =
            descriptions.iter().find(|d| d.alias() == "time_series")
        {
            description.clone()
        } else {
            let get_range_query_description = if time_dimension.get_date_range().is_some() {
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
                    time_dimension.member_evaluator(),
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
            .map_or(false, |tp| tp == "to_date");

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
        time_dimension: Rc<BaseTimeDimension>,
        rolling_window: &RollingWindow,
        state: Rc<MultiStageAppliedState>,
    ) -> Result<(Rc<MultiStageAppliedState>, Rc<BaseTimeDimension>), CubeError> {
        let time_dimension_base_name = time_dimension.base_dimension().full_name();
        let mut new_state = state.clone_state();
        let trailing_granularity =
            GranularityHelper::granularity_from_interval(&rolling_window.trailing);
        let leading_granularity =
            GranularityHelper::granularity_from_interval(&rolling_window.leading);
        let window_granularity =
            GranularityHelper::min_granularity(&trailing_granularity, &leading_granularity)?;
        let result_granularity = GranularityHelper::min_granularity(
            &window_granularity,
            &time_dimension.resolve_granularity()?,
        )?;

        let new_time_dimension = time_dimension.change_granularity(result_granularity.clone())?;
        //We keep only one time_dimension in the leaf query because, even if time_dimension values have different granularity, in the leaf query we need to group by the lowest granularity.
        new_state.set_time_dimensions(vec![new_time_dimension.clone()]);

        let dimensions = new_state
            .dimensions()
            .clone()
            .into_iter()
            .filter(|d| {
                d.member_evaluator()
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
