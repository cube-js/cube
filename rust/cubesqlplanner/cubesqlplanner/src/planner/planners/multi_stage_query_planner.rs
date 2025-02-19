use super::multi_stage::{
    MultiStageAppliedState, MultiStageInodeMember, MultiStageInodeMemberType,
    MultiStageLeafMemberType, MultiStageMember, MultiStageMemberQueryPlanner, MultiStageMemberType,
    MultiStageQueryDescription, MultiStageTimeShift, RollingWindowDescription,
};
use crate::cube_bridge::measure_definition::RollingWindow;
use crate::plan::{Cte, From, Schema, Select, SelectBuilder};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::has_multi_stage_members;
use crate::planner::sql_evaluator::collectors::member_childs;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::{BaseDimension, BaseMeasure};
use crate::planner::{BaseTimeDimension, GranularityHelper, QueryProperties};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
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
    pub fn plan_queries(&self) -> Result<(Vec<Rc<Cte>>, Vec<Rc<Select>>), CubeError> {
        let multi_stage_members = self
            .query_properties
            .all_members(false)
            .into_iter()
            .filter_map(|memb| -> Option<Result<_, CubeError>> {
                match has_multi_stage_members(&memb.member_evaluator(), false) {
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
            self.query_properties.measures_filters().clone(),
        );

        let top_level_ctes = multi_stage_members
            .into_iter()
            .map(|memb| -> Result<_, CubeError> {
                Ok(self
                    .make_queries_descriptions(
                        memb.member_evaluator().clone(),
                        state.clone(),
                        &mut descriptions,
                    )?
                    .alias()
                    .clone())
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut cte_schemas = HashMap::new();
        let all_queries = descriptions
            .into_iter()
            .map(|descr| -> Result<_, CubeError> {
                let res = MultiStageMemberQueryPlanner::new(
                    self.query_tools.clone(),
                    self.query_properties.clone(),
                    descr.clone(),
                )
                .plan_query(&cte_schemas)?;
                cte_schemas.insert(descr.alias().clone(), res.query().schema());
                Ok(res)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let cte_joins = top_level_ctes
            .iter()
            .map(|alias| self.cte_select(alias, &cte_schemas))
            .collect_vec();

        Ok((all_queries, cte_joins))
    }

    pub fn cte_select(
        &self,
        alias: &String,
        cte_schemas: &HashMap<String, Rc<Schema>>,
    ) -> Rc<Select> {
        let schema = cte_schemas.get(alias).unwrap().clone();
        let select_builder =
            SelectBuilder::new(From::new_from_table_reference(alias.clone(), schema, None));

        Rc::new(select_builder.build(SqlNodesFactory::new()))
    }

    fn create_multi_stage_inode_member(
        &self,
        base_member: Rc<MemberSymbol>,
    ) -> Result<(MultiStageInodeMember, bool), CubeError> {
        let inode = if let Some(measure) =
            BaseMeasure::try_new(base_member.clone(), self.query_tools.clone())?
        {
            let member_type = if measure.measure_type() == "rank" {
                MultiStageInodeMemberType::Rank
            } else if !measure.is_calculated() {
                MultiStageInodeMemberType::Aggregate
            } else {
                MultiStageInodeMemberType::Calculate
            };

            let time_shifts = if let Some(refs) = measure.time_shift_references() {
                let time_shifts = refs
                    .iter()
                    .map(|r| MultiStageTimeShift::try_from_reference(r))
                    .collect::<Result<Vec<_>, _>>()?;
                time_shifts
            } else {
                vec![]
            };
            let is_ungrupped = match &member_type {
                MultiStageInodeMemberType::Rank | MultiStageInodeMemberType::Calculate => true,
                _ => self.query_properties.ungrouped(),
            };
            (
                MultiStageInodeMember::new(
                    member_type,
                    measure.reduce_by().clone().unwrap_or_default(),
                    measure.add_group_by().clone().unwrap_or_default(),
                    measure.group_by().clone(),
                    time_shifts,
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
                    vec![],
                ),
                self.query_properties.ungrouped(),
            )
        };
        Ok(inode)
    }

    fn add_time_series(
        &self,
        time_dimension: Rc<BaseTimeDimension>,
        state: Rc<MultiStageAppliedState>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
    ) -> Result<Rc<MultiStageQueryDescription>, CubeError> {
        let description =
            if let Some(description) = descriptions.iter().find(|d| d.alias() == "time_series") {
                description.clone()
            } else {
                let time_series_node = MultiStageQueryDescription::new(
                    MultiStageMember::new(
                        MultiStageMemberType::Leaf(MultiStageLeafMemberType::TimeSeries(
                            time_dimension.clone(),
                        )),
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
    ) -> Result<Rc<MultiStageAppliedState>, CubeError> {
        let time_dimension_name = time_dimension.member_evaluator().full_name();
        let mut new_state = state.clone_state();
        let trailing_granularity =
            GranularityHelper::granularity_from_interval(&rolling_window.trailing);
        let leading_granularity =
            GranularityHelper::granularity_from_interval(&rolling_window.leading);
        let window_granularity =
            GranularityHelper::min_granularity(&trailing_granularity, &leading_granularity)?;
        let result_granularity = GranularityHelper::min_granularity(
            &window_granularity,
            &time_dimension.get_granularity(),
        )?;

        if time_dimension.get_date_range().is_some() && result_granularity.is_some() {
            let granularity = time_dimension.get_granularity().unwrap(); //FIXME remove this unwrap
            let date_range = time_dimension.get_date_range().unwrap(); //FIXME remove this unwrap
            let series = self
                .query_tools
                .base_tools()
                .generate_time_series(granularity, date_range.clone())?;
            if !series.is_empty() {
                let new_from_date = series.first().unwrap()[0].clone();
                let new_to_date = series.last().unwrap()[1].clone();
                new_state.replace_range_in_date_filter(
                    &time_dimension_name,
                    new_from_date,
                    new_to_date,
                );
            }
        }

        new_state
            .change_time_dimension_granularity(&time_dimension_name, result_granularity.clone());

        if let Some(granularity) = self.get_to_date_rolling_granularity(rolling_window)? {
            new_state.replace_to_date_date_range_filter(&time_dimension_name, &granularity);
        } else {
            new_state.replace_regular_date_range_filter(
                &time_dimension_name,
                rolling_window.trailing.clone(),
                rolling_window.leading.clone(),
            );
        }

        Ok(Rc::new(new_state))
    }

    fn try_make_rolling_window(
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
                        trailing: Some("unbounded".to_string()),
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
                let time_dimensions = self.query_properties.time_dimensions();
                if time_dimensions.len() == 0 {
                    let rolling_base = self.add_rolling_window_base(
                        member.clone(),
                        state.clone(),
                        ungrouped,
                        descriptions,
                    )?;
                    return Ok(Some(rolling_base));
                }
                if time_dimensions.len() != 1 {
                    return Err(CubeError::internal(
                        "Rolling window requires one time dimension".to_string(),
                    ));
                }
                let time_dimension = time_dimensions[0].clone();

                let input = vec![
                    self.add_time_series(time_dimension.clone(), state.clone(), descriptions)?,
                    self.add_rolling_window_base(
                        member.clone(),
                        self.make_rolling_base_state(
                            time_dimension.clone(),
                            &rolling_window,
                            state.clone(),
                        )?,
                        ungrouped,
                        descriptions,
                    )?,
                ];

                let time_dimension = time_dimensions[0].clone();

                let alias = format!("cte_{}", descriptions.len());

                let rolling_window_descr = if let Some(granularity) =
                    self.get_to_date_rolling_granularity(&rolling_window)?
                {
                    RollingWindowDescription::new_to_date(time_dimension, granularity)
                } else {
                    RollingWindowDescription::new_regular(
                        time_dimension,
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

    fn make_queries_descriptions(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<MultiStageAppliedState>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
    ) -> Result<Rc<MultiStageQueryDescription>, CubeError> {
        let member_name = member.full_name();
        if let Some(exists) = descriptions
            .iter()
            .find(|q| q.is_match_member_and_state(&member, &state))
        {
            return Ok(exists.clone());
        };

        if let Some(rolling_window_query) =
            self.try_make_rolling_window(member.clone(), state.clone(), descriptions)?
        {
            return Ok(rolling_window_query);
        }

        let childs = member_childs(&member)?;
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

            let dimensions_to_add = multi_stage_member
                .add_group_by()
                .iter()
                .map(|name| self.compile_dimension(name))
                .collect::<Result<Vec<_>, _>>()?;

            let new_state = if !dimensions_to_add.is_empty()
                || !multi_stage_member.time_shifts().is_empty()
                || state.has_filters_for_member(&member_name)
            {
                let mut new_state = state.clone_state();
                if !dimensions_to_add.is_empty() {
                    new_state.add_dimensions(dimensions_to_add);
                }
                if !multi_stage_member.time_shifts().is_empty() {
                    new_state.add_time_shifts(multi_stage_member.time_shifts().clone());
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

    fn compile_dimension(&self, name: &String) -> Result<Rc<BaseDimension>, CubeError> {
        let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();
        let evaluator = evaluator_compiler.add_dimension_evaluator(name.clone())?;
        BaseDimension::try_new_required(evaluator, self.query_tools.clone())
    }
}
