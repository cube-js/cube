use super::multi_stage::MultiStageMemberQueryPlanner;
use super::multi_stage::{MultiStageAppliedState, MultiStageQueryDescription};
use crate::plan::{Expr, From, Select, SelectBuilder, Subquery};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::has_multi_stage_members;
use crate::planner::sql_evaluator::collectors::member_childs;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_evaluator::EvaluationNode;
use crate::planner::QueryProperties;
use crate::planner::{BaseDimension, BaseMeasure, VisitorContext};
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
    pub fn plan_queries(&self) -> Result<(Vec<Rc<Subquery>>, Vec<Rc<Select>>), CubeError> {
        let multi_stage_members = self
            .query_properties
            .all_members(false)
            .into_iter()
            .filter_map(|memb| -> Option<Result<_, CubeError>> {
                match has_multi_stage_members(&memb.member_evaluator()) {
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
        let all_filter_members = self.query_properties.all_filtered_members();
        let state = MultiStageAppliedState::new(
            self.query_properties.dimensions().clone(),
            all_filter_members,
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

        let all_queries = descriptions
            .into_iter()
            .map(|descr| {
                MultiStageMemberQueryPlanner::new(
                    self.query_tools.clone(),
                    self.query_properties.clone(),
                    descr.clone(),
                )
                .plan_query()
            })
            .collect::<Result<Vec<_>, _>>()?;

        let cte_joins = top_level_ctes
            .iter()
            .map(|alias| self.cte_select(alias))
            .collect_vec();

        Ok((all_queries, cte_joins))
    }

    pub fn cte_select(&self, alias: &String) -> Rc<Select> {
        let mut select_builder = SelectBuilder::new(
            From::new_from_table_reference(alias.clone(), None),
            VisitorContext::default(SqlNodesFactory::new()),
        );
        select_builder.set_projection(vec![Expr::Asterix]);

        Rc::new(select_builder.build())
    }

    fn make_queries_descriptions(
        &self,
        member: Rc<EvaluationNode>,
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

        let (dimensions_to_add, time_shifts) = if let Some(measure) =
            BaseMeasure::try_new_from_precompiled(member.clone(), self.query_tools.clone())?
        {
            let dimensions_to_add = if let Some(add_group_by) = measure.add_group_by() {
                add_group_by
                    .iter()
                    .map(|name| self.compile_dimension(name))
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                vec![]
            };

            (dimensions_to_add, measure.time_shifts().clone())
        } else {
            (vec![], vec![])
        };

        let new_state = if !dimensions_to_add.is_empty()
            || !time_shifts.is_empty()
            || state.is_filter_allowed(&member_name)
        {
            let mut new_state = state.clone_state();
            if !dimensions_to_add.is_empty() {
                new_state.add_dimensions(dimensions_to_add);
            }
            if !time_shifts.is_empty() {
                new_state.add_time_shifts(time_shifts);
            }
            if state.is_filter_allowed(&member_name) {
                new_state.disallow_filter(&member_name);
            }
            Rc::new(new_state)
        } else {
            state.clone()
        };

        let childs = member_childs(&member)?;
        let input = childs
            .into_iter()
            .map(
                |child| -> Result<Rc<MultiStageQueryDescription>, CubeError> {
                    self.make_queries_descriptions(child, new_state.clone(), descriptions)
                },
            )
            .collect::<Result<Vec<_>, _>>()?;
        let alias = format!("cte_{}", descriptions.len());
        let description =
            MultiStageQueryDescription::new(member, state.clone(), input, alias.clone());
        descriptions.push(description.clone());
        Ok(description)
    }

    fn compile_dimension(&self, name: &String) -> Result<Rc<BaseDimension>, CubeError> {
        let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();
        let evaluator = evaluator_compiler.add_dimension_evaluator(name.clone())?;
        BaseDimension::try_new(name.clone(), self.query_tools.clone(), evaluator)
    }
}
