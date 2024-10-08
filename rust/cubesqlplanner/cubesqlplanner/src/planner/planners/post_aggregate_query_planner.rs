use super::{CommonUtils, JoinPlanner, OrderPlanner};
use crate::plan::{Filter, From, FromSource, Join, JoinItem, JoinSource, Select};
use crate::planner::base_join_condition::DimensionJoinCondition;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::{
    collect_multiplied_measures, has_post_aggregate_members,
};
use crate::planner::sql_evaluator::sql_nodes::with_render_references_default_node_processor;
use crate::planner::QueryProperties;
use crate::planner::{
    BaseDimension, BaseMeasure, BaseMember, PrimaryJoinCondition, VisitorContext,
};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

pub struct PostAggregateQueryPlanner {
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
}

impl PostAggregateQueryPlanner {
    pub fn new(query_tools: Rc<QueryTools>, query_properties: Rc<QueryProperties>) -> Self {
        Self {
            query_tools,
            query_properties,
        }
    }
    pub fn plan(self) -> Result<Option<Select>, CubeError> {
        let post_aggregate_members = self
            .query_properties
            .all_members(false)
            .into_iter()
            .filter_map(|m| -> Option<Result<_, CubeError>> {
                match has_post_aggregate_members(&m.member_evaluator()) {
                    Ok(res) => {
                        if res {
                            Some(Ok(m))
                        } else {
                            None
                        }
                    }
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        for member in self.query_properties.all_members(false) {}
        Ok(None)
    }
}
