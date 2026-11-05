use crate::logical_plan::{LogicalSchema, SubqueryRef};
use crate::planner::collectors::has_multi_stage_members;
use crate::planner::{InputId, MemberId, MemberSymbol, QueryProperties};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashSet;
use std::rc::Rc;

/// A planned multi-stage subquery as the stages reading it see it.
#[derive(Clone)]
pub struct StageRef {
    subquery: Rc<SubqueryRef>,
    is_multi_stage_dimension: bool,
    // Dimensions of this stage and of every stage it reads, in preorder,
    // each kept at its first occurrence.
    subtree_dimensions: Rc<Vec<Rc<MemberSymbol>>>,
    subtree_time_dimensions: Rc<Vec<Rc<MemberSymbol>>>,
}

impl StageRef {
    /// A stage computing `member` at the grain of `state`.
    pub fn new(
        id: InputId,
        name: String,
        state: &QueryProperties,
        member: &Rc<MemberSymbol>,
        is_multi_stage_dimension: bool,
        inputs: &[StageRef],
    ) -> Self {
        let schema = LogicalSchema::default()
            .set_time_dimensions(state.time_dimensions().clone())
            .set_dimensions(state.dimensions().clone())
            .set_measures(vec![member.clone()])
            .into_rc();
        let subquery = SubqueryRef::builder()
            .id(id)
            .name(name)
            .schema(schema)
            .build();
        let subtree_dimensions = state
            .dimensions()
            .iter()
            .chain(inputs.iter().flat_map(|i| i.subtree_dimensions.iter()))
            .unique_by(|d| d.id().clone())
            .cloned()
            .collect();
        let subtree_time_dimensions = state
            .time_dimensions()
            .iter()
            .chain(inputs.iter().flat_map(|i| i.subtree_time_dimensions.iter()))
            .unique_by(|d| d.id().clone())
            .cloned()
            .collect();
        Self {
            subquery: Rc::new(subquery),
            is_multi_stage_dimension,
            subtree_dimensions: Rc::new(subtree_dimensions),
            subtree_time_dimensions: Rc::new(subtree_time_dimensions),
        }
    }

    pub fn subquery(&self) -> &Rc<SubqueryRef> {
        &self.subquery
    }

    pub fn id(&self) -> InputId {
        self.subquery.id()
    }

    pub fn name(&self) -> &String {
        self.subquery.name()
    }

    pub fn schema(&self) -> &Rc<LogicalSchema> {
        self.subquery.schema()
    }

    pub fn is_multi_stage_dimension(&self) -> bool {
        self.is_multi_stage_dimension
    }

    /// `(dimensions, time_dimensions)` of the subtree that have no multi-stage
    /// members of their own, deduplicated in order.
    pub fn non_multi_stage_dimensions(
        &self,
    ) -> Result<(Vec<Rc<MemberSymbol>>, Vec<Rc<MemberSymbol>>), CubeError> {
        let non_multi_stage = |members: &[Rc<MemberSymbol>]| {
            members
                .iter()
                .unique_by(|d| d.id().clone())
                .filter_map(|d| match has_multi_stage_members(d, true) {
                    Ok(true) => None,
                    Ok(false) => Some(Ok(d.clone())),
                    Err(e) => Some(Err(e)),
                })
                .collect::<Result<Vec<_>, _>>()
        };
        Ok((
            non_multi_stage(&self.subtree_dimensions)?,
            non_multi_stage(&self.subtree_time_dimensions)?,
        ))
    }
}

/// A time series planned for rolling windows; windows over the same date range
/// share it, each adding the calendar granularity it bounds itself by.
#[derive(Clone)]
pub struct TimeSeriesEntry {
    pub stage: StageRef,
    pub time_dimension: Rc<MemberSymbol>,
    pub date_range_subquery: Option<Rc<SubqueryRef>>,
    pub calendar_period_granularities: Vec<String>,
}

/// What one multi-stage planning run has planned so far.
#[derive(Default)]
pub struct StageRegistry {
    stages: Vec<(MemberId, Rc<QueryProperties>, StageRef)>,
    stage_count: usize,
    time_series: Vec<TimeSeriesEntry>,
    date_range_query: Option<StageRef>,
    resolved_multi_stage_dimensions: HashSet<MemberId>,
}

impl StageRegistry {
    /// The stage already computing `member` under an equivalent state.
    pub fn find(&self, member: &Rc<MemberSymbol>, state: &QueryProperties) -> Option<StageRef> {
        self.stages
            .iter()
            .find(|(id, stage_state, _)| id == member.id() && state.eq_as_state(stage_state))
            .map(|(_, _, stage)| stage.clone())
    }

    /// Records a planned stage; `reusable` makes it a target for `find`.
    pub fn add(
        &mut self,
        member: &Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        stage: StageRef,
        reusable: bool,
    ) {
        self.stage_count += 1;
        if reusable {
            self.stages.push((member.id().clone(), state, stage));
        }
    }

    pub fn stage_count(&self) -> usize {
        self.stage_count
    }

    pub fn time_series(&self) -> &[TimeSeriesEntry] {
        &self.time_series
    }

    pub fn time_series_mut(&mut self, index: usize) -> &mut TimeSeriesEntry {
        &mut self.time_series[index]
    }

    pub fn add_time_series(&mut self, entry: TimeSeriesEntry) {
        self.time_series.push(entry);
    }

    pub fn date_range_query(&self) -> Option<&StageRef> {
        self.date_range_query.as_ref()
    }

    pub fn set_date_range_query(&mut self, stage: StageRef) {
        self.date_range_query = Some(stage);
    }

    pub fn resolved_multi_stage_dimensions(&self) -> &HashSet<MemberId> {
        &self.resolved_multi_stage_dimensions
    }

    pub fn add_resolved_multi_stage_dimension(&mut self, id: MemberId) {
        self.resolved_multi_stage_dimensions.insert(id);
    }
}
