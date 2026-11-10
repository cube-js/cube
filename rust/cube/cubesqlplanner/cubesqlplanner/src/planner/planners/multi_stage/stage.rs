use crate::logical_plan::{LogicalSchema, SubqueryRef};
use crate::planner::collectors::has_multi_stage_members;
use crate::planner::{InputId, MemberId, MemberSymbol, OrderByItem, QueryProperties};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashSet;
use std::rc::Rc;

/// A planned multi-stage subquery as the stages reading it see it.
#[derive(Clone)]
pub struct StageRef {
    subquery: Rc<SubqueryRef>,
    // The dimensions of the state the stage is computed at, which readers
    // merge it by; the subquery may output more.
    grain: Rc<LogicalSchema>,
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
        let output = grain_of(state).set_measures(vec![member.clone()]);
        Self::with_output(id, name, state, output, is_multi_stage_dimension, inputs)
    }

    /// A stage outputting only the grain of `state`.
    pub fn grid(id: InputId, name: String, state: &QueryProperties) -> Self {
        Self::with_output(id, name, state, grain_of(state), false, &[])
    }

    /// A stage outputting `output`, computed at the grain of `state`.
    pub fn with_output(
        id: InputId,
        name: String,
        state: &QueryProperties,
        output: LogicalSchema,
        is_multi_stage_dimension: bool,
        inputs: &[StageRef],
    ) -> Self {
        let subquery = SubqueryRef::builder()
            .id(id)
            .name(name)
            .schema(output.into_rc())
            .build();
        Self {
            subquery: Rc::new(subquery),
            grain: grain_of(state).into_rc(),
            is_multi_stage_dimension,
            subtree_dimensions: Rc::new(subtree(state.dimensions(), inputs, |i| {
                &i.subtree_dimensions
            })),
            subtree_time_dimensions: Rc::new(subtree(state.time_dimensions(), inputs, |i| {
                &i.subtree_time_dimensions
            })),
        }
    }

    pub fn grain(&self) -> &Rc<LogicalSchema> {
        &self.grain
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

    pub fn is_multi_stage_dimension(&self) -> bool {
        self.is_multi_stage_dimension
    }

    /// `(dimensions, time_dimensions)` of `state` and of the subtrees of
    /// `inputs` that have no multi-stage members of their own, deduplicated in
    /// order.
    pub fn non_multi_stage_dimensions(
        state: &QueryProperties,
        inputs: &[StageRef],
    ) -> Result<(Vec<Rc<MemberSymbol>>, Vec<Rc<MemberSymbol>>), CubeError> {
        let non_multi_stage = |members: Vec<Rc<MemberSymbol>>| {
            members
                .into_iter()
                .filter_map(|d| match has_multi_stage_members(&d, true) {
                    Ok(true) => None,
                    Ok(false) => Some(Ok(d)),
                    Err(e) => Some(Err(e)),
                })
                .collect::<Result<Vec<_>, _>>()
        };
        Ok((
            non_multi_stage(subtree(state.dimensions(), inputs, |i| {
                &i.subtree_dimensions
            }))?,
            non_multi_stage(subtree(state.time_dimensions(), inputs, |i| {
                &i.subtree_time_dimensions
            }))?,
        ))
    }
}

fn grain_of(state: &QueryProperties) -> LogicalSchema {
    LogicalSchema::default()
        .set_time_dimensions(state.time_dimensions().clone())
        .set_dimensions(state.dimensions().clone())
}

// `own` followed by the subtrees of `inputs`, each member at its first occurrence.
fn subtree(
    own: &[Rc<MemberSymbol>],
    inputs: &[StageRef],
    of: impl Fn(&StageRef) -> &Rc<Vec<Rc<MemberSymbol>>>,
) -> Vec<Rc<MemberSymbol>> {
    own.iter()
        .chain(inputs.iter().flat_map(|i| of(i).iter()))
        .unique_by(|d| d.id().clone())
        .cloned()
        .collect()
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
    date_range_queries: Vec<(MemberId, StageRef)>,
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

    /// The query computing the data range of the time dimension `member`.
    pub fn date_range_query(&self, member: &MemberId) -> Option<&StageRef> {
        self.date_range_queries
            .iter()
            .find(|(id, _)| id == member)
            .map(|(_, stage)| stage)
    }

    pub fn date_range_query_count(&self) -> usize {
        self.date_range_queries.len()
    }

    pub fn add_date_range_query(&mut self, member: MemberId, stage: StageRef) {
        self.date_range_queries.push((member, stage));
    }

    pub fn resolved_multi_stage_dimensions(&self) -> &HashSet<MemberId> {
        &self.resolved_multi_stage_dimensions
    }

    pub fn add_resolved_multi_stage_dimension(&mut self, id: MemberId) {
        self.resolved_multi_stage_dimensions.insert(id);
    }
}

pub(super) fn query_order_by(
    member: &Rc<MemberSymbol>,
    state: &QueryProperties,
) -> Vec<OrderByItem> {
    let measures = if member.as_measure().is_ok() {
        vec![member.clone()]
    } else {
        vec![]
    };

    QueryProperties::default_order(&state.dimensions(), &state.time_dimensions(), &measures)
}

/// Adds `member` to the schema list of its kind, skipping a dimension already
/// present under another name of the same reference chain.
pub(super) fn add_member_by_kind(
    member: &Rc<MemberSymbol>,
    dimensions: &mut Vec<Rc<MemberSymbol>>,
    time_dimensions: &mut Vec<Rc<MemberSymbol>>,
    measures: &mut Vec<Rc<MemberSymbol>>,
) {
    let same_chain = |d: &Rc<MemberSymbol>| {
        d.clone().resolve_reference_chain() == member.clone().resolve_reference_chain()
    };
    match member.as_ref() {
        MemberSymbol::Dimension(_) => {
            if !dimensions.iter().any(same_chain) {
                dimensions.push(member.clone())
            }
        }
        MemberSymbol::TimeDimension(_) => {
            if !time_dimensions.iter().any(same_chain) {
                time_dimensions.push(member.clone())
            }
        }
        MemberSymbol::Measure(_) => measures.push(member.clone()),
        MemberSymbol::Ref(_) => {
            if member.is_measure() {
                measures.push(member.clone())
            } else if member.is_dimension() && !dimensions.iter().any(same_chain) {
                dimensions.push(member.clone())
            }
        }
        MemberSymbol::MemberExpression(_) => {}
    }
}
