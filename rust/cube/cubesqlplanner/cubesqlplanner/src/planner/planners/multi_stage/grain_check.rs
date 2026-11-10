use crate::planner::collectors::{
    has_multi_stage_members, rendered_dependencies, visit_rendered_slots,
};
use crate::planner::state::State;
use crate::planner::symbols::deps::collect_cube_refs;
use crate::planner::GrainDeclaration;
use crate::planner::MeasureKind;
use crate::planner::MemberId;
use crate::planner::MemberSymbol;
use crate::planner::QueryProperties;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

// Names every dimension at once, with the grain declaring them would
// compute the member at.
fn unreachable_dimensions_error(
    member: &Rc<MemberSymbol>,
    dimensions: &[Rc<MemberSymbol>],
    grain_state: &QueryProperties,
    parent_state: &QueryProperties,
) -> Result<CubeError, CubeError> {
    let grain = grain_members(&[grain_state, parent_state]);
    // A granularity of the very dimension the sql reads looks like a match in
    // the listing, so the one case where reading the grain is not enough to
    // tell them apart is spelled out.
    let hints = dimensions
        .iter()
        .filter_map(|dimension| {
            let target = dimension.full_name();
            grain
                .iter()
                .any(|m| granular_time_dimension_base(m).as_ref() == Some(&target))
                .then(|| {
                    format!(
                        " The grain carries {target} at a granularity, which is a value of \
                         its own and not the dimension itself."
                    )
                })
        })
        .collect::<String>();
    let names = dimensions
        .iter()
        .map(|d| d.full_name())
        .collect::<Vec<_>>()
        .join(", ");
    let described_grain = grain.iter().map(describe_grain_member).collect::<Vec<_>>();
    let current_grain = if described_grain.is_empty() {
        "no dimensions".to_string()
    } else {
        described_grain.join(", ")
    };
    // Widening the grain adds to the stage grain only; the keys side stays
    // the parent's and is broadcast onto separately.
    let resulting_grain = grain_members(&[grain_state])
        .iter()
        .map(describe_grain_member)
        .chain(dimensions.iter().map(|d| d.full_name()))
        .collect::<Vec<_>>()
        .join(", ");
    let (reads, are, them) = if dimensions.len() == 1 {
        ("dimension", "is", "it")
    } else {
        ("dimensions", "are", "them")
    };
    let include_key = grain_include_key(member);
    let alternative = if has_plain_measure_reading(member)? {
        " For the ordinary reading of the expression at the query grain, use a plain \
         (non-multi-stage) measure that aggregates the underlying columns directly instead."
    } else {
        ""
    };
    Ok(CubeError::user(format!(
        "Multi-stage member {member} reads {reads} {names}, which {are} not part of the \
         grain it is computed at ({current_grain}).{hints} Adding {them} to `{include_key}` \
         of {member} computes the member at ({resulting_grain}), and its value depends on \
         that grain: for a non-additive aggregation it changes.{alternative} Otherwise \
         remove {them} from the member's sql.",
        member = member.full_name(),
    )))
}

fn grain_include_key(member: &Rc<MemberSymbol>) -> &'static str {
    let multi_stage = match member.as_ref() {
        MemberSymbol::Measure(measure) => measure.multi_stage(),
        MemberSymbol::Dimension(dimension) => dimension.multi_stage(),
        _ => None,
    };
    multi_stage
        .map(|ms| ms.grain_declaration)
        .unwrap_or(GrainDeclaration::Grain)
        .include_key()
}

// An aggregation written as a plain measure over the raw columns reads at the
// query grain only when nothing else about the stage needs it: no grain, filter
// or shift directive, and no multi-stage input.
fn has_plain_measure_reading(member: &Rc<MemberSymbol>) -> Result<bool, CubeError> {
    let Ok(measure) = member.as_measure() else {
        return Ok(false);
    };
    let has_directives = measure.multi_stage().is_some_and(|ms| {
        ms.grain.include.is_some()
            || ms.grain.exclude.is_some()
            || ms.grain.keep_only.is_some()
            || ms.filter.is_some()
            || ms.time_shift.is_some()
    });
    if has_directives
        || !matches!(
            measure.kind(),
            MeasureKind::Aggregated(_) | MeasureKind::Count(_) | MeasureKind::MultipliedCount(_)
        )
    {
        return Ok(false);
    }
    for dep in member.get_dependencies() {
        if has_multi_stage_members(&dep.resolve_reference_chain(), false)? {
            return Ok(false);
        }
    }
    Ok(true)
}

// The dimensions of the given grains, resolved and deduplicated in order.
fn grain_members(states: &[&QueryProperties]) -> Vec<Rc<MemberSymbol>> {
    let mut members: Vec<Rc<MemberSymbol>> = Vec::new();
    for state in states {
        for dimension in state
            .dimensions()
            .iter()
            .chain(state.time_dimensions().iter())
        {
            let resolved = dimension.clone().resolve_reference_chain();
            if !members.iter().any(|m| m.id() == resolved.id()) {
                members.push(resolved);
            }
        }
    }
    members
}

// A time dimension carries its granularity inside its name, which reads as a
// member of its own; the granularity is named separately instead.
fn describe_grain_member(member: &Rc<MemberSymbol>) -> String {
    match member.as_ref() {
        MemberSymbol::TimeDimension(time_dimension) => match time_dimension.granularity() {
            Some(granularity) => format!(
                "{} ({})",
                time_dimension
                    .base_symbol()
                    .clone()
                    .resolve_reference_chain()
                    .full_name(),
                granularity
            ),
            None => member.full_name(),
        },
        MemberSymbol::Dimension(_)
        | MemberSymbol::Measure(_)
        | MemberSymbol::MemberExpression(_)
        | MemberSymbol::Ref(_) => member.full_name(),
    }
}

fn granular_time_dimension_base(member: &Rc<MemberSymbol>) -> Option<String> {
    match member.as_ref() {
        MemberSymbol::TimeDimension(time_dimension) => {
            time_dimension.granularity().as_ref().map(|_| {
                time_dimension
                    .base_symbol()
                    .clone()
                    .resolve_reference_chain()
                    .full_name()
            })
        }
        MemberSymbol::Dimension(_)
        | MemberSymbol::Measure(_)
        | MemberSymbol::MemberExpression(_)
        | MemberSymbol::Ref(_) => None,
    }
}

// Mirrors how references are resolved when the subquery is rendered: a member the
// source exposes as a column stops the walk, and anything else is reachable only
// if every member its own SQL reads is. A dimension that also reads a raw cube
// column is unreachable whatever its member deps resolve to — that column renders
// against the cube alias, which such a subquery never has in scope.
//
// A leaf outside both grains is reported as well. `sql: category` and a constant
// `sql: "'x'"` are indistinguishable here — neither carries a cube ref — so the
// constant is reported too, and declaring it in `grain.include` resolves that the
// same way. The same ambiguity runs the other way: a bare identifier inside a
// larger expression (`UPPER({CUBE.status}) || category`) carries no cube ref
// either, so it passes and fails at the database instead.
fn dimension_is_reachable(
    dimension: &Rc<MemberSymbol>,
    grain_state: &QueryProperties,
    parent_state: &QueryProperties,
    is_masked: &dyn Fn(&Rc<MemberSymbol>) -> bool,
) -> bool {
    let target = dimension.id();
    let carries = |state: &QueryProperties| {
        state
            .dimensions()
            .iter()
            .chain(state.time_dimensions().iter())
            .any(|d| d.clone().resolve_reference_chain().id() == target)
    };
    if carries(grain_state) || carries(parent_state) {
        return true;
    }
    let deps = rendered_dependencies(dimension, is_masked);
    !deps.is_empty()
        && !reads_raw_cube_column(dimension, is_masked)
        && deps.iter().all(|dep| {
            dimension_is_reachable(
                &dep.clone().resolve_reference_chain(),
                grain_state,
                parent_state,
                is_masked,
            )
        })
}

fn reads_raw_cube_column(
    member: &Rc<MemberSymbol>,
    is_masked: &dyn Fn(&Rc<MemberSymbol>) -> bool,
) -> bool {
    let mut found = false;
    visit_rendered_slots(member, is_masked, &mut |slot| {
        found = found || !collect_cube_refs(slot).is_empty()
    });
    found
}

// Fails unless every dimension `member`'s sql reads is a column of the stage
// grain or of the keys side broadcast onto the parent grain.
pub(super) fn check_dimension_reads(
    query_tools: &State,
    member: &Rc<MemberSymbol>,
    grain_state: &QueryProperties,
    parent_state: &QueryProperties,
) -> Result<(), CubeError> {
    let is_masked =
        |m: &Rc<MemberSymbol>| query_tools.query_tools().is_member_masked(&m.full_name());
    let rendered: HashSet<MemberId> = rendered_dependencies(member, &is_masked)
        .into_iter()
        .map(|d| d.resolve_reference_chain().id().clone())
        .collect();
    let mut unreachable: Vec<Rc<MemberSymbol>> = Vec::new();
    for dep in member.get_dependencies() {
        let dep = dep.resolve_reference_chain();
        if dep.is_dimension()
            && rendered.contains(dep.id())
            && !is_multi_stage_dimension(&dep)?
            && !unreachable.iter().any(|d| d.id() == dep.id())
            && !dimension_is_reachable(&dep, grain_state, parent_state, &is_masked)
        {
            unreachable.push(dep);
        }
    }
    if !unreachable.is_empty() {
        return Err(unreachable_dimensions_error(
            member,
            &unreachable,
            grain_state,
            parent_state,
        )?);
    }
    Ok(())
}

// True if `member` is a dimension that has multi-stage members somewhere in
// its dependency tree.
pub(super) fn is_multi_stage_dimension(member: &Rc<MemberSymbol>) -> Result<bool, CubeError> {
    if member.is_dimension() {
        has_multi_stage_members(member, false)
    } else {
        Ok(false)
    }
}
