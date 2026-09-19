use crate::cube_bridge::member_sql::FilterParamsColumn;
use crate::planner::planners::multi_stage::FilterParamsTimeShift;
use crate::planner::sql_call::SqlCallFilterParamsItem;
use crate::planner::SqlInterval;
use cubenativeutils::CubeError;

/// The binding a stage renders for one member, and the offset its column has
/// to carry to stand for the shifted member.
pub struct SelectedBinding<'a> {
    pub item: &'a SqlCallFilterParamsItem,
    pub time_shift: Option<SqlInterval>,
}

/// Picks the `FILTER_PARAMS` binding that belongs in a stage where `shift` is
/// active, or `None` when the model declares none for it. See
/// [`FilterParamsTimeShift`] for what each kind of shift leaves a binding able
/// to state.
pub fn select_binding<'a>(
    items: &'a [SqlCallFilterParamsItem],
    shift: Option<FilterParamsTimeShift>,
) -> Result<Option<SelectedBinding<'a>>, CubeError> {
    reject_shifted_string_columns(items)?;

    let (shift_name, interval) = match shift {
        None => (None, None),
        Some(FilterParamsTimeShift::Interval(interval)) => (None, Some(interval)),
        Some(FilterParamsTimeShift::Calendar(calendar)) => {
            reject_undeclared_shifts(items, &calendar.declared_names)?;
            match calendar.name {
                // A calendar declaration with no name of its own cannot be
                // addressed, so nothing states the band the stage reads.
                None => return Ok(None),
                Some(name) => (Some(name), None),
            }
        }
    };

    let item = items.iter().find(|item| item.time_shift_name == shift_name);

    Ok(item.map(|item| SelectedBinding {
        item,
        time_shift: interval,
    }))
}

/// A shifted member is not the column offset by anything the planner can write,
/// so the predicate has to come from the model.
pub fn reject_shifted_string_columns(items: &[SqlCallFilterParamsItem]) -> Result<(), CubeError> {
    for item in items.iter() {
        let Some(shift_name) = &item.time_shift_name else {
            continue;
        };
        if let FilterParamsColumn::String(_) = &item.column {
            return Err(CubeError::user(format!(
                "FILTER_PARAMS binding for time shift `{}` of `{}` passes a column. A shifted \
                 member is not the column offset by anything the planner can write, so the \
                 predicate has to come from the model: pass a callback taking the filter bounds \
                 instead",
                shift_name, item.filter_symbol_name
            )));
        }
    }
    Ok(())
}

/// Nothing shifts a segment, so a binding naming a shift has no segment to
/// state.
pub fn reject_segment_shifts(items: &[SqlCallFilterParamsItem]) -> Result<(), CubeError> {
    match items.iter().find(|item| item.time_shift_name.is_some()) {
        Some(item) => Err(CubeError::user(format!(
            "FILTER_PARAMS binding for `{}` names time shift `{}`, but nothing shifts a segment",
            item.filter_symbol_name,
            item.time_shift_name.as_deref().unwrap_or_default()
        ))),
        None => Ok(()),
    }
}

// A calendar shift moves the primary key the whole cube joins through, so a
// binding on any of its members can name it: the names are checked against the
// calendar's declarations rather than against the member the binding happens to
// bind, which need not declare a shift of its own.
fn reject_undeclared_shifts(
    items: &[SqlCallFilterParamsItem],
    declared_names: &[String],
) -> Result<(), CubeError> {
    for item in items.iter() {
        let Some(shift_name) = &item.time_shift_name else {
            continue;
        };
        if !declared_names.contains(shift_name) {
            return Err(CubeError::user(format!(
                "FILTER_PARAMS binding names time shift `{}`, which the calendar of `{}` does \
                 not declare",
                shift_name, item.filter_symbol_name
            )));
        }
    }
    Ok(())
}
