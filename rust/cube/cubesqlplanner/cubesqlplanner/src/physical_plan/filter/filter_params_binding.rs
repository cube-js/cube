use crate::cube_bridge::member_sql::FilterParamsColumn;
use crate::planner::planners::multi_stage::FilterParamsTimeShift;
use crate::planner::sql_call::SqlCallFilterParamsItem;
use crate::planner::symbols::MemberSymbol;
use crate::planner::SqlInterval;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// The binding a stage renders for one member, and the offset its column has
/// to carry to stand for the shifted member.
pub struct SelectedBinding<'a> {
    pub item: &'a SqlCallFilterParamsItem,
    pub time_shift: Option<SqlInterval>,
}

/// Picks the `FILTER_PARAMS` binding of `symbol` that belongs in a stage where
/// `shift` is active, or `None` when the model declares none for it.
///
/// A binding narrows the scan of the cube's own SQL to the rows the stage can
/// use, so it may only state a band the stage actually reads. Under an interval
/// shift the stage reads the band offset by that interval, which the source
/// column expresses by being offset the same way, so the plain binding renders
/// shifted. Under a calendar shift the stage reaches its rows through a mapping
/// held in the calendar's own table, which no expression over the source column
/// reproduces: the plain binding would cut the scan down to the reporting
/// period the stage reads past. Only a binding written for that shift knows
/// which band to keep, so only it renders.
pub fn select_binding<'a>(
    items: &'a [SqlCallFilterParamsItem],
    symbol: &Rc<MemberSymbol>,
    shift: Option<FilterParamsTimeShift>,
) -> Result<Option<SelectedBinding<'a>>, CubeError> {
    validate_bindings(items, symbol)?;

    let (shift_name, interval) = match shift {
        None => (None, None),
        Some(FilterParamsTimeShift::Interval(interval)) => (None, Some(interval)),
        Some(FilterParamsTimeShift::Calendar { name }) => match name {
            // A calendar declaration with no name of its own cannot be
            // addressed, so nothing states the band the stage reads.
            None => return Ok(None),
            Some(name) => (Some(name), None),
        },
    };

    let item = items.iter().find(|item| item.time_shift_name == shift_name);

    Ok(item.map(|item| SelectedBinding {
        item,
        time_shift: interval,
    }))
}

/// Rejects a binding whose shift the member does not declare, and one that
/// addresses a shift through a column the planner cannot shift.
fn validate_bindings(
    items: &[SqlCallFilterParamsItem],
    symbol: &Rc<MemberSymbol>,
) -> Result<(), CubeError> {
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
        let declared = symbol
            .as_dimension()
            .ok()
            .map(|dimension| {
                dimension
                    .time_shift()
                    .iter()
                    .any(|declared| declared.name.as_ref() == Some(shift_name))
            })
            .unwrap_or(false);
        if !declared {
            return Err(CubeError::user(format!(
                "FILTER_PARAMS binding names time shift `{}`, which `{}` does not declare",
                shift_name, item.filter_symbol_name
            )));
        }
    }
    Ok(())
}
